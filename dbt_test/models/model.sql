{{
    config(
        alias='TMP_DATA_PROFILE',
        materialized='ephemeral'
    )
}}

{% if (flags.WHICH).upper() == 'RUN' %}

    {% if execute %}

        -- Configure the destination details    
        {%- set destination_database   = 'GOVERNANCE_'+(target.name).upper() -%}
        {%- set read_config_table      = run_query('SELECT * FROM ' + destination_database + '.DATA_META.CONFIG_DATA_PROFILE') -%}

        {% set get_current_timestamp %}
            SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS utc_time_zone
        {% endset %}

        {% if execute %}
            {% set profiled_at = run_query(get_current_timestamp).columns[0].values()[0] %}
        {% endif %}

        -- Iterate through the layer
        {%- for profile_detail in read_config_table -%}

            {%- set schema_create -%}
                {{ create_new_schema(destination_database, profile_detail[0]) }}
            {%- endset -%}

            {% do run_query(schema_create) %}
            
            -- Create the table in snowflake if not exists
            {%- set create_table -%}
                {{ create_data_profile_table(destination_database, profile_detail[0], profile_detail[1]) }}
            {%- endset -%}

            {% do run_query(create_table) %}

            {% if execute %}
                {% set source_database = profile_detail[2].replace("+(target.name).upper()+", (target.name).upper()) %}
            {% endif %}

            {% if profile_detail[4] is not none %} {% set include_tables = profile_detail[4].split(',') %} {% else %} {% set include_tables = [] %} {% endif %}
            {% if profile_detail[5] is not none %} {% set exclude_tables = profile_detail[5].split(',') %} {% else %} {% set exclude_tables = [] %} {% endif %}

            -- Read the table names from information schema for that particular layer
            {%- set read_information_schema_datas -%}
                {{ read_information_schema(source_database, profile_detail[3], include_tables, exclude_tables) }}
            {%- endset -%}

            {% set information_schema_datas = run_query(read_information_schema_datas) %}
            
            -- This loop is used to itetrate the tables in layer
            {%- for information_schema_data in information_schema_datas -%}
                {%- set source_table_name = information_schema_data[0] + '.' + information_schema_data[1] + '.' + information_schema_data[2] -%}
                {%- set source_columns    = adapter.get_columns_in_relation(source_table_name) | list -%}
                {%- set chunk_columns     = [] -%}
                
                -- Validating last insert query status
                {% set validator_query %}
                    SELECT 

                        'Query ID :' || query_id || ';\n' || ERROR_MESSAGE AS error_info

                    FROM TABLE({{ destination_database }}.information_schema.query_history())
                    WHERE 
                        session_id = CURRENT_SESSION()
                        AND execution_status LIKE 'FAILED%';
                {% endset %}
                
                -- This loop is used to iterate the columns inside the table
                {%- for source_column in source_columns -%}
                    {%- do chunk_columns.append(source_column) -%}

                    {%- if (chunk_columns | length) == 100 -%}
                        {%- set insert_rows -%}
                            INSERT INTO {{ destination_database }}.{{ profile_detail[0] }}.{{ profile_detail[1] }} (

                                    {%- for chunk_column in chunk_columns -%}
                                        {{ do_data_profile(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                        {% if not loop.last %} UNION ALL {% endif %}
                                    {%- endfor -%}
                                )
                        {%- endset -%}

                        {% do run_query(insert_rows) %}
                        {%- do chunk_columns.clear() -%}

                        {% set validator_results = run_query(validator_query ) %}

                        -- If query status failed, Raising the exception
                        {% if validator_results | length > 0 %}
                            {{ exceptions.raise_compiler_error( validator_results.columns[0].values()[0] ) }}
                        {% endif %}

                    {%- endif -%}
                {%- endfor -%} 

                -- This condition iterate the columns if any of them are missed in above condition
                {%- if (chunk_columns | length) != 0 -%}
                    {%- set insert_rows -%}
                        INSERT INTO {{ destination_database }}.{{ profile_detail[0] }}.{{ profile_detail[1] }} (

                                {%- for chunk_column in chunk_columns -%}
                                    {{ do_data_profile(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                    {% if not loop.last %} UNION ALL {% endif %}
                                {%- endfor -%}
                            )
                    {%- endset -%}

                    {% do run_query(insert_rows) %}
                    {%- do chunk_columns.clear() -%}

                    {% set validator_results = run_query(validator_query ) %}

                    -- If query status failed, Raising the exception
                    {% if validator_results | length > 0 %}
                        {{ exceptions.raise_compiler_error( validator_results.columns[0].values()[0] ) }}
                    {% endif %}
                    
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {% endif %}
{% endif %}

-- This select statement is used to temporarily stored the data in table after that we deleted this table using post hook method 
SELECT 'temp_data_for_creating_the_table' AS temp_column