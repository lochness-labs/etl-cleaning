{% macro convert_to_string(
                            field,
                            default_value='NULL',
                            conversion_required=false,
                            actions=[]
                        ) -%}

    {% import "functions/actions.sql" as f_actions -%}
    {% set ns = namespace(f= '"' + field + '"') -%}
    {{ f_actions.run(ns, actions) }}
    
    {% if default_value != 'NULL' -%}
        CASE 
            WHEN {{ ns.f }} = '' THEN {{ "'" ~ default_value ~ "'"}}
            WHEN {{ ns.f }} IS NULL THEN {{ "'" ~ default_value ~ "'"}}
            ELSE {{ ns.f }} 
        END
    {% else -%}
        CASE WHEN {{ ns.f }} = '' THEN {{ default_value }}  ELSE {{ ns.f }} END
    {% endif -%}     

{% endmacro -%}