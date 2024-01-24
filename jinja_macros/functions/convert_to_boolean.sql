{% macro convert_to_boolean(
                            field,
                            default_value='NULL',
                            conversion_required=false,
                            actions=[]
                        ) -%}
   
    {% import "functions/actions.sql" as f_actions -%}
    {% set ns = namespace(f= '"' + field + '"') -%}
    {{ f_actions.run(ns, actions) }}

    {% if conversion_required -%}

        CASE
            WHEN  {{ ns.f }} = '' THEN {{ default_value }}
            WHEN UPPER( {{ ns.f }} ) in ('NO', 'F', 'FALSE') THEN FALSE
            WHEN UPPER( {{ ns.f }} ) in ('YES', 'T', 'TRUE') THEN TRUE
            ELSE CAST( {{ ns.f }} AS boolean)
        END

    {% else -%}
        COALESCE(
            TRY(
                CASE
                    WHEN  {{ ns.f }} = '' THEN {{ default_value }}
                    WHEN UPPER( {{ ns.f }} ) in ('NO', 'F', 'FALSE') THEN FALSE
                    WHEN UPPER( {{ ns.f }} ) in ('YES', 'T', 'TRUE') THEN TRUE
                    ELSE CAST( {{ ns.f }} AS boolean)
                END 
            ),
            {{ default_value }}
        )
    {% endif %}

{% endmacro -%}
