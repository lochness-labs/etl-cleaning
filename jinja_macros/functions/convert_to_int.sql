{% macro convert_to_int(
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
            WHEN {{ ns.f }} IS NULL THEN {{ default_value }}
            WHEN {{ ns.f }} like '%.%' THEN CAST(CAST(  replace( {{ ns.f }}, ',', '') AS double ) as int)
            ELSE CAST(  replace( {{ ns.f }}, ',', '') AS int )
        END

    {% else -%}
        COALESCE(
            TRY(CASE 
                    WHEN  {{ ns.f }} = '' THEN {{ default_value }}
                    WHEN {{ ns.f }} IS NULL THEN {{ default_value }}
                    WHEN {{ ns.f }} like '%.%' THEN CAST(CAST(  replace( {{ ns.f }}, ',', '') AS double ) as int)
                    ELSE CAST( replace( {{ ns.f }}, ',', '') AS int )
                END),
            {{ default_value }}
        )

    {% endif -%}

{% endmacro -%}