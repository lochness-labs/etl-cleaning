{% macro convert_to_date(
                            field,
                            default_value='NULL',
                            conversion_required=false,
                            actions=[]
                        ) -%}

    {% import "functions/actions.sql" as f_actions -%}
    {% set ns = namespace(f= '"' + field + '"') -%}
    {{ f_actions.run(ns, actions) }}

    {% if conversion_required -%}

        CASE WHEN  {{ ns.f }} = '' THEN {{ default_value }}
        ELSE 
            COALESCE(
                TRY( CAST( {{ ns.f }} AS date) ),
                TRY( CAST( date_parse({{ ns.f }}, '%Y%m%d') AS date) ),
                TRY( CAST( from_unixtime( CAST( {{ ns.f }} AS int)) AS date ) ),
                TRY( CAST( from_iso8601_timestamp( {{ ns.f }} ) AS date) ),
                TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d %H:%i:%s') AS date) ),
                TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d') AS date) ),
                TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%d-%m-%Y') AS date) ),
                CAST( {{ ns.f }} as date )
            )
        END

    {% else -%}

        CASE WHEN  {{ ns.f }} = '' THEN {{ default_value }}
        ELSE 
            COALESCE(
                TRY( CAST( {{ ns.f }} AS date) ),
                TRY( CAST( date_parse({{ ns.f }}, '%Y%m%d') AS date) ),
                TRY( CAST( from_unixtime( CAST( {{ ns.f }} AS int)) AS date ) ),
                TRY( CAST( from_iso8601_timestamp( {{ ns.f }} ) AS date) ),
                TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d %H:%i:%s') AS date) ),
                TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d') AS date) ),
                TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%d-%m-%Y') AS date) ),
                {{default_value}}
            )
        END

    {% endif -%}
    

{% endmacro -%}
