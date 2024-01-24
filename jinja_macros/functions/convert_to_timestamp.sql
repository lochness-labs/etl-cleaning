{% macro convert_to_timestamp(
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
            ELSE 
                COALESCE(
                    TRY( CAST( {{ ns.f }} as timestamp )),
                    TRY( CAST( from_iso8601_timestamp( {{ ns.f }} ) as timestamp ) ),
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d %H:%i:%s.%f') as timestamp ) ), 
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d %H:%i:%s') as timestamp ) ), 
                    TRY( CAST( date_parse({{ ns.f }}, '%Y%m%d %H:%i:%s') as timestamp ) ), 
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%d-%m-%Y %H:%i:%s') as timestamp ) ),
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%d-%m-%Y %H:%i') as timestamp ) ),
                    CAST( {{ ns.f }} as timestamp )
                )
        END

    {% else -%}

        CASE 
            WHEN  {{ ns.f }} = '' THEN {{ default_value }}
            ELSE 
                COALESCE(
                    TRY( CAST( {{ ns.f }} as timestamp )),
                    TRY( CAST( from_iso8601_timestamp( {{ ns.f }} ) as timestamp ) ),
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d %H:%i:%s.%f') as timestamp ) ), 
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%Y-%m-%d %H:%i:%s') as timestamp ) ),
                    TRY( CAST( date_parse({{ ns.f }}, '%Y%m%d %H:%i:%s') as timestamp ) ), 
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%d-%m-%Y %H:%i:%s') as timestamp ) ),
                    TRY( CAST( date_parse(replace( {{ ns.f }}, '/', '-'), '%d-%m-%Y %H:%i') as timestamp ) ),
                    {{ default_value }}
                )
        END

    {% endif -%}

{% endmacro -%}
