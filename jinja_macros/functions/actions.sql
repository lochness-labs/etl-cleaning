{% macro run(ns, actions) -%}

    {% for action in actions -%}

        {% if action == 'keep_only_numbers' -%}
            {% set ns.f = "regexp_extract(" + ns.f +  ", '(\d+)' )" -%}
        {% endif -%}

        {% if action == 'convert_nan_na_to_null' -%}
            {% set ns.f = " REGEXP_REPLACE(" + ns.f + ", '^(nan|<NA>|NaT|None|\[\])$', '') " -%}
        {% endif -%}

        {% if action == 'fix_numpy_arrays' -%}
            {% set ns.f = "regexp_replace(regexp_replace(regexp_replace(regexp_replace(" + ns.f + ", '\r\n', ' ' ), '^\[\s+', '['), '\s+]', ']'), '\s+', ',')" -%}
        {% endif -%}

        {% if 'coalesce' in action -%}
            {% for key, value in action.items() -%}
                {% for column in value -%}
                    {% set ns.f = "coalesce(" + ns.f + ', "' + column + '")' -%}
                {% endfor -%}
            {% endfor -%}
        {% endif -%}

    {% endfor -%}

{% endmacro -%}