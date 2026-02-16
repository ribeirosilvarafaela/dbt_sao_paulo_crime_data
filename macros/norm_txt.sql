{% macro norm_txt(column_name) -%}
case
    when {{ column_name }} is null then 'INDISPONIVEL'
    when trim(cast({{ column_name }} as string)) in ('', 'NULL') then 'INDISPONIVEL'
    else
        regexp_replace(
            upper(
                regexp_replace(
                    normalize(trim(cast({{ column_name }} as string)), nfd),
                    r'\pM',
                    ''
                )
            ),
            r'^S[\.\s]+',
            'SAO '
        )
end
{%- endmacro %}
