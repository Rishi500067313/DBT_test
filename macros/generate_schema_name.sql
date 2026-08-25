{% macro generate_schema_name(custom_schema_name, node) -%}
{#
    dbt's default behavior suffixes a custom schema onto the target
    schema (e.g. "public_stg"). The assessment's warehouse convention is
    two literal schemas, dwh.stg and dwh.prod — this override makes
    `+schema: stg` / `+schema: prod` mean exactly that schema, not a
    suffix, matching how the real dwh is laid out.
#}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
