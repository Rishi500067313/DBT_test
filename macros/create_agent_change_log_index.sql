{% macro create_agent_change_log_index() %}
{#
    Supports int_agent_ranges' "PARTITION BY customer ORDER BY changed_at"
    and its incremental lookback filter on changed_at.
#}
create index if not exists idx_agent_change_log_customer_changed_at
    on {{ this }} (customer, changed_at);
{% endmacro %}
