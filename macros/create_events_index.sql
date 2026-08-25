{% macro create_events_index() %}
{#
    Perf requirement (50M rows / <30s): lets Postgres satisfy
    "PARTITION BY customer ORDER BY dt" in int_events_sessioned via an
    index scan instead of sorting the whole table from scratch.
    A post-hook on the staging model rather than on-run-start, since it
    needs stg_events to exist first.
#}
create index if not exists idx_stg_events_customer_dt
    on {{ this }} (customer, dt);
{% endmacro %}
