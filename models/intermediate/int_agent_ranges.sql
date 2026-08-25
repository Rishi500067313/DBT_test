{{
    config(
        materialized='incremental',
        unique_key=['customer', 'valid_from'],
        incremental_strategy='delete+insert',
        post_hook="create index if not exists idx_int_agent_ranges_customer_valid_from on {{ this }} (customer, valid_from)"
    )
}}

{#
    Q4.2/Q4.3 — pre-materialize agent_change_log into point-in-time
    ranges once, instead of re-deriving "who was the agent at time T" per
    query against the raw (potentially 100M-row) log every time.

    Q4.4 — the log is append-only but entries can arrive late, which can
    retroactively change a PREVIOUS range's valid_to, not just add a new
    range. A pure "only touch brand-new rows" incremental model would
    miss that. Fix: reprocess a trailing lookback window (by changed_at,
    var: agent_ranges_lookback_days, default 7) on every run rather than
    trusting strict arrival order. This bounds how late an entry can
    arrive and still be picked up correctly -- entries later than the
    lookback window are a known, documented gap, not a silent one.

    customers.agent_id (given in the background as "the current agent")
    is used twice here, deliberately -- ignoring it would waste the one
    piece of ground truth we were handed:
      1. The currently-open range (valid_to IS NULL) is overridden with
         customers.agent_id rather than trusted blindly from the log's
         last row. In the sample data the two already agree, but nothing
         guarantees that in general -- customers is the one explicitly
         described as authoritative for "now", so it wins on conflict.
         (See tests/warn_agent_log_customers_mismatch.sql, which flags
         -- without failing the build -- any customer where they don't.)
      2. A customer with a current agent but ZERO change-log rows would
         otherwise get NULL for every single one of their events, purely
         because they've never been reassigned. customers_without_log
         gives them one all-time-open range instead.
#}

{%- set lookback_days = var('agent_ranges_lookback_days', 7) -%}

with log as (

    select
        customer,
        agent_id,
        changed_at
    from {{ ref('stg_agent_change_log') }}

    {% if is_incremental() %}
    where changed_at >= (
        select coalesce(max(valid_from), '1900-01-01'::timestamp) - interval '{{ lookback_days }} days'
        from {{ this }}
    )
    {% endif %}

),

log_ranges as (

    select
        customer,
        agent_id,
        changed_at as valid_from,
        lead(changed_at) over (partition by customer order by changed_at) as valid_to
    from log

),

ranges_with_current_override as (

    select
        r.customer,
        case
            when r.valid_to is null then coalesce(c.agent_id, r.agent_id)
            else r.agent_id
        end as agent_id,
        r.valid_from,
        r.valid_to
    from log_ranges r
    left join {{ ref('stg_customers') }} c on c.customer = r.customer

),

customers_without_log as (

    select
        c.customer,
        c.agent_id,
        '-infinity'::timestamp as valid_from,
        null::timestamp as valid_to
    from {{ ref('stg_customers') }} c
    where not exists (
        select 1 from {{ ref('stg_agent_change_log') }} l where l.customer = c.customer
    )

)

select * from ranges_with_current_override
union all
select * from customers_without_log
