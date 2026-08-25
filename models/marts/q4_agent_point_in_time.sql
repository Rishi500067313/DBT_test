{{ config(pre_hook="set local max_parallel_workers_per_gather = 8") }}

{#
    Reads off int_agent_ranges (Q4.2's materialized ranges) rather than a
    LATERAL lookup against the raw log, per the Q4.2/Q4.3 recommendation:
    a plain range comparison against a small, indexed table instead of
    re-deriving history from a 100M-row log on every query.

    Edge case: a customer's very first event(s) can predate their first
    logged change (true in the sample data for Alice) -- there is no
    historical agent for those, and int_agent_ranges correctly has no
    row covering that time, so this returns NULL rather than guessing
    from customers.agent_id (which is only the CURRENT agent).

    Unlike Q1/Q2, this one doesn't need manual sharding: with an
    equality join on customer plus a small ranges table, Postgres's
    planner already picks a Parallel Hash Join on its own (ranges as the
    hash-build side, events parallel-scanned as the probe side) --
    verified via EXPLAIN at real 50M-row scale. The only lever that
    mattered was worker count: 23.4s at the default
    max_parallel_workers_per_gather=2, 9.98s at 8. `set local` scopes
    that bump to just this model's transaction, not the whole session.
#}
select
    e.id,
    e.customer,
    e.dt,
    r.agent_id
from {{ ref('stg_events') }} e
left join {{ ref('int_agent_ranges') }} r
    on r.customer = e.customer
    and e.dt >= r.valid_from
    and (e.dt < r.valid_to or r.valid_to is null)
order by e.customer, e.dt
