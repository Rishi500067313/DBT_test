{{ config(materialized='view') }}

{#
    One of 8 shards of Q2, mirroring Q1's sharding (see
    macros/create_events_partitions.sql). Built directly on
    int_events_sessioned_shard_5, not the unioned int_events_sessioned
    view -- querying through the union would run as a single unsharded
    query again, which is exactly the bottleneck this is avoiding.

    This split matters even though Q2 only uses built-in window
    functions (LEAD), not the custom aggregate: benchmarked at 50M rows
    on this machine, LAG/SUM grouping + LEAD duration + a GROUP BY alone
    took 276.69s single-threaded -- multiple full-table window passes
    plus a large hash aggregate add up even with built-in operators.
    A single partition's worth (~1/8th, run alongside the other 7)
    finishes in a small fraction of that.
#}

with durations as (

    select
        customer,
        session_number,
        status_id,
        dt,
        lead(dt) over (
            partition by customer, session_number
            order by dt
        ) as next_dt
    from {{ ref('int_events_sessioned_shard_5') }}

)

select
    d.customer,
    d.session_number,
    sg.status_group_name,
    round(
        sum(extract(epoch from (d.next_dt - d.dt)) / 60.0)::numeric,
        2
    ) as minutes_in_status_group
from durations d
join {{ ref('stg_statuses') }} s
    on s.status_id = d.status_id
join {{ ref('stg_status_groups') }} sg
    on sg.status_group_id = s.status_group_id
where d.next_dt is not null
group by d.customer, d.session_number, sg.status_group_name
