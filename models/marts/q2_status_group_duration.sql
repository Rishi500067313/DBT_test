{{ config(materialized='view') }}

{#
    Combines the 8 independent shards
    (models/marts/status_group_duration_shards/) into the single logical
    Q2 result. Safe as a plain UNION ALL for the same reason as Q1's
    union: the shards are a true, non-overlapping partition of the
    customer space, so every (customer, session_number, status_group)
    group is computed entirely within one shard -- nothing to reconcile
    across the union.
#}

select * from {{ ref('q2_status_group_duration_shard_0') }}
union all
select * from {{ ref('q2_status_group_duration_shard_1') }}
union all
select * from {{ ref('q2_status_group_duration_shard_2') }}
union all
select * from {{ ref('q2_status_group_duration_shard_3') }}
union all
select * from {{ ref('q2_status_group_duration_shard_4') }}
union all
select * from {{ ref('q2_status_group_duration_shard_5') }}
union all
select * from {{ ref('q2_status_group_duration_shard_6') }}
union all
select * from {{ ref('q2_status_group_duration_shard_7') }}
