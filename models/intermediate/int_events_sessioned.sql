{{ config(materialized='view') }}

{#
    Combines the 8 independent shards (models/intermediate/sessioned_shards/)
    back into the single logical Q1 result. A plain UNION ALL is safe and
    complete here specifically because the shards are a true partition of
    the customer space (hash-based, no overlap, no customer split across
    two shards) -- there's nothing left to reconcile between them.

    Downstream models (q1_session_grouping, q2_status_group_duration)
    keep referencing this view unchanged; the sharding is an
    implementation detail behind it. See
    macros/create_events_partitions.sql for the benchmarks behind the
    8-way split, and note in q2_status_group_duration.sql: querying
    *through* this view (e.g. Q2's LEAD()) runs as a single, unsharded
    query again -- the parallelism benefit is specific to the session_agg
    computation that produces this table's rows, not automatically
    inherited by everything built on top of it.
#}

select * from {{ ref('int_events_sessioned_shard_0') }}
union all
select * from {{ ref('int_events_sessioned_shard_1') }}
union all
select * from {{ ref('int_events_sessioned_shard_2') }}
union all
select * from {{ ref('int_events_sessioned_shard_3') }}
union all
select * from {{ ref('int_events_sessioned_shard_4') }}
union all
select * from {{ ref('int_events_sessioned_shard_5') }}
union all
select * from {{ ref('int_events_sessioned_shard_6') }}
union all
select * from {{ ref('int_events_sessioned_shard_7') }}
