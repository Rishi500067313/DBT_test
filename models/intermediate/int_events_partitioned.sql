{{
    config(
        materialized='view',
        pre_hook="{{ create_events_partitions() }}"
    )
}}

{#
    Not part of the real data flow -- this model exists only so dbt's DAG
    has a node to hang the partition-creation/reload pre_hook on, with
    stg_events as its ref()'d dependency (so dbt builds stg_events first)
    and the 8 sessioned_shards/*.sql models depending on THIS (via an
    explicit `{% do ref(...) %}`, since they read the raw partition
    tables directly, not through ref()) so they never run before the
    reload finishes. See macros/create_events_partitions.sql.
#}

select * from {{ ref('stg_events') }} limit 0
