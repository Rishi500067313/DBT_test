{{
    config(
        materialized='incremental',
        unique_key=['customer', 'dt'],
        incremental_strategy='delete+insert',
        post_hook=[
            "create index if not exists idx_int_events_sessioned_shard_1_customer_dt on {{ this }} (customer, dt)",
            "create index if not exists idx_int_events_sessioned_shard_1_customer_start on {{ this }} (customer, dt desc) where is_session_start = 1"
        ]
    )
}}

{#
    One of 8 independent shards of Q1's session computation. See
    macros/create_events_partitions.sql for why 8 shards (real benchmark
    numbers, not a guess) and for why this reads a raw partition table
    (prod.events_partitioned_p1) rather than a dbt ref() -- dbt has no
    native support for Postgres hash-partitioned tables.

    Because events land in a partition purely by hash(customer), every
    customer's complete history lives in exactly ONE shard -- a session
    never spans a partition boundary, so this is a complete, independent
    computation per shard, not an approximation. The actual parallel
    speedup only materializes if dbt runs all 8 shard models on 8
    separate connections at once: `dbt run --threads 8` (or more).

    Session logic (session_agg custom aggregate + incremental lookback
    anchored to each customer's last known session start, not a flat
    time window) is unchanged from the original single-table version --
    only the source table changed, from stg_events to this one partition.
#}

{# Force dbt to run int_events_partitioned (which loads prod.events_partitioned_p1) before this shard -- these shards read a raw partition table, not a ref(), so dbt has no other way to know the dependency. #}
{% do ref('int_events_partitioned') %}

with last_known_start as (

    {% if is_incremental() %}
    select distinct on (customer)
        customer,
        dt              as last_start_dt,
        session_number  as last_session_number
    from {{ this }}
    where is_session_start = 1
    order by customer, dt desc
    {% else %}
    select
        null::varchar   as customer,
        null::timestamp as last_start_dt,
        null::integer   as last_session_number
    where false
    {% endif %}

),

events as (

    select
        e.id,
        e.customer,
        e.dt,
        e.status_id,
        coalesce(lk.last_session_number, 1) as base_session_number
    from prod.events_partitioned_p1 e
    left join last_known_start lk on lk.customer = e.customer
    where e.dt >= coalesce(lk.last_start_dt, '1900-01-01'::timestamp)

),

sessioned as (

    select
        id,
        customer,
        dt,
        status_id,
        base_session_number,
        session_agg(dt) over (
            partition by customer
            order by dt
            rows between unbounded preceding and current row
        ) as s
    from events

)

select
    id,
    customer,
    dt,
    status_id,
    (s).is_start::int                       as is_session_start,
    (s).session_no + base_session_number - 1 as session_number
from sessioned
