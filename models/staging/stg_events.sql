select
    id::integer   as id,
    customer::varchar as customer,
    dt::timestamp as dt,
    status_id::integer as status_id
from {{ ref('events') }}
