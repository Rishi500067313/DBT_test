select
    id,
    customer,
    dt,
    is_session_start,
    session_number
from {{ ref('int_events_sessioned') }}
order by customer, dt
