select
    customer::varchar as customer,
    agent_id::varchar as agent_id
from {{ ref('customers') }}
