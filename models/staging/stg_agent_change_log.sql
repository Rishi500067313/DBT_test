select
    id::integer as id,
    customer::varchar as customer,
    agent_id::varchar as agent_id,
    changed_at::timestamp as changed_at
from {{ ref('agent_change_log') }}
