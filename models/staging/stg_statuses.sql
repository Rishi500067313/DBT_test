select
    status_id::integer as status_id,
    status_name::varchar as status_name,
    status_group_id::integer as status_group_id
from {{ ref('statuses') }}
