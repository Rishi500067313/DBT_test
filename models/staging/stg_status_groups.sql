select
    status_group_id::integer as status_group_id,
    status_group_name::varchar as status_group_name
from {{ ref('status_groups') }}
