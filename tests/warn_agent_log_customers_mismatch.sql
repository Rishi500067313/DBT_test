{{ config(severity = 'warn') }}

{#
    Data-quality signal for Q4: customers.agent_id is described as "the
    current agent" and agent_change_log's last entry per customer should
    agree with it. int_agent_ranges resolves conflicts in favor of
    customers (see its header comment) so a mismatch here doesn't break
    anything downstream -- but it means the two source systems have
    drifted and is worth someone's attention.
#}

with last_log_entry as (
    select distinct on (customer)
        customer,
        agent_id as last_logged_agent
    from {{ ref('stg_agent_change_log') }}
    order by customer, changed_at desc
)

select
    l.customer,
    l.last_logged_agent,
    c.agent_id as customers_agent_id
from last_log_entry l
join {{ ref('stg_customers') }} c on c.customer = l.customer
where l.last_logged_agent is distinct from c.agent_id
