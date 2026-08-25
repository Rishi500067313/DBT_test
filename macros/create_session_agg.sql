{% macro create_session_agg() %}
{#
    Sessionization (Q1) needs a stateful, single-ordered-pass calculation:
    "is this event more than 1 hour after the last SESSION START for this
    customer" depends on which earlier row was itself flagged a start, not
    just the literal previous row — so a plain LAG()/cumulative-SUM()
    window function can't express it, and a recursive CTE doesn't scale to
    50M rows in 30 seconds.

    session_agg is a custom Postgres aggregate that carries
    (last_start, session_no) as running state and is applied as a window
    function with `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
    Because that frame's start never moves, Postgres only ever needs the
    forward transition function — the same mechanism that makes
    `SUM(x) OVER (ORDER BY dt)` a single O(n) pass instead of O(n^2) — so
    this is a genuine single ordered pass per customer.

    This is DDL, not a model, so it's created here idempotently on every
    dbt invocation rather than depending on someone having run a one-off
    migration first.
#}
{% set create_type_sql %}
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'session_state') THEN
            CREATE TYPE session_state AS (
                last_start  timestamp,
                session_no  integer,
                is_start    boolean
            );
        END IF;
    END$$;
{% endset %}

{% set create_function_sql %}
    CREATE OR REPLACE FUNCTION session_state_transition(state session_state, dt timestamp)
    RETURNS session_state
    LANGUAGE plpgsql
    AS $f$
    BEGIN
        IF state IS NULL THEN
            -- first event ever seen for this customer -> always a session start
            RETURN ROW(dt, 1, true)::session_state;
        ELSIF dt - state.last_start > interval '1 hour' THEN
            RETURN ROW(dt, state.session_no + 1, true)::session_state;
        ELSE
            RETURN ROW(state.last_start, state.session_no, false)::session_state;
        END IF;
    END;
    $f$;
{% endset %}

{% set create_aggregate_sql %}
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_proc p
            JOIN pg_aggregate a ON a.aggfnoid = p.oid
            WHERE p.proname = 'session_agg'
        ) THEN
            CREATE AGGREGATE session_agg(timestamp) (
                SFUNC = session_state_transition,
                STYPE = session_state
            );
        END IF;
    END$$;
{% endset %}

{% do run_query(create_type_sql) %}
{% do run_query(create_function_sql) %}
{% do run_query(create_aggregate_sql) %}
{% endmacro %}
