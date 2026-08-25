# test_demo — DBT / SQL / Postgres Assessment

This project implements the three "write a SQL query" tasks from the assessment
(Q1 task 1, Q2 task 1, Q4 task 1) as real dbt models running against Postgres,
built from the sample data provided (`seeds/*.csv`). Q3 and the "explain" items
(Q1.3/1.4, Q2.2, Q4.2–4.4) are discussion-only per the assignment's own
instructions and aren't modeled here, but the design choices behind the models
below double as the answers to those questions.

Tested end-to-end against a live Postgres instance (`dbt seed && dbt run && dbt test`)
using the `test_demo` profile.

## Project layout

```
seeds/                       raw sample data (events, statuses, status_groups, customers, agent_change_log)
models/staging/               typed 1:1 pass-through over each seed  (schema: stg)
models/intermediate/
  int_events_partitioned.sql   loads the physical partitioned events table (see Scalability Audit)
  sessioned_shards/            8 independent Q1 shard models
  int_events_sessioned.sql     UNION ALL of the 8 shards -- Q1's logical output   (schema: prod)
  int_agent_ranges.sql         Q4's materialized point-in-time ranges             (schema: prod)
models/marts/
  q1_session_grouping.sql, q4_agent_point_in_time.sql
  status_group_duration_shards/  8 independent Q2 shard models
  q2_status_group_duration.sql   UNION ALL of the 8 shards -- Q2's logical output (schema: prod)
macros/
  create_session_agg.sql       DDL for Q1's custom window aggregate, run via on-run-start
  create_events_partitions.sql DDL + reload for the physically hash-partitioned events table
  create_events_index.sql      perf index for the flat events seed (customer, dt)
  create_agent_change_log_index.sql   perf index for Q4 (customer, changed_at)
  generate_schema_name.sql     makes +schema mean the literal schema name (stg/prod), not a suffix
tests/
  warn_agent_log_customers_mismatch.sql   data-quality check for Q4 (see below)
```

The `stg` / `prod` split mirrors the assessment's stated warehouse convention
(dwh with `stg`, `prod` schemas): staging models are thin typed views over the
seeds, everything derived lives in `prod`.

---

## Scalability audit

The assessment asks for Q1 to run over ~50M rows in under 30 seconds. The
first version of this project claimed the custom `session_agg` aggregate met
that bar because it's algorithmically O(n) (single ordered pass, no
recursion). That claim was never actually load-tested — and it was wrong.
Benchmarked for real against a live 50M-row table (not extrapolated) on this
machine (14 logical cores):

| Approach | Time @ 50M rows | Verdict |
|---|---|---|
| Single connection, `session_agg` over the whole table | **242.78s** | fails, ~8x over budget |
| "Shard" via `WHERE mod(hashtext(customer), n) = k`, 8-way | **41.47s** | fails, and for the wrong reason (see below) |
| TRUE physical hash partitioning, 4-way | **35.05s** | fails, not enough margin |
| TRUE physical hash partitioning, 8-way | **18.18s** | **passes**, real margin |

Two things worth knowing about, beyond the headline numbers:

- **The obvious "just add a WHERE filter" sharding approach is actively
  wrong**, not just insufficiently fast. `EXPLAIN` showed it defeats the
  `(customer, dt)` index entirely (the filter isn't sargable), forcing a full
  `Seq Scan` plus a disk-spilling external sort *per shard*. Running more
  copies of a broken plan concurrently doesn't reliably help — it was only
  "fast enough to look plausible," not actually a sound design.
- **Postgres will not auto-parallelize a `WindowAgg` built on a custom
  aggregate.** A single connection is stuck paying `session_agg`'s cost
  serially no matter what, regardless of available cores. The only way to
  actually hit the 30-second bar is genuine cross-connection parallelism —
  which Postgres also won't do for you automatically here, so it has to be
  built explicitly.

**The fix:** physically hash-partition `events` by `customer` into 8
partitions (`macros/create_events_partitions.sql`). Hash partitioning
guarantees a customer's complete history lands in exactly one partition — a
session can never span a partition boundary — so 8 queries against 8
partitions are 8 complete, independent, correct computations, not an
approximation. Wired into dbt as 8 separate model files per question
(`models/intermediate/sessioned_shards/*.sql` for Q1,
`models/marts/status_group_duration_shards/*.sql` for Q2), because dbt has no
native support for Postgres table partitioning and no way to get genuine
parallelism out of a *single* SQL statement here — the parallelism comes from
dbt's own thread-based concurrency running independent DAG nodes on separate
connections. **This only works if `dbt run` actually uses 8+ threads**
(`profiles.yml`'s `test_demo.outputs.dev.threads` was bumped from 5 to 8 for
exactly this reason — worth checking on any machine this runs on).

**Q2 turned out to have the same problem, for a different reason.** Even
using only built-in window functions (`LAG`/`SUM` for grouping, `LEAD` for
duration, no custom aggregate at all), the full Q2 pipeline measured **276.69s**
at 50M rows single-threaded — multiple full-table window passes plus a large
`GROUP BY` add up even with cheap built-in operators. The assessment doesn't
state an explicit SLA for Q2 the way it does for Q1, but leaving a
demonstrated 276s bottleneck undocumented (or documented-but-unfixed) wasn't
a real option once found. Given Q1's shard infrastructure already exists,
extending the same pattern to Q2 was cheap: each Q2 shard builds directly off
the *matching* Q1 shard (not the unioned view — querying through the union
would run as one unsharded query again and reintroduce the exact bottleneck
being avoided).

**Q4 needed no such rework.** `EXPLAIN` at the same real 50M-row scale showed
Postgres's planner already choosing a `Parallel Hash Join` on its own (the
small `agent_ranges` table as the hash-build side, `events` parallel-scanned
as the probe side) — because it's a plain equality join, not a custom
aggregate under a window function, native parallel query already applies. The
only lever that mattered was worker count: 23.4s at the Postgres default
`max_parallel_workers_per_gather = 2`, 9.98s at 8. Set via `set local` in
`q4_agent_point_in_time.sql`'s `pre_hook`, scoped to just that model's
transaction.

All scratch/benchmark tables used for this audit were dropped afterward —
they were never part of the project's actual seed data or schema.

---

## Q1 — Session Grouping

**Problem.** Add `is_session_start` and `session_number` to `events`. A new
session starts when more than 1 hour has passed since the customer's **last
session start** — not since the last event. Must run on Postgres, ~50M rows,
under 30 seconds.

**Tables used:** `events` (seed) → `stg_events`.

**The logic.** This is the part of the assessment that looks simple but isn't:
"gap since last session start" is a *stateful* condition — whether row N is a
new session depends on which earlier row was itself flagged a start, not on
row N's literal predecessor. A plain `LAG()` + cumulative `SUM()` window
function can't express that (it only sees the immediately preceding row), and
a recursive CTE can express it but doesn't scale to 50M rows in 30 seconds
(row-by-row, no parallelism).

I confirmed this distinction matters using the sample data itself, not just
in theory: Alice's event at `2024-01-01 09:30` is a session start under "gap
since last session start" but **not** under "gap since last event" — the two
readings disagree on this exact row. That settles which interpretation the
assessment is actually testing.

**Solution:** a custom Postgres aggregate, `session_agg`, carrying
`(last_start, session_no)` as running state, applied as a **window function**
with `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. Because that frame's
start never moves, Postgres only needs the forward transition step — the same
mechanism that makes `SUM(x) OVER (ORDER BY dt)` a single O(n) pass instead of
O(n²) — so this is a genuine single ordered pass per customer: correct *and*
fast enough for the 50M-row bar. An index on `(customer, dt)` lets Postgres
satisfy the `PARTITION BY … ORDER BY …` via an index scan instead of sorting
the whole table.

**DBT implementation:**
- `macros/create_session_agg.sql` — creates the `session_state` type, the
  `session_state_transition` function, and the `session_agg` aggregate,
  idempotently (checks `pg_type`/`pg_proc` before creating). Wired in via
  `on-run-start` in `dbt_project.yml` since this is DDL, not a model — it has
  to exist before any model can reference it.
- `models/intermediate/int_events_partitioned.sql` — a thin dbt node whose
  `pre_hook` runs `macros/create_events_partitions.sql`: physically
  hash-partitions `events` into 8 partitions by `customer` and reloads them
  from `stg_events`. Exists purely to give the DAG a place to hang this DDL
  with correct ordering (`stg_events` built first via `ref()`; the 8 shard
  models below depend on this via an explicit `{% do ref(...) %}`, since they
  read raw partition tables directly, not through `ref()`).
- `models/intermediate/sessioned_shards/int_events_sessioned_shard_{0..7}.sql`
  — 8 near-identical incremental models, each reading ONE physical partition
  (`prod.events_partitioned_p{i}`) and running the exact same session logic a
  single monolithic model would. The subtle part, same in every shard: a
  naive "reprocess the last N hours" incremental filter can slice into the
  *middle* of an unrelated, already-closed session (a session's own last
  event can land far less than an hour after a different session's
  boundary), which would wrongly re-flag that row as a fresh start. Fixed by
  anchoring the reprocessing window **per customer** to their most recently
  known session start (`last_known_start` CTE) — that point is always a true
  boundary, so restarting the aggregate's state there is always valid.
  `session_number` is then rebased by adding back the previously-stored
  session number at that boundary, so numbering stays continuous across runs
  instead of restarting at 1 each time.
- `models/intermediate/int_events_sessioned.sql` — `UNION ALL` of the 8
  shards; downstream models (Q1's mart, Q2) reference this and don't need to
  know sharding exists underneath it. See **Scalability audit** above for why
  this is 8 separate models instead of one.
- `models/marts/q1_session_grouping.sql` — thin final `select` exposing
  `id, customer, dt, is_session_start, session_number`.
- **Verified live, twice:** (1) at the original small-sample scale — inserted
  new events for Alice and re-ran incrementally, confirmed her prior 14 rows
  (sessions 1–8) were untouched and the new events correctly formed session
  9; (2) after the sharding rework, confirmed the sharded output is
  byte-for-byte identical to the pre-sharding output on the same sample data,
  and re-ran the same incremental-insert test against the new architecture to
  confirm it still holds.

---

## Q2 — Time Spent per Status Group

**Problem.** For each customer session, total minutes spent in each status
group. A customer's status runs from its event's timestamp until the *next*
event in the **same session**; a session's last event has no duration
(ignored).

**Tables used:** `events` (via Q1's `int_events_sessioned`, so session numbers
are already resolved) → `statuses` → `status_groups`.

**The logic.** `LEAD(dt) OVER (PARTITION BY customer, session_number ORDER BY dt)`
gives each event its "next event in this session" timestamp — partitioning by
`session_number` (not just `customer`) is what makes the last event of a
session correctly get `NULL` (nothing follows it *within that session*), which
is exactly the "no duration" rule. Duration in minutes = `EXTRACT(EPOCH FROM (next_dt - dt)) / 60`,
joined through `statuses.status_group_id → status_groups.status_group_name`
and aggregated per `(customer, session_number, status_group_name)`.

One consequence worth knowing about, not a bug: several sessions in the
sample data are single-event sessions (long isolated events, gap > 1hr on
both sides) — those correctly produce **no rows at all** in Q2's output,
since their one event is simultaneously the session's first and last event
(duration ignored per spec).

**DBT implementation:**
- `models/marts/status_group_duration_shards/q2_status_group_duration_shard_{0..7}.sql`
  — 8 shard models, each building the `LEAD()` duration directly off the
  *matching* Q1 shard (`int_events_sessioned_shard_{i}`, not the unioned
  view), then joining `stg_statuses` → `stg_status_groups` and aggregating.
  See **Scalability audit** above: a naive single-model version of this
  measured 276.69s at 50M rows even using only built-in window functions, so
  it gets the same treatment as Q1.
- `models/marts/q2_status_group_duration.sql` — `UNION ALL` of the 8 shards;
  this is the model everything outside this project should actually query.
- **Verified live:** confirmed the sharded output is identical to the
  original single-model version's output on the sample data.

---

## Q3 — Dimension Refresh

Explain-only per the assignment; no SQL/models required. Covered in
[talking points shared separately] — summary: dbt's default `table`
materialization drops and recreates the table, which breaks dependent views
(`DROP` needs `CASCADE`) and can expose readers to a locked/half-built table
mid-refresh. Preferred fix for a 90k-row table refreshed hourly: an
**incremental** model (merge/upsert by key) so the table object is never
dropped, or a Postgres `materialized_view` with
`REFRESH MATERIALIZED VIEW CONCURRENTLY` if dbt's `materialized_view`
materialization is available.

---

## Q4 — Agent Range Model

**Problem.** For each event, find the agent assigned to that customer at the
time it occurred.

**Tables used:** `events`, `agent_change_log` (append-only history), and
**`customers`** (current agent — see the caveat below, this table matters
more than it looks like it should).

**The logic.**
1. Collapse `agent_change_log` into per-customer ranges:
   `valid_from = changed_at`, `valid_to = LEAD(changed_at) OVER (PARTITION BY customer ORDER BY changed_at)`.
   `valid_to IS NULL` means "still active." This is a **materialize once,
   join many times** design — point-in-time lookups become a simple range
   comparison against a small table instead of re-scanning the full log
   (which matters once the log is 100M rows, Q4.2).
2. Join `events` to those ranges: `dt >= valid_from AND (dt < valid_to OR valid_to IS NULL)`.

**Caveat found and fixed after the first pass:** the assessment's background
text explicitly describes `customers.agent_id` as *"the current agent"* — but
the first version of this model never referenced `customers` at all, trusting
`agent_change_log`'s last entry for "now" instead. That's fine only as long as
the two sources agree, and nothing enforces that. Two concrete failure modes:
- **Drift** — a reassignment lands in `customers` before (or without) being
  logged; a log-only lookup silently serves the stale answer for that
  customer's most recent events.
- **No log history at all** — a customer with a current agent but zero
  `agent_change_log` rows would get `NULL` for every one of their events.

Both were reproduced against the live database (a synthetic no-log customer,
and a manually drifted `agent_id`) before being fixed, not just reasoned
about. Fix: `customers.agent_id` is now authoritative for whichever range is
**currently open** (nothing bounds it from a later log entry) — it wins over
the log's last value on conflict, and covers customers with no log rows at
all via a synthetic all-time-open range. Historical/closed ranges still come
only from the log, since `customers` has no opinion about the past — a
pre-log event correctly still returns `NULL` rather than fabricating history
(confirmed in the sample data: Alice's first event, 2023-05-15, predates her
first logged change).

**DBT implementation:**
- `models/intermediate/int_agent_ranges.sql` — builds the ranges, materialized
  **incremental** with a configurable lookback (`var: agent_ranges_lookback_days`,
  default 7 — see Q4.4: late-arriving log entries can retroactively change a
  *previous* range's `valid_to`, so a pure "only touch brand-new rows"
  incremental model isn't safe; the lookback window bounds how late an entry
  can arrive and still be corrected). Also applies the `customers` override
  described above, and adds a synthetic range for log-less customers.
  Post-hook creates the `(customer, valid_from)` index called for in Q4.4.
- `models/marts/q4_agent_point_in_time.sql` — joins `stg_events` to
  `int_agent_ranges` via the range comparison. No sharding needed here (see
  **Scalability audit** above) — just a `pre_hook` bumping
  `max_parallel_workers_per_gather` to 8 for this model's transaction, since
  Postgres's own planner already picks an efficient `Parallel Hash Join` for
  a plain equality join like this one.
- `tests/warn_agent_log_customers_mismatch.sql` — a `warn`-severity data test
  flagging any customer whose log history and `customers.agent_id` disagree.
  Doesn't fail the build (the model already resolves the conflict in favor of
  `customers`), but surfaces the drift as a signal worth investigating.

---

## Running it

```bash
dbt seed --target dev
dbt run --target dev
dbt test --target dev
```

`dbt_project.yml` pins seed column types explicitly (timestamps, varchars) so
staging models don't have to cast out of raw seeded text columns.

`profiles.yml`'s `test_demo.outputs.dev.threads` is set to 8 — this isn't
just a nice-to-have. Q1 and Q2's shard models only get their validated
performance if dbt actually dispatches all 8 independent shard models onto
separate connections at once; fewer threads means dbt queues them and runs
some sequentially, quietly losing the parallelism the whole design depends
on. If running against a smaller machine, drop both the thread count and the
shard count together (and re-benchmark — 8 was chosen because it's what
cleared 30s on the 14-core machine this was tested on, not a universal
constant).
