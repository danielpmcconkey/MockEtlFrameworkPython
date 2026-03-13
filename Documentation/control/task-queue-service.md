# TaskQueueService

`src/etl/control/task_queue_service.py`

Long-running queue-based executor. Polls `control.task_queue` for pending tasks and processes them across multiple threads using a claim-by-job model. Eliminates Python startup overhead by paying the import/init cost once, then parallelises work across configurable threads.

## Threading Model

- **N threads**, all identical (configurable via `TaskQueueSettings.thread_count`, default 5)
- Each thread claims ALL pending tasks for a single job using PostgreSQL advisory locks
- Tasks within a job are processed sequentially in effective-date order
- Different jobs run in parallel across threads
- Each call to `_claim_next_job_batch()` opens and closes its own `psycopg` connection (psycopg is not thread-safe by default)

### Why Claim-by-Job

The model guarantees:

- Oct 1 Job A and Oct 2 Job B can run in parallel (different threads)
- Oct 1 Job A and Oct 2 Job A **never** run in parallel (same thread owns all of Job A's dates)
- Append-mode writes and CDC ordering are safe without any special per-job config

### Claim Flow

1. Query distinct job names with `status = 'Pending'`
2. Try `pg_try_advisory_xact_lock(hashtext(job_name))` on each until one succeeds
3. Claim all pending rows for that job, atomically setting them to `Running` (`FOR UPDATE SKIP LOCKED`)
4. Transaction commits (releases advisory lock -- rows are now `Running`)
5. Return tasks sorted by `effective_date`

### Batch Failure

If a task fails, all remaining tasks in the batch are marked `Failed` with a message referencing the failed task. This preserves ordering safety for append-mode jobs where later dates depend on earlier dates completing successfully.

## Task Execution

Each task goes through `_execute_task()`:

1. Look up the `JobRegistration` in a name-keyed cache (case-insensitive). If not found, reload the registry from the database.
2. Build the initial state dict with `ETL_EFFECTIVE_DATE_KEY` set to the task's `effective_date`.
3. Insert a `job_runs` record via `control_db` (`Pending -> Running`).
4. Run the pipeline through `job_runner.run()`.
5. Sum row counts across any `pandas.DataFrame` values in the final state and record `Succeeded` (or `Failed` on exception).

The `task_queue` row status is updated separately from the `job_runs` record -- the queue tracks task-level status while `job_runs` tracks pipeline-level status.

## Idle & Shutdown

- Workers call `_record_activity()` (thread-safe via `threading.Lock`) each time they claim a batch
- A dedicated watchdog thread checks once per minute whether the time since last activity exceeds `idle_shutdown_seconds`
- Default: 28,800 seconds (8 hours)
- When the threshold is reached, the watchdog sets `_shutdown_requested = True` and all workers exit their loops

No SIGINT handler. All worker and watchdog threads are `daemon=True`, so they terminate when the main thread exits.

### Internal Data Class

`TaskQueueItem` (frozen dataclass in `task_queue_service.py`) represents a claimed task: `task_id`, `job_name`, `effective_date`.

### Module-Level Helpers

Queue status updates are handled by module-level functions rather than methods on the class:

- `_claim_next_job_batch()` -- advisory-lock-based claim logic
- `_mark_task_succeeded(task_id)` -- sets task status to `Succeeded`
- `_mark_task_failed(task_id, error_message)` -- sets task status to `Failed` with error

## Configuration

Settings come from `AppConfig.task_queue` (`TaskQueueSettings`):

| Setting | Purpose |
|---|---|
| `thread_count` | Number of worker threads |
| `idle_shutdown_seconds` | Seconds of inactivity before automatic shutdown |
| `poll_interval_ms` | Milliseconds between poll attempts when no work is found |

## Queue Population

```sql
INSERT INTO control.task_queue (job_name, effective_date, execution_mode)
SELECT j.job_name, d.dt::date, 'parallel'
FROM control.jobs j
CROSS JOIN generate_series('2024-10-01'::date, '2024-12-31'::date, '1 day') d(dt)
WHERE j.is_active = true
ORDER BY d.dt, j.job_name;
```

## Monitoring

```sql
SELECT status, COUNT(*) FROM control.task_queue GROUP BY status;
```

## Lifecycle

1. Instantiate `TaskQueueService(config)` and call `.run()`
2. Populate the queue via SQL
3. Workers pick up work automatically via polling
4. Exits after idle timeout (default 8 hours)
