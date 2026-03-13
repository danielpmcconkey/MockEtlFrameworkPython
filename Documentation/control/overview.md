# Control Layer

Orchestration layer in `src/etl/control/`. Sits above `job_runner`. Reads job registrations and dependency graph from the PostgreSQL `control` schema, determines which jobs need to run, executes them in the correct order, and records the outcome of every run attempt.

## Database Schema

The `control` schema contains:

- `control.jobs` -- Job registrations (ID, name, description, conf path, active flag)
- `control.job_runs` -- Run history (run_date, effective dates, status, attempt number)
- `control.job_dependencies` -- Dependency graph (downstream job, upstream job, dependency type)
- `control.task_queue` -- Queue for long-running batch execution

## Data Classes

| Class | File | Purpose |
|---|---|---|
| `JobRegistration` | `control_db.py` | Frozen dataclass for a `control.jobs` row -- job ID, name, description, conf path, active flag |
| `JobDependency` | `control_db.py` | Frozen dataclass for a `control.job_dependencies` row -- downstream job ID, upstream job ID, dependency type (default `"SameDay"`) |

Both are defined in `control_db.py` alongside the data-access functions, rather than in separate model files.

## control_db

`control_db.py`. Module-level data-access functions for the control schema. Each function opens and closes its own `psycopg` connection -- no connection pooling state is held between calls.

**Reads:**
- `get_active_jobs` -- all active job registrations, ordered by job_id
- `get_all_dependencies` -- full dependency graph
- `get_succeeded_job_ids` -- jobs that succeeded for a given run_date
- `get_ever_succeeded_job_ids` -- jobs that have ever succeeded
- `get_last_succeeded_max_effective_date` -- latest successful effective date for a job (or `None`)
- `get_next_attempt_number` -- next attempt number for a (job, effective date range) pair

**Writes:**
- `insert_run` -- records run_date, effective date range, attempt number, triggered_by; returns the new run_id
- `mark_running`, `mark_succeeded`, `mark_failed`, `mark_skipped` -- status transitions with timestamps

## execution_plan

`execution_plan.py`. Module with a single public function `build()` that applies Kahn's topological sort to produce an ordered run list. Only unsatisfied dependency edges are counted: `SameDay` edges are always treated as unsatisfied during sorting (deferred to execution-time checking); a `Latest` edge is considered satisfied -- and removed from the graph -- if the upstream job has ever succeeded (its ID is in the `ever_succeeded_ids` set). Raises `ValueError` on cycle detection.

## Dependency Types

| Type | Semantics |
|---|---|
| `SameDay` | The upstream job must have a `Succeeded` run with the same `run_date` as the downstream job. Within a single executor invocation, jobs run in topological order, so this is satisfied naturally when A precedes B in the plan. |
| `Latest` | The upstream job must have succeeded at least once for any `run_date`. Used for one-time setup jobs or slowly-changing reference data. |

## Job Registration

```sql
INSERT INTO control.jobs (job_name, description, job_conf_path, is_active)
VALUES ('SomeJob', 'Description', 'conf/jobs/some_job.json', true)
ON CONFLICT (job_name) DO NOTHING;
```

## Orchestrator Reference

| Service | Doc |
|---|---|
| JobExecutorService (single-date runs) | [job-executor-service.md](job-executor-service.md) |
| TaskQueueService (long-running queue) | [task-queue-service.md](task-queue-service.md) |
