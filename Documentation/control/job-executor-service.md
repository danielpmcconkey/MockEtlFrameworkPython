# JobExecutorService

`src/etl/control/job_executor_service.py`

Public orchestrator for single-date job execution. Requires an explicit effective date -- no auto-advance or gap-fill.

## Execution Flow

1. Load active jobs and dependencies from `control` schema via `control_db`
2. Build topological execution plan via `execution_plan.build()`
3. For each job in plan order:
   a. Check dependency satisfaction -- if any `SameDay` upstream job failed earlier in this run, skip the downstream job
   b. Inject effective date into the initial state dict as `ETL_EFFECTIVE_DATE_KEY`
   c. Run the pipeline through `job_runner.run()`
   d. Record status: `Pending -> Running -> Succeeded / Failed`
4. Failed jobs' `SameDay` dependents are recorded as `Skipped`

### Row Counting

After a successful pipeline run, the executor inspects the final state dict and sums the row counts of all `pandas.DataFrame` values. This total is recorded as `rows_processed` on the `job_runs` record (or `None` if no DataFrames are present).

## Effective Date vs Run Date

- `run_date`: Calendar date the executor actually ran. Always `datetime.date.today()`. Set internally.
- `min_effective_date` / `max_effective_date`: Which data date the run processed. Supplied by the caller. For single-date runs, both are set to the same `effective_date` value.

These are separate concerns. A run on March 8 might process data for October 15.

## Usage

The module exposes a single `run()` function:

```python
from etl.control import job_executor_service

job_executor_service.run(effective_date)                # all active jobs
job_executor_service.run(effective_date, "SomeJobName") # one specific job
```

Accepts a required `effective_date` (`datetime.date`) and an optional `specific_job_name` (`str`). The job name match is case-insensitive. Raises `ValueError` if `specific_job_name` is given but no active job matches.
