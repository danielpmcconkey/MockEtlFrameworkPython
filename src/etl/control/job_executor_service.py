"""Single-date orchestrator — runs all jobs for one effective date.

Loads active jobs and dependencies, builds an execution plan via
topological sort, then runs each job in order.  SameDay dependencies
that failed earlier in this run cause downstream jobs to be skipped.
"""

from __future__ import annotations

import datetime
import logging
import traceback

import pandas as pd

from etl import job_runner
from etl.control import control_db
from etl.control.execution_plan import build as build_plan
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY

logger = logging.getLogger(__name__)


def run(
    effective_date: datetime.date,
    specific_job_name: str | None = None,
) -> None:
    """Execute all active jobs (or one named job) for *effective_date*.

    Parameters
    ----------
    effective_date:
        The logical business date to inject into every job pipeline.
    specific_job_name:
        If given, only that job is executed.  Raises ``ValueError`` if no
        active job matches (case-insensitive).
    """
    run_date = datetime.date.today()

    logger.info(
        "run_date = %s, effective_date = %s%s",
        run_date.isoformat(),
        effective_date.isoformat(),
        f", job = {specific_job_name}" if specific_job_name else "",
    )

    all_jobs = control_db.get_active_jobs()
    all_deps = control_db.get_all_dependencies()
    ever_succeeded = control_db.get_ever_succeeded_job_ids()

    # -----------------------------------------------------------------
    # Determine which jobs to consider
    # -----------------------------------------------------------------
    if specific_job_name is not None:
        match = next(
            (
                j
                for j in all_jobs
                if j.job_name.lower() == specific_job_name.lower()
            ),
            None,
        )
        if match is None:
            raise ValueError(
                f"No active job found with name '{specific_job_name}'."
            )
        jobs_to_consider = [match]
    else:
        jobs_to_consider = all_jobs

    plan = build_plan(jobs_to_consider, all_deps, ever_succeeded)

    if not plan:
        logger.info("Nothing to run.")
        return

    logger.info("%d job(s) in plan.", len(plan))

    # -----------------------------------------------------------------
    # Execute in dependency order
    # -----------------------------------------------------------------
    failed_this_run: set[int] = set()

    for job in plan:
        # Check whether any SameDay upstream has already failed.
        upstream_failed = any(
            dep.depends_on_job_id in failed_this_run
            for dep in all_deps
            if dep.job_id == job.job_id and dep.dependency_type == "SameDay"
        )

        if upstream_failed:
            logger.info(
                "Skipping '%s' — SameDay upstream failed.", job.job_name
            )
            skip_run_id = control_db.insert_run(
                job.job_id, run_date, None, None, 1, "dependency"
            )
            control_db.mark_skipped(skip_run_id)
            continue

        attempt_num = control_db.get_next_attempt_number(
            job.job_id, effective_date, effective_date
        )
        run_id = control_db.insert_run(
            job.job_id,
            run_date,
            effective_date,
            effective_date,
            attempt_num,
            "scheduler",
        )
        control_db.mark_running(run_id)

        logger.info(
            "Running '%s' eff=%s (run_id=%d, attempt=%d)...",
            job.job_name,
            effective_date.isoformat(),
            run_id,
            attempt_num,
        )

        try:
            initial_state: dict[str, object] = {
                ETL_EFFECTIVE_DATE_KEY: effective_date,
            }
            final_state = job_runner.run(job.job_conf_path, initial_state)

            # Sum row counts across all pandas DataFrames in the final state.
            frames = [
                v for v in final_state.values() if isinstance(v, pd.DataFrame)
            ]
            rows_processed: int | None = (
                sum(len(f) for f in frames) if frames else None
            )

            control_db.mark_succeeded(run_id, rows_processed)
            logger.info(
                "'%s' %s succeeded.", job.job_name, effective_date.isoformat()
            )

        except Exception:
            control_db.mark_failed(run_id, traceback.format_exc())
            failed_this_run.add(job.job_id)
            logger.exception(
                "'%s' %s FAILED.", job.job_name, effective_date.isoformat()
            )

    failures = len(failed_this_run)
    if failures == 0:
        logger.info("All jobs completed successfully.")
    else:
        logger.info("Done. %d job(s) failed.", failures)
