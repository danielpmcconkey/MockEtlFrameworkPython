"""Multi-threaded service that polls control.task_queue for pending tasks.

Threading model:
  - N worker threads (configurable via TaskQueueSettings.thread_count)
  - Each worker claims ALL pending tasks for one job via
    pg_try_advisory_xact_lock(hashtext(job_name))
  - Tasks within a job are processed sequentially in effective_date order
  - Different jobs run in parallel across threads
  - If a task fails, remaining tasks in the batch are marked Failed

Exit behaviour:
  - A watchdog thread checks once per minute whether time since any worker
    last found work exceeds idle_shutdown_seconds (default 8 hours).
"""

from __future__ import annotations

import datetime
import logging
import threading
import time
from dataclasses import dataclass

import pandas as pd
import psycopg

from etl import connection_helper, job_runner
from etl.app_config import AppConfig, TaskQueueSettings
from etl.control import control_db
from etl.control.control_db import JobRegistration
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# Data classes
# -------------------------------------------------------------------------

@dataclass(frozen=True)
class TaskQueueItem:
    """A claimed task from the queue."""

    task_id: int
    job_name: str
    effective_date: datetime.date


# -------------------------------------------------------------------------
# Queue DB operations
# -------------------------------------------------------------------------

def _claim_next_job_batch() -> list[TaskQueueItem] | None:
    """Claim all pending tasks for a single job using advisory locks.

    Returns None if no work is available.
    """
    conn = psycopg.connect(connection_helper.get_connection_string())
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT job_name FROM control.task_queue "
                    "WHERE status = 'Pending' ORDER BY job_name"
                )
                job_names = [row[0] for row in cur.fetchall()]

                if not job_names:
                    return None

                claimed_job: str | None = None
                for job_name in job_names:
                    cur.execute(
                        "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                        (job_name,),
                    )
                    got_lock = cur.fetchone()[0]
                    if got_lock:
                        claimed_job = job_name
                        break

                if claimed_job is None:
                    return None

                cur.execute(
                    "UPDATE control.task_queue "
                    "SET status = 'Running', started_at = NOW() "
                    "WHERE task_id IN ("
                    "    SELECT task_id FROM control.task_queue "
                    "    WHERE status = 'Pending' AND job_name = %s "
                    "    FOR UPDATE SKIP LOCKED"
                    ") "
                    "RETURNING task_id, job_name, effective_date",
                    (claimed_job,),
                )
                rows = cur.fetchall()

        tasks = [
            TaskQueueItem(task_id=r[0], job_name=r[1], effective_date=r[2])
            for r in rows
        ]
        tasks.sort(key=lambda t: t.effective_date)
        return tasks
    finally:
        conn.close()


def _mark_task_succeeded(task_id: int) -> None:
    with psycopg.connect(connection_helper.get_connection_string()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE control.task_queue "
                "SET status = 'Succeeded', completed_at = NOW() "
                "WHERE task_id = %s",
                (task_id,),
            )
        conn.commit()


def _mark_task_failed(task_id: int, error_message: str) -> None:
    with psycopg.connect(connection_helper.get_connection_string()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE control.task_queue "
                "SET status = 'Failed', completed_at = NOW(), "
                "error_message = %s "
                "WHERE task_id = %s",
                (error_message, task_id),
            )
        conn.commit()


# -------------------------------------------------------------------------
# Service
# -------------------------------------------------------------------------

class TaskQueueService:
    """Long-running queue-based executor."""

    def __init__(self, config: AppConfig) -> None:
        self._config: TaskQueueSettings = config.task_queue
        self._thread_count: int = self._config.thread_count
        self._shutdown_requested: bool = False
        self._activity_lock = threading.Lock()
        self._last_activity: float = time.monotonic()
        self._jobs_by_name: dict[str, JobRegistration] = {}

    def _record_activity(self) -> None:
        with self._activity_lock:
            self._last_activity = time.monotonic()

    def run(self) -> None:
        """Start worker threads, watchdog, and block until all workers exit."""
        logger.info("Starting queue executor...")
        logger.info("%d worker thread(s)", self._thread_count)

        jobs = control_db.get_active_jobs()
        self._jobs_by_name = {j.job_name.lower(): j for j in jobs}
        logger.info("Loaded %d active job(s) from registry.", len(self._jobs_by_name))

        threads: list[threading.Thread] = []
        try:
            for i in range(self._thread_count):
                label = f"W{i}"
                t = threading.Thread(
                    target=self._worker_loop,
                    args=(label,),
                    name=f"QueueWorker-{label}",
                    daemon=True,
                )
                t.start()
                threads.append(t)

            watchdog = threading.Thread(
                target=self._watchdog_loop,
                name="IdleWatchdog",
                daemon=True,
            )
            watchdog.start()

            for t in threads:
                t.join()

            logger.info("All threads finished. Shutting down.")
        except Exception:
            logger.exception("FATAL error in TaskQueueService")
            self._shutdown_requested = True
            for t in threads:
                t.join(timeout=10)
            raise

    def _watchdog_loop(self) -> None:
        logger.info(
            "Idle shutdown threshold: %ds", self._config.idle_shutdown_seconds
        )
        while not self._shutdown_requested:
            time.sleep(60)
            with self._activity_lock:
                idle_seconds = time.monotonic() - self._last_activity
            if idle_seconds >= self._config.idle_shutdown_seconds:
                hours = int(idle_seconds // 3600)
                mins = int((idle_seconds % 3600) // 60)
                logger.info(
                    "No work found for %d:%02d. Signalling shutdown.",
                    hours,
                    mins,
                )
                self._shutdown_requested = True
                return

    def _worker_loop(self, label: str) -> None:
        logger.info("[%s] Worker started", label)
        poll_seconds = self._config.poll_interval_ms / 1000.0

        while not self._shutdown_requested:
            try:
                batch = _claim_next_job_batch()

                if not batch:
                    time.sleep(poll_seconds)
                    continue

                self._record_activity()
                logger.info(
                    "[%s] Claimed %d task(s) for job '%s'",
                    label,
                    len(batch),
                    batch[0].job_name,
                )
                self._process_batch(batch, label)

            except Exception:
                logger.exception("[%s] ERROR (no task context)", label)

        logger.info("[%s] Worker exiting.", label)

    def _process_batch(
        self, batch: list[TaskQueueItem], label: str
    ) -> None:
        for i, task in enumerate(batch):
            if self._shutdown_requested:
                return

            try:
                logger.info(
                    "[%s] Running task %d: %s @ %s (%d/%d)",
                    label,
                    task.task_id,
                    task.job_name,
                    task.effective_date.isoformat(),
                    i + 1,
                    len(batch),
                )
                self._execute_task(task, label)

                _mark_task_succeeded(task.task_id)
                logger.info(
                    "[%s] Task %d SUCCEEDED: %s @ %s",
                    label,
                    task.task_id,
                    task.job_name,
                    task.effective_date.isoformat(),
                )

            except Exception as exc:
                _mark_task_failed(task.task_id, str(exc))
                logger.error(
                    "[%s] Task %d FAILED: %s @ %s -- %s",
                    label,
                    task.task_id,
                    task.job_name,
                    task.effective_date.isoformat(),
                    exc,
                )

                for j in range(i + 1, len(batch)):
                    remaining = batch[j]
                    _mark_task_failed(
                        remaining.task_id,
                        f"Skipped: prior task {task.task_id} "
                        f"({task.job_name} @ {task.effective_date.isoformat()}) failed",
                    )
                    logger.warning(
                        "[%s] Task %d SKIPPED: %s @ %s (prior failure)",
                        label,
                        remaining.task_id,
                        remaining.job_name,
                        remaining.effective_date.isoformat(),
                    )
                return

    def _execute_task(self, task: TaskQueueItem, label: str) -> None:
        """Run a single task: look up the job, record a job_run, run the pipeline."""
        job = self._jobs_by_name.get(task.job_name.lower())

        if job is None:
            logger.info(
                "[%s] Job '%s' not in cache. Reloading registry...",
                label,
                task.job_name,
            )
            jobs = control_db.get_active_jobs()
            self._jobs_by_name = {j.job_name.lower(): j for j in jobs}
            job = self._jobs_by_name.get(task.job_name.lower())
            if job is None:
                raise ValueError(
                    f"No active job found with name '{task.job_name}'"
                )

        initial_state: dict[str, object] = {
            ETL_EFFECTIVE_DATE_KEY: task.effective_date,
        }

        run_date = datetime.date.today()
        attempt_num = control_db.get_next_attempt_number(
            job.job_id, task.effective_date, task.effective_date
        )
        run_id = control_db.insert_run(
            job.job_id,
            run_date,
            task.effective_date,
            task.effective_date,
            attempt_num,
            "queue",
        )
        control_db.mark_running(run_id)

        try:
            final_state = job_runner.run(job.job_conf_path, initial_state)

            rows_processed: int | None = None
            frames = [
                v for v in final_state.values() if isinstance(v, pd.DataFrame)
            ]
            if frames:
                rows_processed = sum(len(f) for f in frames)

            control_db.mark_succeeded(run_id, rows_processed)
        except Exception:
            control_db.mark_failed(
                run_id,
                f"See task_queue task_id={task.task_id} for details",
            )
            raise
