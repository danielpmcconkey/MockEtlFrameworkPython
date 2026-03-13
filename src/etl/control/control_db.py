"""Data access layer for the control schema.

All methods open and close their own connections — no connection pooling
state is held between calls.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass

import psycopg

from etl import connection_helper

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# Data classes
# -------------------------------------------------------------------------

@dataclass(frozen=True)
class JobRegistration:
    """A row from control.jobs — a registered ETL job."""

    job_id: int
    job_name: str
    description: str | None
    job_conf_path: str
    is_active: bool


@dataclass(frozen=True)
class JobDependency:
    """A row from control.job_dependencies — a directed edge in the job graph.

    job_id is the downstream job; depends_on_job_id is the upstream job
    that must succeed first.
    """

    job_id: int
    depends_on_job_id: int
    dependency_type: str = "SameDay"


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _connect() -> psycopg.Connection:
    return psycopg.connect(connection_helper.get_connection_string())


# -------------------------------------------------------------------------
# Read
# -------------------------------------------------------------------------

def get_active_jobs() -> list[JobRegistration]:
    """Return all active job registrations, ordered by job_id."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_id, job_name, description, job_conf_path, is_active "
                "FROM control.jobs WHERE is_active = true ORDER BY job_id"
            )
            return [
                JobRegistration(
                    job_id=row[0],
                    job_name=row[1],
                    description=row[2],
                    job_conf_path=row[3],
                    is_active=row[4],
                )
                for row in cur.fetchall()
            ]


def get_all_dependencies() -> list[JobDependency]:
    """Return every row from control.job_dependencies."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_id, depends_on_job_id, dependency_type "
                "FROM control.job_dependencies"
            )
            return [
                JobDependency(
                    job_id=row[0],
                    depends_on_job_id=row[1],
                    dependency_type=row[2],
                )
                for row in cur.fetchall()
            ]


def get_succeeded_job_ids(run_date: datetime.date) -> set[int]:
    """Return job IDs that have a Succeeded run for the given run_date."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT job_id FROM control.job_runs "
                "WHERE run_date = %s AND status = 'Succeeded'",
                (run_date,),
            )
            return {row[0] for row in cur.fetchall()}


def get_ever_succeeded_job_ids() -> set[int]:
    """Return job IDs that have ever had a Succeeded run for any run_date."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT job_id FROM control.job_runs "
                "WHERE status = 'Succeeded'"
            )
            return {row[0] for row in cur.fetchall()}


def get_last_succeeded_max_effective_date(
    job_id: int,
) -> datetime.date | None:
    """Return the latest max_effective_date among all Succeeded runs for this
    job, or None if the job has never succeeded.

    Used by the executor to determine the gap-fill start date.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(max_effective_date) FROM control.job_runs "
                "WHERE job_id = %s AND status = 'Succeeded' "
                "AND max_effective_date IS NOT NULL",
                (job_id,),
            )
            row = cur.fetchone()
            return row[0] if row is not None and row[0] is not None else None


def get_next_attempt_number(
    job_id: int,
    min_effective_date: datetime.date,
    max_effective_date: datetime.date,
) -> int:
    """Return the next attempt number for the given (job, date range) pair.

    Attempt number increments on each retry of the same effective date range.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1 "
                "FROM control.job_runs "
                "WHERE job_id = %s "
                "AND min_effective_date = %s "
                "AND max_effective_date = %s",
                (job_id, min_effective_date, max_effective_date),
            )
            row = cur.fetchone()
            return int(row[0])


# -------------------------------------------------------------------------
# Write
# -------------------------------------------------------------------------

def insert_run(
    job_id: int,
    run_date: datetime.date,
    min_effective_date: datetime.date | None,
    max_effective_date: datetime.date | None,
    attempt_number: int,
    triggered_by: str,
) -> int:
    """Insert a Pending run record and return the new run_id.

    min_effective_date / max_effective_date may be None for Skipped rows
    where no data was processed.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO control.job_runs "
                "(job_id, run_date, min_effective_date, max_effective_date, "
                "attempt_number, status, triggered_by) "
                "VALUES (%s, %s, %s, %s, %s, 'Pending', %s) "
                "RETURNING run_id",
                (
                    job_id,
                    run_date,
                    min_effective_date,
                    max_effective_date,
                    attempt_number,
                    triggered_by,
                ),
            )
            run_id = cur.fetchone()[0]
            conn.commit()
            return int(run_id)


def mark_running(run_id: int) -> None:
    """Set a run to Running status with a started_at timestamp."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE control.job_runs "
                "SET status = 'Running', started_at = now() "
                "WHERE run_id = %s",
                (run_id,),
            )
            conn.commit()


def mark_succeeded(run_id: int, rows_processed: int | None) -> None:
    """Set a run to Succeeded status with a completed_at timestamp."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE control.job_runs "
                "SET status = 'Succeeded', completed_at = now(), "
                "rows_processed = %s "
                "WHERE run_id = %s",
                (rows_processed, run_id),
            )
            conn.commit()


def mark_failed(run_id: int, error_message: str) -> None:
    """Set a run to Failed status with a completed_at timestamp and error."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE control.job_runs "
                "SET status = 'Failed', completed_at = now(), "
                "error_message = %s "
                "WHERE run_id = %s",
                (error_message, run_id),
            )
            conn.commit()


def mark_skipped(run_id: int) -> None:
    """Set a run to Skipped status with a completed_at timestamp."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE control.job_runs "
                "SET status = 'Skipped', completed_at = now() "
                "WHERE run_id = %s",
                (run_id,),
            )
            conn.commit()
