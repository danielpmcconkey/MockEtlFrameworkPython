"""Scans date-partitioned directories and returns the latest partition."""

from __future__ import annotations

from datetime import date
from pathlib import Path


def find_latest_partition(
    job_dir: str, *, before: date | None = None
) -> str | None:
    job_path = Path(job_dir)
    if not job_path.is_dir():
        return None

    dates: list[str] = []
    for entry in job_path.iterdir():
        if entry.is_dir():
            try:
                d = date.fromisoformat(entry.name)
                if before is not None and d >= before:
                    continue
                dates.append(entry.name)
            except ValueError:
                pass

    if not dates:
        return None

    dates.sort(reverse=True)
    return dates[0]
