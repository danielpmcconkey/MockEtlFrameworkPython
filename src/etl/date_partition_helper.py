"""Scans date-partitioned directories and returns the latest partition."""

from __future__ import annotations

import os
from datetime import date


def find_latest_partition(job_dir: str) -> str | None:
    if not os.path.isdir(job_dir):
        return None

    dates: list[str] = []
    for name in os.listdir(job_dir):
        full = os.path.join(job_dir, name)
        if os.path.isdir(full):
            try:
                date.fromisoformat(name)
                dates.append(name)
            except ValueError:
                pass

    if not dates:
        return None

    dates.sort(reverse=True)
    return dates[0]
