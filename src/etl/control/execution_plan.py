"""Topological sort of job dependencies for execution ordering.

Uses Kahn's algorithm. A dependency edge is only an ordering constraint
when it is unsatisfied:
  - SameDay dependencies are always treated as unsatisfied (checked at
    execution time, not plan time).
  - Latest dependencies are satisfied when the upstream job has ever
    succeeded (its job_id is in ever_succeeded_ids).
"""

from __future__ import annotations

import logging
from collections import deque

from etl.control.control_db import JobDependency, JobRegistration

log = logging.getLogger(__name__)


def build(
    jobs: list[JobRegistration],
    deps: list[JobDependency],
    ever_succeeded_ids: set[int],
) -> list[JobRegistration]:
    """Return *jobs* in an order that respects unsatisfied dependencies.

    Both endpoints of an edge must be in *jobs* for the edge to matter.

    Raises ``ValueError`` if a cycle is detected among unsatisfied edges.
    """
    to_run_by_id: dict[int, JobRegistration] = {j.job_id: j for j in jobs}

    # Build adjacency list and in-degree map over ordering-relevant edges.
    in_degree: dict[int, int] = {jid: 0 for jid in to_run_by_id}
    downstream: dict[int, list[int]] = {jid: [] for jid in to_run_by_id}

    for dep in deps:
        upstream_id = dep.depends_on_job_id
        downstream_id = dep.job_id

        # Both jobs must be in the run set for this edge to matter.
        if upstream_id not in to_run_by_id or downstream_id not in to_run_by_id:
            continue

        # SameDay deps are never pre-satisfied — ordering must be enforced.
        # Latest deps are satisfied once the upstream has ever succeeded.
        satisfied = (
            dep.dependency_type != "SameDay"
            and upstream_id in ever_succeeded_ids
        )

        if satisfied:
            continue

        downstream[upstream_id].append(downstream_id)
        in_degree[downstream_id] += 1

    # Kahn's algorithm.
    queue: deque[int] = deque(
        jid for jid, deg in in_degree.items() if deg == 0
    )
    result: list[JobRegistration] = []

    while queue:
        jid = queue.popleft()
        result.append(to_run_by_id[jid])

        for next_id in downstream[jid]:
            in_degree[next_id] -= 1
            if in_degree[next_id] == 0:
                queue.append(next_id)

    if len(result) != len(to_run_by_id):
        raise ValueError(
            "Cycle detected in the job dependency graph. "
            "Cannot build a valid execution plan."
        )

    log.debug("Execution plan: %d job(s) ordered", len(result))
    return result
