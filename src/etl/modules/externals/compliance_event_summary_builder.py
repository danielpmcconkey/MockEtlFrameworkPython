"""ComplianceEventSummaryBuilder — counts compliance events by type and status."""
from __future__ import annotations

from datetime import date

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "event_type", "status", "event_count", "ifw_effective_date",
    ]

    compliance_events: pd.DataFrame | None = shared_state.get("compliance_events")  # type: ignore[assignment]

    # W1: Sunday skip — return empty on Sundays
    max_date: date = shared_state.get(ETL_EFFECTIVE_DATE_KEY, date.today())  # type: ignore[assignment]
    if max_date.weekday() == 6:  # Sunday
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    if compliance_events is None or compliance_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # AP1: accounts sourced but never used (dead-end)

    as_of = compliance_events.iloc[0]["ifw_effective_date"]

    # Count events by (event_type, status)
    counts: dict[tuple[str, str], int] = {}
    for _, row in compliance_events.iterrows():
        event_type = str(row.get("event_type", "") or "")
        status = str(row.get("status", "") or "")
        key = (event_type, status)

        if key not in counts:
            counts[key] = 0
        counts[key] += 1

    output_rows = []
    for (event_type, status), count in counts.items():
        output_rows.append({
            "event_type": event_type,
            "status": status,
            "event_count": count,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.ComplianceEventSummaryBuilder", execute)
