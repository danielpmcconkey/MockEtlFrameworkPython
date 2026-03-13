"""ComplianceOpenItemsBuilder — filters compliance events to open/escalated items."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "event_id", "customer_id", "first_name", "last_name",
        "event_type", "event_date", "status", "ifw_effective_date",
    ]

    compliance_events: pd.DataFrame | None = shared_state.get("compliance_events")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if compliance_events is None or compliance_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W2: Weekend fallback — use Friday's data on Sat/Sun
    max_date: date = shared_state[ETL_EFFECTIVE_DATE_KEY]  # type: ignore[assignment]
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    # AP4: unused columns sourced — review_date from compliance_events, prefix/suffix from customers

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    if customers is not None:
        for _, cust_row in customers.iterrows():
            cust_id = int(cust_row["id"])
            first_name = str(cust_row.get("first_name", "") or "")
            last_name = str(cust_row.get("last_name", "") or "")
            customer_lookup[cust_id] = (first_name, last_name)

    # Filter to target date rows and Open/Escalated status
    output_rows = []
    for _, row in compliance_events.iterrows():
        row_date = row["ifw_effective_date"]
        # Convert to date for comparison if needed
        if isinstance(row_date, pd.Timestamp):
            row_date = row_date.date()
        elif isinstance(row_date, str):
            row_date = date.fromisoformat(row_date)

        if row_date != target_date:
            continue

        status = str(row.get("status", "") or "")
        if status not in ("Open", "Escalated"):
            continue

        customer_id = int(row["customer_id"])
        first_name, last_name = customer_lookup.get(customer_id, ("", ""))

        output_rows.append({
            "event_id": row["event_id"],
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "event_type": row["event_type"],
            "event_date": row["event_date"],
            "status": status,
            "ifw_effective_date": target_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.ComplianceOpenItemsBuilder", execute)
