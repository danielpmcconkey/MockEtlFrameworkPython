"""DoNotContactProcessor — find customers opted out of ALL preferences, with W1 Sunday skip."""
from __future__ import annotations

from datetime import date

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "ifw_effective_date",
    ]

    max_date: date = shared_state.get(ETL_EFFECTIVE_DATE_KEY, date.today())  # type: ignore[assignment]

    # W1: Sunday skip -- return empty DataFrame on Sundays
    if max_date.weekday() == 6:  # Sunday
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    prefs: pd.DataFrame | None = shared_state.get("customer_preferences")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if (prefs is None or prefs.empty
            or customers is None or customers.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    for _, row in customers.iterrows():
        cust_id = int(row["id"])
        first_name = str(row["first_name"]) if pd.notna(row["first_name"]) else ""
        last_name = str(row["last_name"]) if pd.notna(row["last_name"]) else ""
        customer_lookup[cust_id] = (first_name, last_name)

    # AP6: Row-by-row -- find customers opted out of ALL preferences
    customer_prefs: dict[int, tuple[int, int]] = {}  # (total, opted_out)
    for _, row in prefs.iterrows():
        cust_id = int(row["customer_id"])
        opted_in = bool(row["opted_in"])

        if cust_id not in customer_prefs:
            customer_prefs[cust_id] = (0, 0)

        current = customer_prefs[cust_id]
        if not opted_in:
            customer_prefs[cust_id] = (current[0] + 1, current[1] + 1)
        else:
            customer_prefs[cust_id] = (current[0] + 1, current[1])

    as_of = prefs.iloc[0]["ifw_effective_date"]

    rows = []
    for cust_id, (total, opted_out) in customer_prefs.items():
        # Customer opted out of ALL preferences
        if total > 0 and total == opted_out and cust_id in customer_lookup:
            first_name, last_name = customer_lookup[cust_id]
            rows.append({
                "customer_id": cust_id,
                "first_name": first_name,
                "last_name": last_name,
                "ifw_effective_date": as_of,
            })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.DoNotContactProcessor", execute)
