"""OverdraftCustomerProfileProcessor — per-customer overdraft profile with W2 weekend fallback."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "overdraft_count",
        "total_overdraft_amount", "avg_overdraft", "ifw_effective_date",
    ]

    effective_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
    if effective_date is not None:
        max_date = effective_date
    else:
        max_date = date.today()

    # W2: Weekend fallback -- use Friday's data on Sat/Sun
    target_date = max_date
    # date.weekday(): Monday=0 ... Saturday=5, Sunday=6
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    overdraft_events: pd.DataFrame | None = shared_state.get("overdraft_events")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    # AP1: accounts sourced but never used (dead-end)
    # AP4: prefix, suffix, birthdate sourced from customers but unused

    if (
        overdraft_events is None or overdraft_events.empty
        or customers is None or customers.empty
    ):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Filter overdraft events to target date
    target_date_str = str(target_date)
    filtered_events = overdraft_events[
        overdraft_events["ifw_effective_date"].apply(
            lambda d: str(d) == target_date_str
        )
    ]

    if filtered_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    for _, cust in customers.iterrows():
        cust_id = int(cust["id"])
        first_name = str(cust["first_name"]) if pd.notna(cust["first_name"]) else ""
        last_name = str(cust["last_name"]) if pd.notna(cust["last_name"]) else ""
        customer_lookup[cust_id] = (first_name, last_name)

    # Group overdraft events by customer
    customer_overdrafts: dict[int, tuple[int, Decimal]] = {}
    for _, evt in filtered_events.iterrows():
        customer_id = int(evt["customer_id"])
        amount = Decimal(str(evt["overdraft_amount"]))

        if customer_id not in customer_overdrafts:
            customer_overdrafts[customer_id] = (0, Decimal("0"))

        count, total_amount = customer_overdrafts[customer_id]
        customer_overdrafts[customer_id] = (count + 1, total_amount + amount)

    rows = []
    for cust_id, (count, total_amount) in customer_overdrafts.items():
        first_name, last_name = customer_lookup.get(cust_id, ("", ""))
        # C# uses Math.Round(..., 2) which is banker's rounding
        avg_overdraft = round(total_amount / count, 2) if count > 0 else Decimal("0")

        rows.append({
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "overdraft_count": count,
            "total_overdraft_amount": total_amount,
            "avg_overdraft": avg_overdraft,
            "ifw_effective_date": target_date_str,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.OverdraftCustomerProfileProcessor", execute)
