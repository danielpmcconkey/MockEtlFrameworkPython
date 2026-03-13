"""InvestmentAccountOverviewBuilder — join investments with customer names, skip Sundays."""
from __future__ import annotations

import datetime

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "investment_id", "customer_id", "first_name", "last_name",
        "account_type", "current_value", "risk_profile", "ifw_effective_date",
    ]

    investments: pd.DataFrame | None = shared_state.get("investments")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    # W1: Sunday skip — return empty DataFrame on Sundays
    max_date = shared_state.get("__etlEffectiveDate")
    if max_date is None:
        max_date = datetime.date.today()

    if max_date.weekday() == 6:  # Sunday
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    if (investments is None or investments.empty
            or customers is None or customers.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""
        customer_lookup[cust_id] = (first_name, last_name)

    rows = []
    for _, row in investments.iterrows():
        customer_id = int(row["customer_id"])
        first_name, last_name = customer_lookup.get(customer_id, ("", ""))

        rows.append({
            "investment_id": int(row["investment_id"]),
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "account_type": str(row["account_type"]) if pd.notna(row["account_type"]) else "",
            "current_value": float(row["current_value"]),
            "risk_profile": str(row["risk_profile"]) if pd.notna(row["risk_profile"]) else "",
            "ifw_effective_date": row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.InvestmentAccountOverviewBuilder", execute)
