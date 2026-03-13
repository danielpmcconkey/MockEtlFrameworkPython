"""CustomerBranchActivityBuilder — counts branch visits per customer."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "ifw_effective_date", "visit_count",
    ]

    branch_visits: pd.DataFrame | None = shared_state.get("branch_visits")
    customers: pd.DataFrame | None = shared_state.get("customers")

    # Weekend guard on customers empty
    if customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    if branch_visits is None or len(branch_visits) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup: customer_id -> (first_name, last_name)
    customer_names: dict[int, tuple[str, str]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""
        customer_names[cust_id] = (first_name, last_name)

    # Group branch_visits by customer_id, count visits per customer
    visit_counts: dict[int, int] = {}
    for _, visit_row in branch_visits.iterrows():
        cust_id = int(visit_row["customer_id"])
        if cust_id not in visit_counts:
            visit_counts[cust_id] = 0
        visit_counts[cust_id] += 1

    # Get ifw_effective_date from first branch_visit row
    as_of = branch_visits.iloc[0]["ifw_effective_date"]

    # Build output rows
    output_rows = []
    for customer_id, visit_count in visit_counts.items():
        first_name = None
        last_name = None
        if customer_id in customer_names:
            first_name, last_name = customer_names[customer_id]

        output_rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "ifw_effective_date": as_of,
            "visit_count": visit_count,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerBranchActivityBuilder", execute)
