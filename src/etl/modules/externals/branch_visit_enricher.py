"""BranchVisitEnricher — enrich branch visits with branch and customer names."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "visit_id", "customer_id", "first_name", "last_name",
        "branch_id", "branch_name", "visit_timestamp", "visit_purpose", "ifw_effective_date",
    ]

    branch_visits: pd.DataFrame | None = shared_state.get("branch_visits")  # type: ignore[assignment]
    branches: pd.DataFrame | None = shared_state.get("branches")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    # Weekend guard on customers empty (matches C# order of checks)
    if customers is None or customers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    if branch_visits is None or branch_visits.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build branch_id -> branch_name lookup
    branch_names: dict[int, str] = {}
    if branches is not None and not branches.empty:
        for _, row in branches.iterrows():
            branch_id = int(row["branch_id"])
            branch_name = str(row["branch_name"]) if pd.notna(row["branch_name"]) else ""
            branch_names[branch_id] = branch_name

    # Build customer_id -> (first_name, last_name) lookup
    customer_names: dict[int, tuple[str, str]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""
        customer_names[cust_id] = (first_name, last_name)

    # Row-by-row: iterate branch_visits, enrich with branch_name and customer name
    output_rows = []
    for _, visit_row in branch_visits.iterrows():
        customer_id = int(visit_row["customer_id"])
        branch_id = int(visit_row["branch_id"])

        branch_name = branch_names.get(branch_id, "")
        # C# GetValueOrDefault with (null!, null!) — when key missing, returns (null, null)
        first_name, last_name = customer_names.get(customer_id, (None, None))

        output_rows.append({
            "visit_id": visit_row["visit_id"],
            "customer_id": visit_row["customer_id"],
            "first_name": first_name,
            "last_name": last_name,
            "branch_id": visit_row["branch_id"],
            "branch_name": branch_name,
            "visit_timestamp": visit_row["visit_timestamp"],
            "visit_purpose": visit_row["visit_purpose"],
            "ifw_effective_date": visit_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.BranchVisitEnricher", execute)
