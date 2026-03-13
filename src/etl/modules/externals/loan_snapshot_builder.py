"""LoanSnapshotBuilder — passthrough snapshot of loan accounts, dropping date columns."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "loan_id", "customer_id", "loan_type", "original_amount",
        "current_balance", "interest_rate", "loan_status", "ifw_effective_date",
    ]

    loan_accounts: pd.DataFrame | None = shared_state.get("loan_accounts")  # type: ignore[assignment]

    if loan_accounts is None or loan_accounts.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Pass-through: copy loan rows, skipping origination_date and maturity_date
    rows = []
    for _, row in loan_accounts.iterrows():
        rows.append({
            "loan_id": row["loan_id"],
            "customer_id": row["customer_id"],
            "loan_type": row["loan_type"],
            "original_amount": row["original_amount"],
            "current_balance": row["current_balance"],
            "interest_rate": row["interest_rate"],
            "loan_status": row["loan_status"],
            "ifw_effective_date": row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.LoanSnapshotBuilder", execute)
