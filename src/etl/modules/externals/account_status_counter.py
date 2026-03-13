"""AccountStatusCounter — count accounts by (type, status)."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_type", "account_status", "account_count", "ifw_effective_date",
    ]

    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    if accounts is None or accounts.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Get ifw_effective_date from first account row
    as_of = accounts.iloc[0]["ifw_effective_date"]

    # Build (account_type, account_status) -> count dictionary
    counts: dict[tuple[str, str], int] = {}
    for _, acct_row in accounts.iterrows():
        account_type = str(acct_row["account_type"]) if pd.notna(acct_row["account_type"]) else ""
        account_status = str(acct_row["account_status"]) if pd.notna(acct_row["account_status"]) else ""
        key = (account_type, account_status)

        if key not in counts:
            counts[key] = 0
        counts[key] += 1

    rows = []
    for (acct_type, acct_status), count in counts.items():
        rows.append({
            "account_type": acct_type,
            "account_status": acct_status,
            "account_count": count,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.AccountStatusCounter", execute)
