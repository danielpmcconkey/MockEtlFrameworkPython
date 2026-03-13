"""AccountDistributionCalculator — percentage distribution by account type."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_type", "account_count", "total_accounts", "percentage", "ifw_effective_date",
    ]

    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    if accounts is None or accounts.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Get ifw_effective_date from first account row
    as_of = accounts.iloc[0]["ifw_effective_date"]
    total_accounts = len(accounts)

    # Count accounts by type
    type_counts: dict[str, int] = {}
    for _, acct_row in accounts.iterrows():
        account_type = str(acct_row["account_type"]) if pd.notna(acct_row["account_type"]) else ""
        if account_type not in type_counts:
            type_counts[account_type] = 0
        type_counts[account_type] += 1

    rows = []
    for acct_type, type_count in type_counts.items():
        # W6: float arithmetic (C# uses (double)typeCount / totalAccounts * 100.0)
        percentage = float(type_count) / total_accounts * 100.0

        rows.append({
            "account_type": acct_type,
            "account_count": type_count,
            "total_accounts": total_accounts,
            "percentage": percentage,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.AccountDistributionCalculator", execute)
