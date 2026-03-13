"""OverdraftByAccountTypeProcessor — overdraft rate by account type with W4 integer division."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_type", "account_count", "overdraft_count", "overdraft_rate",
        "ifw_effective_date",
    ]

    overdraft_events: pd.DataFrame | None = shared_state.get("overdraft_events")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    if (
        overdraft_events is None or overdraft_events.empty
        or accounts is None or accounts.empty
    ):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    as_of = overdraft_events.iloc[0]["ifw_effective_date"]

    # Build account_id -> account_type lookup (AP6: row-by-row iteration)
    account_type_lookup: dict[int, str] = {}
    for _, acct in accounts.iterrows():
        account_id = int(acct["account_id"])
        account_type = str(acct["account_type"]) if pd.notna(acct["account_type"]) else ""
        account_type_lookup[account_id] = account_type

    # Count accounts per type
    account_counts: dict[str, int] = {}
    for _, acct in accounts.iterrows():
        account_type = str(acct["account_type"]) if pd.notna(acct["account_type"]) else ""
        if account_type not in account_counts:
            account_counts[account_type] = 0
        account_counts[account_type] += 1

    # AP6: Row-by-row iteration to count overdrafts per account_type
    overdraft_counts: dict[str, int] = {}
    for _, evt in overdraft_events.iterrows():
        account_id = int(evt["account_id"])
        account_type = account_type_lookup.get(account_id, "Unknown")

        if account_type not in overdraft_counts:
            overdraft_counts[account_type] = 0
        overdraft_counts[account_type] += 1

    rows = []
    for account_type, account_count in account_counts.items():
        od_count = overdraft_counts.get(account_type, 0)

        # W4: Integer division -- overdraft_count / account_count both int -> truncates to 0
        overdraft_rate = Decimal(od_count // account_count)

        rows.append({
            "account_type": account_type,
            "account_count": account_count,
            "overdraft_count": od_count,
            "overdraft_rate": overdraft_rate,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.OverdraftByAccountTypeProcessor", execute)
