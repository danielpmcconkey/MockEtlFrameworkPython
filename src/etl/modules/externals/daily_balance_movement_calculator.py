"""DailyBalanceMovementCalculator — compute debit/credit totals per account using float arithmetic (W6)."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_id", "customer_id", "debit_total", "credit_total",
        "net_movement", "ifw_effective_date",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    if (transactions is None or transactions.empty
            or accounts is None or accounts.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build account_id -> customer_id lookup
    account_to_customer: dict[int, int] = {}
    for _, acct_row in accounts.iterrows():
        account_id = int(acct_row["account_id"])
        customer_id = int(acct_row["customer_id"])
        account_to_customer[account_id] = customer_id

    # W6: Use float (double) arithmetic instead of decimal (epsilon errors)
    stats: dict[int, tuple[float, float, object]] = {}
    for _, row in transactions.iterrows():
        account_id = int(row["account_id"])
        txn_type = str(row["txn_type"]) if pd.notna(row["txn_type"]) else ""
        amount = float(row["amount"])

        if account_id not in stats:
            stats[account_id] = (0.0, 0.0, row["ifw_effective_date"])

        current = stats[account_id]
        if txn_type == "Debit":
            stats[account_id] = (current[0] + amount, current[1], current[2])
        elif txn_type == "Credit":
            stats[account_id] = (current[0], current[1] + amount, current[2])

    rows = []
    for account_id, (debit_total, credit_total, as_of) in stats.items():
        customer_id = account_to_customer.get(account_id, 0)

        # W6: net_movement computed with float arithmetic (epsilon errors accumulate)
        net_movement = credit_total - debit_total

        rows.append({
            "account_id": account_id,
            "customer_id": customer_id,
            "debit_total": debit_total,
            "credit_total": credit_total,
            "net_movement": net_movement,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.DailyBalanceMovementCalculator", execute)
