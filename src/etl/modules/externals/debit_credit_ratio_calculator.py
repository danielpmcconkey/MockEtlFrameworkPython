"""DebitCreditRatioCalculator — compute debit/credit ratios per account with W4 integer division and W6 float arithmetic."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_id", "customer_id", "debit_count", "credit_count",
        "debit_credit_ratio", "debit_amount", "credit_amount",
        "amount_ratio", "ifw_effective_date",
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

    # Aggregate debit/credit counts and amounts per account
    # W6: Use float (double) arithmetic for amounts (epsilon errors)
    stats: dict[int, tuple[int, int, float, float, object]] = {}
    for _, row in transactions.iterrows():
        account_id = int(row["account_id"])
        txn_type = str(row["txn_type"]) if pd.notna(row["txn_type"]) else ""
        amount = float(row["amount"])

        if account_id not in stats:
            stats[account_id] = (0, 0, 0.0, 0.0, row["ifw_effective_date"])

        current = stats[account_id]
        if txn_type == "Debit":
            stats[account_id] = (current[0] + 1, current[1], current[2] + amount, current[3], current[4])
        elif txn_type == "Credit":
            stats[account_id] = (current[0], current[1] + 1, current[2], current[3] + amount, current[4])

    rows = []
    for account_id, (debit_count, credit_count, debit_amount, credit_amount, as_of) in stats.items():
        customer_id = account_to_customer.get(account_id, 0)

        # W4: Integer division — debit_count / credit_count (both int) -> truncates to 0
        debit_credit_ratio = debit_count // credit_count if credit_count > 0 else 0

        # W6: Double arithmetic for amount ratio (epsilon errors)
        amount_ratio = debit_amount / credit_amount if credit_amount > 0.0 else 0.0

        rows.append({
            "account_id": account_id,
            "customer_id": customer_id,
            "debit_count": debit_count,
            "credit_count": credit_count,
            "debit_credit_ratio": debit_credit_ratio,
            "debit_amount": debit_amount,
            "credit_amount": credit_amount,
            "amount_ratio": amount_ratio,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.DebitCreditRatioCalculator", execute)
