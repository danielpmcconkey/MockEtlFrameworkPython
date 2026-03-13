"""CustomerTxnActivityBuilder — aggregate transaction counts and amounts per customer."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "ifw_effective_date", "transaction_count",
        "total_amount", "debit_count", "credit_count",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    # Weekend guard on accounts empty
    if accounts is None or accounts.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    if transactions is None or transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build account_id -> customer_id lookup
    account_to_customer: dict[int, int] = {}
    for _, acct_row in accounts.iterrows():
        account_id = int(acct_row["account_id"])
        customer_id = int(acct_row["customer_id"])
        account_to_customer[account_id] = customer_id

    # Group transactions by customer_id
    # (count, total_amount, debits, credits)
    customer_txns: dict[int, tuple[int, float, int, int]] = {}
    for _, txn_row in transactions.iterrows():
        account_id = int(txn_row["account_id"])
        customer_id = account_to_customer.get(account_id, 0)
        if customer_id == 0:
            continue

        amount = float(txn_row["amount"])
        txn_type = str(txn_row["txn_type"]) if pd.notna(txn_row["txn_type"]) else ""

        if customer_id not in customer_txns:
            customer_txns[customer_id] = (0, 0.0, 0, 0)

        current = customer_txns[customer_id]
        is_debit = 1 if txn_type == "Debit" else 0
        is_credit = 1 if txn_type == "Credit" else 0
        customer_txns[customer_id] = (
            current[0] + 1,
            current[1] + amount,
            current[2] + is_debit,
            current[3] + is_credit,
        )

    # Get ifw_effective_date from first transaction row
    as_of = transactions.iloc[0]["ifw_effective_date"]

    # Build output rows
    rows = []
    for cust_id, (count, total_amount, debits, credits) in customer_txns.items():
        rows.append({
            "customer_id": cust_id,
            "ifw_effective_date": as_of,
            "transaction_count": count,
            "total_amount": total_amount,
            "debit_count": debits,
            "credit_count": credits,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerTxnActivityBuilder", execute)
