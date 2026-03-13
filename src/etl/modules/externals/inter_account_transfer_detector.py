"""InterAccountTransferDetector — detect matching debit/credit pairs across accounts."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "debit_txn_id", "credit_txn_id", "from_account_id", "to_account_id",
        "amount", "txn_timestamp", "ifw_effective_date",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    # accounts is sourced but not actually used in the C# logic
    _accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    if transactions is None or transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Collect debits and credits
    debits: list[tuple] = []
    credits: list[tuple] = []

    # AP6: Row-by-row iteration to separate debits and credits
    for _, row in transactions.iterrows():
        txn_id = int(row["transaction_id"])
        account_id = int(row["account_id"])
        amount = float(row["amount"])
        timestamp = str(row["txn_timestamp"]) if pd.notna(row["txn_timestamp"]) else ""
        txn_type = str(row["txn_type"]) if pd.notna(row["txn_type"]) else ""

        if txn_type == "Debit":
            debits.append((txn_id, account_id, amount, timestamp, row["ifw_effective_date"]))
        elif txn_type == "Credit":
            credits.append((txn_id, account_id, amount, timestamp, row["ifw_effective_date"]))

    # AP6: O(n^2) nested loop matching where SQL self-join would work
    matched_credits: set[int] = set()
    rows = []

    for debit in debits:
        d_txn_id, d_account_id, d_amount, d_timestamp, d_as_of = debit
        for credit in credits:
            c_txn_id, c_account_id, c_amount, c_timestamp, c_as_of = credit
            if c_txn_id in matched_credits:
                continue

            # Match: same amount, same timestamp, different accounts
            if (d_amount == c_amount
                    and d_timestamp == c_timestamp
                    and d_account_id != c_account_id):
                matched_credits.add(c_txn_id)

                rows.append({
                    "debit_txn_id": d_txn_id,
                    "credit_txn_id": c_txn_id,
                    "from_account_id": d_account_id,
                    "to_account_id": c_account_id,
                    "amount": d_amount,
                    "txn_timestamp": d_timestamp,
                    "ifw_effective_date": d_as_of,
                })

                break

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.InterAccountTransferDetector", execute)
