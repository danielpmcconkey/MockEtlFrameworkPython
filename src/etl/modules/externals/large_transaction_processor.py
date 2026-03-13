"""LargeTransactionProcessor — filter transactions > $500 and join customer names."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "transaction_id", "account_id", "customer_id", "first_name", "last_name",
        "txn_type", "amount", "description", "txn_timestamp", "ifw_effective_date",
    ]

    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if (accounts is None or accounts.empty
            or customers is None or customers.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    if transactions is None or transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build account_id -> customer_id lookup
    account_to_customer: dict[int, int] = {}
    for _, acct_row in accounts.iterrows():
        account_id = int(acct_row["account_id"])
        customer_id = int(acct_row["customer_id"])
        account_to_customer[account_id] = customer_id

    # Build customer_id -> (first_name, last_name) lookup
    customer_names: dict[int, tuple[str, str]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""
        customer_names[cust_id] = (first_name, last_name)

    # Iterate transactions, filter amount > 500
    rows = []
    for _, txn_row in transactions.iterrows():
        amount = float(txn_row["amount"])
        if amount > 500:
            account_id = int(txn_row["account_id"])
            customer_id = account_to_customer.get(account_id, 0)
            first_name, last_name = customer_names.get(customer_id, ("", ""))

            rows.append({
                "transaction_id": txn_row["transaction_id"],
                "account_id": txn_row["account_id"],
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "txn_type": txn_row["txn_type"],
                "amount": txn_row["amount"],
                "description": txn_row["description"],
                "txn_timestamp": txn_row["txn_timestamp"],
                "ifw_effective_date": txn_row["ifw_effective_date"],
            })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.LargeTransactionProcessor", execute)
