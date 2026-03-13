"""HighBalanceFilter — filter accounts with balance > 10000 and join customer names."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_id", "customer_id", "account_type", "current_balance",
        "first_name", "last_name", "ifw_effective_date",
    ]

    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if (accounts is None or accounts.empty
            or customers is None or customers.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer_id -> (first_name, last_name) lookup
    customer_names: dict[int, tuple[str, str]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""
        customer_names[cust_id] = (first_name, last_name)

    rows = []
    for _, acct_row in accounts.iterrows():
        balance = Decimal(str(acct_row["current_balance"]))
        if balance > 10000:
            customer_id = int(acct_row["customer_id"])
            first_name, last_name = customer_names.get(customer_id, ("", ""))

            rows.append({
                "account_id": acct_row["account_id"],
                "customer_id": acct_row["customer_id"],
                "account_type": acct_row["account_type"],
                "current_balance": acct_row["current_balance"],
                "first_name": first_name,
                "last_name": last_name,
                "ifw_effective_date": acct_row["ifw_effective_date"],
            })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.HighBalanceFilter", execute)
