"""CustomerAccountSummaryBuilder — aggregates account counts and balances per customer."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "account_count", "total_balance", "active_balance", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")
    accounts: pd.DataFrame | None = shared_state.get("accounts")

    # Weekend guard on customers or accounts empty
    if customers is None or len(customers) == 0 or accounts is None or len(accounts) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Group accounts by customer_id
    accounts_by_customer: dict[int, tuple[int, Decimal, Decimal]] = {}
    for _, acct_row in accounts.iterrows():
        cust_id = int(acct_row["customer_id"])
        balance = Decimal(str(acct_row["current_balance"]))
        status = str(acct_row["account_status"]) if pd.notna(acct_row["account_status"]) else ""

        if cust_id not in accounts_by_customer:
            accounts_by_customer[cust_id] = (0, Decimal(0), Decimal(0))

        count, total_bal, active_bal = accounts_by_customer[cust_id]
        active_add = balance if status == "Active" else Decimal(0)
        accounts_by_customer[cust_id] = (count + 1, total_bal + balance, active_bal + active_add)

    # Iterate customers, look up aggregated account data
    output_rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""

        account_count, total_balance, active_balance = accounts_by_customer.get(
            customer_id, (0, Decimal(0), Decimal(0))
        )

        output_rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "account_count": account_count,
            "total_balance": total_balance,
            "active_balance": active_balance,
            "ifw_effective_date": cust_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerAccountSummaryBuilder", execute)
