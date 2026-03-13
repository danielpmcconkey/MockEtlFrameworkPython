"""DormantAccountDetector — find accounts with no transactions on target date, with W2 weekend fallback."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_id", "customer_id", "first_name", "last_name",
        "account_type", "current_balance", "ifw_effective_date",
    ]

    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if accounts is None or accounts.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W2: Weekend fallback to Friday
    max_date: date = shared_state[ETL_EFFECTIVE_DATE_KEY]  # type: ignore[assignment]
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    target_date_str = target_date.isoformat()

    # Build set of account_ids that have transactions on the target date
    active_accounts: set[int] = set()
    if transactions is not None:
        # AP6: Row-by-row iteration where SQL set operation would do
        for _, txn_row in transactions.iterrows():
            as_of = txn_row["ifw_effective_date"]
            # C# compares DateOnly == targetDate; in Python the DataFrame value
            # could be a date or a string, so normalise to string for comparison
            as_of_str = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
            if as_of_str == target_date_str:
                account_id = int(txn_row["account_id"])
                active_accounts.add(account_id)

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    if customers is not None:
        for _, cust_row in customers.iterrows():
            cust_id = int(cust_row["id"])
            first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
            last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""
            customer_lookup[cust_id] = (first_name, last_name)

    # AP6: Row-by-row iteration to find dormant accounts
    rows = []
    for _, acct_row in accounts.iterrows():
        account_id = int(acct_row["account_id"])
        customer_id = int(acct_row["customer_id"])

        # Account is dormant if it has zero transactions on the target date
        if account_id not in active_accounts:
            first_name, last_name = customer_lookup.get(customer_id, ("", ""))

            rows.append({
                "account_id": account_id,
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "account_type": acct_row["account_type"],
                "current_balance": acct_row["current_balance"],
                # C# outputs targetDate.ToString("yyyy-MM-dd")
                "ifw_effective_date": target_date_str,
            })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.DormantAccountDetector", execute)
