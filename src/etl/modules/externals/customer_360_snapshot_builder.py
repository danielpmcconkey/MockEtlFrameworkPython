"""Customer360SnapshotBuilder — builds a 360-degree customer snapshot with account, card, and investment aggregates."""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pandas as pd

from etl.modules.external import register
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "account_count", "total_balance", "card_count",
        "investment_count", "total_investment_value", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")
    accounts: pd.DataFrame | None = shared_state.get("accounts")
    cards: pd.DataFrame | None = shared_state.get("cards")
    investments: pd.DataFrame | None = shared_state.get("investments")

    if customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W2: Weekend fallback to Friday
    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    # Filter customers to target date
    filtered_customers = customers[customers["ifw_effective_date"] == target_date]

    # Build per-customer account counts and balances
    account_count_by_customer: dict[int, int] = {}
    balance_by_customer: dict[int, Decimal] = {}
    if accounts is not None:
        for _, row in accounts.iterrows():
            if row["ifw_effective_date"] != target_date:
                continue
            cust_id = int(row["customer_id"])
            account_count_by_customer[cust_id] = account_count_by_customer.get(cust_id, 0) + 1
            balance_by_customer[cust_id] = balance_by_customer.get(cust_id, Decimal(0)) + Decimal(str(row["current_balance"]))

    # Build per-customer card counts
    card_count_by_customer: dict[int, int] = {}
    if cards is not None:
        for _, row in cards.iterrows():
            if row["ifw_effective_date"] != target_date:
                continue
            cust_id = int(row["customer_id"])
            card_count_by_customer[cust_id] = card_count_by_customer.get(cust_id, 0) + 1

    # Build per-customer investment counts and values
    investment_count_by_customer: dict[int, int] = {}
    investment_value_by_customer: dict[int, Decimal] = {}
    if investments is not None:
        for _, row in investments.iterrows():
            if row["ifw_effective_date"] != target_date:
                continue
            cust_id = int(row["customer_id"])
            investment_count_by_customer[cust_id] = investment_count_by_customer.get(cust_id, 0) + 1
            investment_value_by_customer[cust_id] = investment_value_by_customer.get(cust_id, Decimal(0)) + Decimal(str(row["current_value"]))

    # AP6: Row-by-row iteration building full customer view
    output_rows = []
    for _, cust_row in filtered_customers.iterrows():
        customer_id = int(cust_row["id"])

        total_bal = balance_by_customer.get(customer_id, Decimal(0))
        total_inv = investment_value_by_customer.get(customer_id, Decimal(0))

        output_rows.append({
            "customer_id": customer_id,
            "first_name": str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else "",
            "last_name": str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else "",
            "account_count": account_count_by_customer.get(customer_id, 0),
            "total_balance": float(round(total_bal, 2)),
            "card_count": card_count_by_customer.get(customer_id, 0),
            "investment_count": investment_count_by_customer.get(customer_id, 0),
            "total_investment_value": float(round(total_inv, 2)),
            "ifw_effective_date": target_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.Customer360SnapshotBuilder", execute)
