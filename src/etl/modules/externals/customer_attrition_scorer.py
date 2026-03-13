"""CustomerAttritionScorer — computes attrition risk scores using double-precision arithmetic."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "account_count", "txn_count", "avg_balance",
        "attrition_score", "risk_level", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")
    accounts: pd.DataFrame | None = shared_state.get("accounts")
    transactions: pd.DataFrame | None = shared_state.get("transactions")

    if customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]

    # Build per-customer account counts and balances
    account_count_by_customer: dict[int, int] = {}
    balance_by_customer: dict[int, Decimal] = {}
    if accounts is not None:
        for _, row in accounts.iterrows():
            cust_id = int(row["customer_id"])
            account_count_by_customer[cust_id] = account_count_by_customer.get(cust_id, 0) + 1
            balance_by_customer[cust_id] = balance_by_customer.get(cust_id, Decimal(0)) + Decimal(str(row["current_balance"]))

    # Build account_id -> customer_id lookup
    account_to_customer: dict[int, int] = {}
    if accounts is not None:
        for _, row in accounts.iterrows():
            account_to_customer[int(row["account_id"])] = int(row["customer_id"])

    # Build per-customer transaction counts
    txn_count_by_customer: dict[int, int] = {}
    if transactions is not None:
        for _, row in transactions.iterrows():
            acct_id = int(row["account_id"])
            cust_id = account_to_customer.get(acct_id, 0)
            if cust_id == 0:
                continue
            txn_count_by_customer[cust_id] = txn_count_by_customer.get(cust_id, 0) + 1

    # AP6: Row-by-row iteration computing attrition score
    output_rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        acct_count = account_count_by_customer.get(customer_id, 0)
        txn_count = txn_count_by_customer.get(customer_id, 0)
        total_balance = balance_by_customer.get(customer_id, Decimal(0))
        avg_balance = total_balance / acct_count if acct_count > 0 else Decimal(0)

        # W6: Double epsilon — use float instead of decimal for score accumulation
        dormancy_factor = 1.0 if acct_count == 0 else 0.0
        # AP7: Magic threshold — txn_count < 3 = "declining"
        declining_txn_factor = 1.0 if txn_count < 3 else 0.0
        # AP7: Magic threshold — balance < 100 = "low"
        low_balance_factor = 1.0 if float(avg_balance) < 100.0 else 0.0

        # W6: Double accumulation with floating-point errors
        attrition_score = 0.0
        attrition_score += dormancy_factor * 40.0
        attrition_score += declining_txn_factor * 35.0
        attrition_score += low_balance_factor * 25.0

        if attrition_score >= 75.0:
            risk_level = "High"
        elif attrition_score >= 40.0:
            risk_level = "Medium"
        else:
            risk_level = "Low"

        output_rows.append({
            "customer_id": customer_id,
            "first_name": str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else "",
            "last_name": str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else "",
            "account_count": acct_count,
            "txn_count": txn_count,
            "avg_balance": float(round(avg_balance, 2)),
            "attrition_score": attrition_score,
            "risk_level": risk_level,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerAttritionScorer", execute)
