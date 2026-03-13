"""CrossSellCandidateFinder — identifies missing product cross-sell opportunities per customer."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "has_checking", "has_savings", "has_credit",
        "has_card", "has_investment", "missing_products", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")
    accounts: pd.DataFrame | None = shared_state.get("accounts")
    cards: pd.DataFrame | None = shared_state.get("cards")
    investments: pd.DataFrame | None = shared_state.get("investments")

    if customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]

    # Build per-customer account type sets
    account_types_by_customer: dict[int, set[str]] = {}
    if accounts is not None:
        for _, row in accounts.iterrows():
            cust_id = int(row["customer_id"])
            if cust_id not in account_types_by_customer:
                account_types_by_customer[cust_id] = set()
            acct_type = str(row["account_type"]) if pd.notna(row["account_type"]) else ""
            account_types_by_customer[cust_id].add(acct_type)

    # Build per-customer card presence
    customers_with_cards: set[int] = set()
    if cards is not None:
        for _, row in cards.iterrows():
            customers_with_cards.add(int(row["customer_id"]))

    # Build per-customer investment presence
    customers_with_investments: set[int] = set()
    if investments is not None:
        for _, row in investments.iterrows():
            customers_with_investments.add(int(row["customer_id"]))

    # AP6: Row-by-row iteration through customers
    output_rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        acct_types = account_types_by_customer.get(customer_id, set())

        has_checking = "Checking" in acct_types
        has_savings = "Savings" in acct_types
        has_credit = "Credit" in acct_types
        has_card = customer_id in customers_with_cards
        has_investment = customer_id in customers_with_investments

        missing = []
        if not has_checking:
            missing.append("Checking")
        if not has_savings:
            missing.append("Savings")
        if not has_credit:
            missing.append("Credit")

        # AP5: Asymmetric NULL handling — no card -> "No Card" string
        if not has_card:
            missing.append("No Card")

        # AP5: Asymmetric NULL handling — no investment -> 0 (different strategy)
        investment_value = 1 if has_investment else 0

        missing_products = "; ".join(missing) if len(missing) > 0 else "None"

        output_rows.append({
            "customer_id": customer_id,
            "first_name": str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else "",
            "last_name": str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else "",
            "has_checking": has_checking,
            "has_savings": has_savings,
            "has_credit": has_credit,
            "has_card": "Yes" if has_card else "No Card",
            "has_investment": investment_value,
            "missing_products": missing_products,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CrossSellCandidateFinder", execute)
