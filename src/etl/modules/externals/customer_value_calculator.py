"""CustomerValueCalculator — compute composite customer value scores."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "transaction_score", "balance_score", "visit_score",
        "composite_score", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    branch_visits: pd.DataFrame | None = shared_state.get("branch_visits")  # type: ignore[assignment]

    # Weekend guard on customers or accounts empty
    if (customers is None or customers.empty
            or accounts is None or accounts.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Scoring weights (C# uses decimal)
    transaction_weight = Decimal("0.4")
    balance_weight = Decimal("0.35")
    visit_weight = Decimal("0.25")

    # Build account_id -> customer_id lookup
    account_to_customer: dict[int, int] = {}
    for _, acct_row in accounts.iterrows():
        account_id = int(acct_row["account_id"])
        customer_id = int(acct_row["customer_id"])
        account_to_customer[account_id] = customer_id

    # Compute per-customer transaction counts (via account lookup)
    txn_count_by_customer: dict[int, int] = {}
    if transactions is not None:
        for _, txn_row in transactions.iterrows():
            account_id = int(txn_row["account_id"])
            customer_id = account_to_customer.get(account_id, 0)
            if customer_id == 0:
                continue
            if customer_id not in txn_count_by_customer:
                txn_count_by_customer[customer_id] = 0
            txn_count_by_customer[customer_id] += 1

    # Compute per-customer total account balance
    balance_by_customer: dict[int, Decimal] = {}
    for _, acct_row in accounts.iterrows():
        customer_id = int(acct_row["customer_id"])
        balance = Decimal(str(acct_row["current_balance"]))
        if customer_id not in balance_by_customer:
            balance_by_customer[customer_id] = Decimal("0")
        balance_by_customer[customer_id] += balance

    # Compute per-customer branch visit counts
    visit_count_by_customer: dict[int, int] = {}
    if branch_visits is not None:
        for _, visit_row in branch_visits.iterrows():
            customer_id = int(visit_row["customer_id"])
            if customer_id not in visit_count_by_customer:
                visit_count_by_customer[customer_id] = 0
            visit_count_by_customer[customer_id] += 1

    # Iterate customers, compute scores
    rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""

        # transaction_score: count * 10.0, capped at 1000
        txn_count = txn_count_by_customer.get(customer_id, 0)
        transaction_score = min(Decimal(str(txn_count)) * Decimal("10.0"), Decimal("1000"))

        # balance_score: total balance / 1000.0, capped at 1000
        total_balance = balance_by_customer.get(customer_id, Decimal("0"))
        balance_score = min(total_balance / Decimal("1000.0"), Decimal("1000"))

        # visit_score: count * 50.0, capped at 1000
        visit_count = visit_count_by_customer.get(customer_id, 0)
        visit_score = min(Decimal(str(visit_count)) * Decimal("50.0"), Decimal("1000"))

        # composite_score
        composite_score = (transaction_score * transaction_weight
                           + balance_score * balance_weight
                           + visit_score * visit_weight)

        # W5: C# Math.Round uses banker's rounding (MidpointRounding.ToEven)
        # Python's Decimal quantize with ROUND_HALF_EVEN matches this
        two_places = Decimal("0.01")
        rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "transaction_score": float(transaction_score.quantize(two_places, rounding=ROUND_HALF_EVEN)),
            "balance_score": float(balance_score.quantize(two_places, rounding=ROUND_HALF_EVEN)),
            "visit_score": float(visit_score.quantize(two_places, rounding=ROUND_HALF_EVEN)),
            "composite_score": float(composite_score.quantize(two_places, rounding=ROUND_HALF_EVEN)),
            "ifw_effective_date": cust_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerValueCalculator", execute)
