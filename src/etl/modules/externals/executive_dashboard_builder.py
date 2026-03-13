"""ExecutiveDashboardBuilder — compute aggregate KPI metrics for executive dashboard."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "metric_name", "metric_value", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    loan_accounts: pd.DataFrame | None = shared_state.get("loan_accounts")  # type: ignore[assignment]
    branch_visits: pd.DataFrame | None = shared_state.get("branch_visits")  # type: ignore[assignment]

    # Weekend guard on customers, accounts, or loan_accounts empty
    if (customers is None or customers.empty
            or accounts is None or accounts.empty
            or loan_accounts is None or loan_accounts.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Get ifw_effective_date from first customer row (or first transaction row as fallback)
    as_of = customers.iloc[0]["ifw_effective_date"]
    if pd.isna(as_of) and transactions is not None and not transactions.empty:
        as_of = transactions.iloc[0]["ifw_effective_date"]

    # 1. total_customers
    total_customers = Decimal(str(len(customers)))

    # 2. total_accounts
    total_accounts = Decimal(str(len(accounts)))

    # 3. total_balance = sum of all account current_balance
    total_balance = Decimal("0")
    for _, row in accounts.iterrows():
        total_balance += Decimal(str(row["current_balance"]))

    # 4. total_transactions & 5. total_txn_amount
    total_transactions = Decimal("0")
    total_txn_amount = Decimal("0")
    if transactions is not None:
        total_transactions = Decimal(str(len(transactions)))
        for _, row in transactions.iterrows():
            total_txn_amount += Decimal(str(row["amount"]))

    # 6. avg_txn_amount
    avg_txn_amount = total_txn_amount / total_transactions if total_transactions > 0 else Decimal("0")

    # 7. total_loans
    total_loans = Decimal(str(len(loan_accounts)))

    # 8. total_loan_balance
    total_loan_balance = Decimal("0")
    for _, row in loan_accounts.iterrows():
        total_loan_balance += Decimal(str(row["current_balance"]))

    # 9. total_branch_visits
    total_branch_visits = Decimal("0")
    if branch_visits is not None:
        total_branch_visits = Decimal(str(len(branch_visits)))

    # Build metric rows — W5: C# Math.Round uses banker's rounding
    two_places = Decimal("0.01")
    metrics = [
        ("total_customers", total_customers.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("total_accounts", total_accounts.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("total_balance", total_balance.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("total_transactions", total_transactions.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("total_txn_amount", total_txn_amount.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("avg_txn_amount", avg_txn_amount.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("total_loans", total_loans.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("total_loan_balance", total_loan_balance.quantize(two_places, rounding=ROUND_HALF_EVEN)),
        ("total_branch_visits", total_branch_visits.quantize(two_places, rounding=ROUND_HALF_EVEN)),
    ]

    rows = []
    for name, value in metrics:
        rows.append({
            "metric_name": name,
            "metric_value": float(value),
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.ExecutiveDashboardBuilder", execute)
