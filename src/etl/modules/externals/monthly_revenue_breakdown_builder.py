"""MonthlyRevenueBreakdownBuilder — daily revenue summary with quarterly bonus rows."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "revenue_source", "total_revenue", "transaction_count", "ifw_effective_date",
    ]

    overdraft_events: pd.DataFrame | None = shared_state.get("overdraft_events")  # type: ignore[assignment]
    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]

    max_date = shared_state["__etlEffectiveDate"]

    # Compute daily overdraft fee revenue (charged, not waived)
    overdraft_revenue = 0.0
    overdraft_count = 0
    if overdraft_events is not None:
        for _, row in overdraft_events.iterrows():
            fee_waived = bool(row["fee_waived"])
            if not fee_waived:
                overdraft_revenue += float(row["fee_amount"])
                overdraft_count += 1

    # Compute daily credit transaction revenue as proxy for interest
    credit_revenue = 0.0
    credit_count = 0
    if transactions is not None:
        for _, row in transactions.iterrows():
            txn_type = str(row["txn_type"]) if pd.notna(row["txn_type"]) else ""
            if txn_type == "Credit":
                credit_revenue += float(row["amount"])
                credit_count += 1

    # W5: Banker's rounding
    rows = [
        {
            "revenue_source": "overdraft_fees",
            "total_revenue": round(overdraft_revenue, 2),
            "transaction_count": overdraft_count,
            "ifw_effective_date": max_date,
        },
        {
            "revenue_source": "credit_interest_proxy",
            "total_revenue": round(credit_revenue, 2),
            "transaction_count": credit_count,
            "ifw_effective_date": max_date,
        },
    ]

    # W3c: End-of-quarter boundary — append quarterly summary rows on Oct 31
    # Fiscal quarter boundary: Q4 starts Nov 1, so Oct 31 is the last day of Q3
    if max_date.month == 10 and max_date.day == 31:
        q_overdraft_revenue = overdraft_revenue
        q_overdraft_count = overdraft_count
        q_credit_revenue = credit_revenue
        q_credit_count = credit_count

        rows.append({
            "revenue_source": "QUARTERLY_TOTAL_overdraft_fees",
            "total_revenue": round(q_overdraft_revenue, 2),
            "transaction_count": q_overdraft_count,
            "ifw_effective_date": max_date,
        })
        rows.append({
            "revenue_source": "QUARTERLY_TOTAL_credit_interest_proxy",
            "total_revenue": round(q_credit_revenue, 2),
            "transaction_count": q_credit_count,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.MonthlyRevenueBreakdownBuilder", execute)
