"""LoanRiskCalculator — compute risk tier from average credit scores per customer."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "loan_id", "customer_id", "loan_type", "current_balance",
        "interest_rate", "loan_status", "avg_credit_score", "risk_tier", "ifw_effective_date",
    ]

    loan_accounts: pd.DataFrame | None = shared_state.get("loan_accounts")  # type: ignore[assignment]
    credit_scores: pd.DataFrame | None = shared_state.get("credit_scores")  # type: ignore[assignment]

    if (loan_accounts is None or loan_accounts.empty
            or credit_scores is None or credit_scores.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Group credit scores by customer_id to compute avg score per customer
    score_list_by_customer: dict[int, list[float]] = {}
    for _, row in credit_scores.iterrows():
        cust_id = int(row["customer_id"])
        score = float(row["score"])

        if cust_id not in score_list_by_customer:
            score_list_by_customer[cust_id] = []
        score_list_by_customer[cust_id].append(score)

    avg_score_by_customer: dict[int, float] = {}
    for cust_id, scores in score_list_by_customer.items():
        avg_score_by_customer[cust_id] = sum(scores) / len(scores)

    # For each loan, look up customer's avg credit score and compute risk tier
    rows = []
    for _, loan_row in loan_accounts.iterrows():
        customer_id = int(loan_row["customer_id"])

        if customer_id in avg_score_by_customer:
            avg_score = avg_score_by_customer[customer_id]
            avg_credit_score: object = avg_score

            if avg_score >= 750:
                risk_tier = "Low Risk"
            elif avg_score >= 650:
                risk_tier = "Medium Risk"
            elif avg_score >= 550:
                risk_tier = "High Risk"
            else:
                risk_tier = "Very High Risk"
        else:
            avg_credit_score = None
            risk_tier = "Unknown"

        rows.append({
            "loan_id": loan_row["loan_id"],
            "customer_id": loan_row["customer_id"],
            "loan_type": loan_row["loan_type"],
            "current_balance": loan_row["current_balance"],
            "interest_rate": loan_row["interest_rate"],
            "loan_status": loan_row["loan_status"],
            "avg_credit_score": avg_credit_score,
            "risk_tier": risk_tier,
            "ifw_effective_date": loan_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.LoanRiskCalculator", execute)
