"""CustomerCreditSummaryBuilder — aggregates credit scores, loan balances, and account balances per customer."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "avg_credit_score",
        "total_loan_balance", "total_account_balance", "loan_count",
        "account_count", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")
    accounts: pd.DataFrame | None = shared_state.get("accounts")
    credit_scores: pd.DataFrame | None = shared_state.get("credit_scores")
    loan_accounts: pd.DataFrame | None = shared_state.get("loan_accounts")

    if (customers is None or len(customers) == 0
            or accounts is None or len(accounts) == 0
            or credit_scores is None or len(credit_scores) == 0
            or loan_accounts is None or len(loan_accounts) == 0):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Group credit scores by customer_id
    scores_by_customer: dict[int, list[Decimal]] = {}
    for _, row in credit_scores.iterrows():
        cust_id = int(row["customer_id"])
        score = Decimal(str(row["score"]))
        if cust_id not in scores_by_customer:
            scores_by_customer[cust_id] = []
        scores_by_customer[cust_id].append(score)

    # Group loan balances by customer_id
    loans_by_customer: dict[int, tuple[Decimal, int]] = {}
    for _, row in loan_accounts.iterrows():
        cust_id = int(row["customer_id"])
        balance = Decimal(str(row["current_balance"]))
        if cust_id not in loans_by_customer:
            loans_by_customer[cust_id] = (Decimal(0), 0)
        current_bal, current_count = loans_by_customer[cust_id]
        loans_by_customer[cust_id] = (current_bal + balance, current_count + 1)

    # Group account balances by customer_id
    accounts_by_customer: dict[int, tuple[Decimal, int]] = {}
    for _, row in accounts.iterrows():
        cust_id = int(row["customer_id"])
        balance = Decimal(str(row["current_balance"]))
        if cust_id not in accounts_by_customer:
            accounts_by_customer[cust_id] = (Decimal(0), 0)
        current_bal, current_count = accounts_by_customer[cust_id]
        accounts_by_customer[cust_id] = (current_bal + balance, current_count + 1)

    # Build output row per customer
    output_rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""

        # Avg credit score — C# uses LINQ .Average() which returns decimal
        if customer_id in scores_by_customer:
            scores = scores_by_customer[customer_id]
            avg_credit_score = sum(scores) / len(scores)
        else:
            avg_credit_score = None  # C# uses DBNull.Value

        # Loan totals
        total_loan_balance = Decimal(0)
        loan_count = 0
        if customer_id in loans_by_customer:
            total_loan_balance, loan_count = loans_by_customer[customer_id]

        # Account totals
        total_account_balance = Decimal(0)
        account_count = 0
        if customer_id in accounts_by_customer:
            total_account_balance, account_count = accounts_by_customer[customer_id]

        output_rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "avg_credit_score": avg_credit_score,
            "total_loan_balance": total_loan_balance,
            "total_account_balance": total_account_balance,
            "loan_count": loan_count,
            "account_count": account_count,
            "ifw_effective_date": cust_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerCreditSummaryBuilder", execute)
