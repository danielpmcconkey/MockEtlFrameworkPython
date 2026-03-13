"""CreditScoreAverager — averages credit scores per customer across bureaus."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "avg_score",
        "equifax_score", "transunion_score", "experian_score",
        "ifw_effective_date",
    ]

    credit_scores: pd.DataFrame | None = shared_state.get("credit_scores")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if (credit_scores is None or credit_scores.empty
            or customers is None or customers.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Group credit scores by customer_id
    scores_by_customer: dict[int, list[tuple[str, Decimal]]] = {}
    for _, row in credit_scores.iterrows():
        cust_id = int(row["customer_id"])
        bureau = str(row.get("bureau", "") or "")
        score = Decimal(str(row["score"]))

        if cust_id not in scores_by_customer:
            scores_by_customer[cust_id] = []
        scores_by_customer[cust_id].append((bureau, score))

    # Build customer_id -> (first_name, last_name, ifw_effective_date) lookup
    customer_names: dict[int, tuple[str, str, object]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        first_name = str(cust_row.get("first_name", "") or "")
        last_name = str(cust_row.get("last_name", "") or "")
        customer_names[cust_id] = (first_name, last_name, cust_row["ifw_effective_date"])

    # For each customer with credit scores, compute averages and individual bureau scores
    output_rows = []
    for customer_id, scores in scores_by_customer.items():
        if customer_id not in customer_names:
            continue

        first_name, last_name, as_of = customer_names[customer_id]

        avg_score = sum(s for _, s in scores) / len(scores)

        # Look up individual bureau scores
        equifax_score = None
        transunion_score = None
        experian_score = None

        for bureau, score in scores:
            match bureau.lower():
                case "equifax":
                    equifax_score = score
                case "transunion":
                    transunion_score = score
                case "experian":
                    experian_score = score

        output_rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "avg_score": avg_score,
            "equifax_score": equifax_score,
            "transunion_score": transunion_score,
            "experian_score": experian_score,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CreditScoreAverager", execute)
