"""CreditScoreProcessor — passthrough of credit score rows."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "credit_score_id", "customer_id", "bureau", "score",
        "ifw_effective_date",
    ]

    credit_scores: pd.DataFrame | None = shared_state.get("credit_scores")  # type: ignore[assignment]

    if credit_scores is None or credit_scores.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Pass-through: copy all credit score rows
    output_rows = []
    for _, row in credit_scores.iterrows():
        output_rows.append({
            "credit_score_id": row["credit_score_id"],
            "customer_id": row["customer_id"],
            "bureau": row["bureau"],
            "score": row["score"],
            "ifw_effective_date": row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CreditScoreProcessor", execute)
