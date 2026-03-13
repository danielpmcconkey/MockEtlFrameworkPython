"""InvestmentRiskClassifier — classify investments by value tier with asymmetric null handling."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "investment_id", "customer_id", "account_type",
        "current_value", "risk_profile", "risk_tier", "ifw_effective_date",
    ]

    investments: pd.DataFrame | None = shared_state.get("investments")  # type: ignore[assignment]

    if investments is None or investments.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    rows = []
    for _, row in investments.iterrows():
        investment_id = int(row["investment_id"])
        customer_id = int(row["customer_id"])
        account_type = str(row["account_type"]) if pd.notna(row["account_type"]) else ""

        # AP5: Asymmetric NULLs — null current_value -> 0, but null risk_profile -> "Unknown"
        current_value = float(row["current_value"]) if pd.notna(row["current_value"]) else 0.0
        risk_profile = str(row["risk_profile"]) if pd.notna(row["risk_profile"]) else "Unknown"

        # AP7: Magic values — hardcoded thresholds for risk tier
        if current_value > 200000:
            risk_tier = "High Value"
        elif current_value > 50000:
            risk_tier = "Medium Value"
        else:
            risk_tier = "Low Value"

        rows.append({
            "investment_id": investment_id,
            "customer_id": customer_id,
            "account_type": account_type,
            "current_value": current_value,
            "risk_profile": risk_profile,
            "risk_tier": risk_tier,
            "ifw_effective_date": row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.InvestmentRiskClassifier", execute)
