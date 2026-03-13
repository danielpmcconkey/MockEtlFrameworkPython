"""CardFraudFlagsProcessor — flag high-risk transactions over $500 with banker's rounding."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "card_txn_id", "card_id", "customer_id", "merchant_name", "mcc_code",
        "risk_level", "amount", "txn_timestamp", "ifw_effective_date",
    ]

    card_transactions: pd.DataFrame | None = shared_state.get("card_transactions")  # type: ignore[assignment]
    merchant_categories: pd.DataFrame | None = shared_state.get("merchant_categories")  # type: ignore[assignment]

    if card_transactions is None or card_transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build MCC -> risk_level lookup
    risk_lookup: dict[str, str] = {}
    if merchant_categories is not None and not merchant_categories.empty:
        for _, mcc in merchant_categories.iterrows():
            code = str(mcc["mcc_code"]) if pd.notna(mcc["mcc_code"]) else ""
            risk = str(mcc["risk_level"]) if pd.notna(mcc["risk_level"]) else ""
            risk_lookup[code] = risk

    output_rows = []
    for _, txn in card_transactions.iterrows():
        mcc_code = str(txn["merchant_category_code"]) if pd.notna(txn["merchant_category_code"]) else ""
        risk_level = risk_lookup.get(mcc_code, "")

        # W5: Banker's rounding on amount — Python round() uses banker's rounding
        amount = round(float(txn["amount"]), 2)

        # AP7: Magic value — hardcoded $500 threshold
        if risk_level == "High" and amount > 500:
            output_rows.append({
                "card_txn_id": txn["card_txn_id"],
                "card_id": txn["card_id"],
                "customer_id": txn["customer_id"],
                "merchant_name": txn["merchant_name"],
                "mcc_code": mcc_code,
                "risk_level": risk_level,
                "amount": amount,
                "txn_timestamp": txn["txn_timestamp"],
                "ifw_effective_date": txn["ifw_effective_date"],
            })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CardFraudFlagsProcessor", execute)
