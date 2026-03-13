"""HighRiskMerchantActivityProcessor — filter card transactions to high-risk MCC codes."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "card_txn_id", "merchant_name", "mcc_code", "mcc_description", "amount",
        "txn_timestamp", "ifw_effective_date",
    ]

    card_transactions: pd.DataFrame | None = shared_state.get("card_transactions")  # type: ignore[assignment]
    merchant_categories: pd.DataFrame | None = shared_state.get("merchant_categories")  # type: ignore[assignment]

    if card_transactions is None or card_transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build MCC lookup
    mcc_lookup: dict[str, tuple[str, str]] = {}
    if merchant_categories is not None:
        for _, mcc_row in merchant_categories.iterrows():
            code = str(mcc_row["mcc_code"]) if pd.notna(mcc_row["mcc_code"]) else ""
            description = str(mcc_row["mcc_description"]) if pd.notna(mcc_row["mcc_description"]) else ""
            risk_level = str(mcc_row["risk_level"]) if pd.notna(mcc_row["risk_level"]) else ""
            mcc_lookup[code] = (description, risk_level)

    # AP6: Row-by-row iteration to join and filter (SQL would work)
    rows = []
    for _, txn in card_transactions.iterrows():
        mcc_code = str(txn["merchant_category_code"]) if pd.notna(txn["merchant_category_code"]) else ""
        if mcc_code not in mcc_lookup:
            continue

        description, risk_level = mcc_lookup[mcc_code]
        # AP7: Magic value — hardcoded risk level string
        if risk_level == "High":
            rows.append({
                "card_txn_id": txn["card_txn_id"],
                "merchant_name": txn["merchant_name"],
                "mcc_code": mcc_code,
                "mcc_description": description,
                "amount": txn["amount"],
                "txn_timestamp": txn["txn_timestamp"],
                "ifw_effective_date": txn["ifw_effective_date"],
            })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.HighRiskMerchantActivityProcessor", execute)
