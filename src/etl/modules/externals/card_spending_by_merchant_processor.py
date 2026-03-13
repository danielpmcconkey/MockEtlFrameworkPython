"""CardSpendingByMerchantProcessor — groups card transactions by MCC code."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "mcc_code", "mcc_description", "txn_count", "total_spending",
        "ifw_effective_date",
    ]

    card_transactions: pd.DataFrame | None = shared_state.get("card_transactions")  # type: ignore[assignment]
    merchant_categories: pd.DataFrame | None = shared_state.get("merchant_categories")  # type: ignore[assignment]

    if card_transactions is None or card_transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build MCC lookup
    mcc_lookup: dict[str, str] = {}
    if merchant_categories is not None:
        for _, row in merchant_categories.iterrows():
            code = str(row.get("mcc_code", "") or "")
            desc = str(row.get("mcc_description", "") or "")
            mcc_lookup[code] = desc

    as_of = card_transactions.iloc[0]["ifw_effective_date"]

    # AP6: Row-by-row iteration to group by MCC
    groups: dict[str, tuple[int, Decimal]] = {}
    for _, txn in card_transactions.iterrows():
        mcc_code = str(txn.get("merchant_category_code", "") or "")
        amount = Decimal(str(txn["amount"]))

        if mcc_code not in groups:
            groups[mcc_code] = (0, Decimal(0))

        current = groups[mcc_code]
        groups[mcc_code] = (current[0] + 1, current[1] + amount)

    output_rows = []
    for mcc_code, (count, total) in groups.items():
        desc = mcc_lookup.get(mcc_code, "")
        output_rows.append({
            "mcc_code": mcc_code,
            "mcc_description": desc,
            "txn_count": count,
            "total_spending": total,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CardSpendingByMerchantProcessor", execute)
