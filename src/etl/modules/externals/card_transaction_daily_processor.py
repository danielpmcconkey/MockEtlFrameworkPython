"""CardTransactionDailyProcessor — daily card transaction summary by card type."""
from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "card_type", "txn_count", "total_amount", "avg_amount",
        "ifw_effective_date",
    ]

    max_date: date = shared_state.get(ETL_EFFECTIVE_DATE_KEY, date.today())  # type: ignore[assignment]

    card_transactions: pd.DataFrame | None = shared_state.get("card_transactions")  # type: ignore[assignment]
    cards: pd.DataFrame | None = shared_state.get("cards")  # type: ignore[assignment]

    # AP1: accounts and customers sourced but never used (dead-end)

    if (card_transactions is None or card_transactions.empty
            or cards is None or cards.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build card_id -> card_type lookup
    card_type_lookup: dict[int, str] = {}
    for _, card in cards.iterrows():
        card_id = int(card["card_id"])
        card_type = str(card.get("card_type", "") or "")
        card_type_lookup[card_id] = card_type

    # Group transactions by card_type
    groups: dict[str, tuple[int, Decimal]] = {}
    as_of = card_transactions.iloc[0]["ifw_effective_date"]

    for _, txn in card_transactions.iterrows():
        card_id = int(txn["card_id"])
        amount = Decimal(str(txn["amount"]))
        card_type = card_type_lookup.get(card_id, "Unknown")

        if card_type not in groups:
            groups[card_type] = (0, Decimal(0))

        current = groups[card_type]
        groups[card_type] = (current[0] + 1, current[1] + amount)

    output_rows = []
    for card_type, (count, total) in groups.items():
        avg_amount = round(total / count, 2) if count > 0 else Decimal(0)

        output_rows.append({
            "card_type": card_type,
            "txn_count": count,
            "total_amount": total,
            "avg_amount": avg_amount,
            "ifw_effective_date": as_of,
        })

    # W3b: End-of-month boundary — append monthly summary row
    days_in_month = calendar.monthrange(max_date.year, max_date.month)[1]
    if max_date.day == days_in_month:
        total_count = sum(g[0] for g in groups.values())
        total_amount = sum(g[1] for g in groups.values())

        output_rows.append({
            "card_type": "MONTHLY_TOTAL",
            "txn_count": total_count,
            "total_amount": total_amount,
            "avg_amount": round(total_amount / total_count, 2) if total_count > 0 else Decimal(0),
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CardTransactionDailyProcessor", execute)
