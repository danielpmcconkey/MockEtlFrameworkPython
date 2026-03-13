"""CardTypeDistributionProcessor — card type counts with W6 float percentage."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "card_type", "card_count", "pct_of_total", "ifw_effective_date",
    ]

    cards: pd.DataFrame | None = shared_state.get("cards")  # type: ignore[assignment]

    if cards is None or cards.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    as_of = cards.iloc[0]["ifw_effective_date"]

    # Group by card_type
    counts: dict[str, int] = {}
    for _, card in cards.iterrows():
        card_type = str(card.get("card_type", "") or "")
        if card_type not in counts:
            counts[card_type] = 0
        counts[card_type] += 1

    total_cards = len(cards)

    # W6: Double epsilon — use float (double) instead of decimal for percentage
    output_rows = []
    for card_type, count in counts.items():
        pct = float(count) / float(total_cards)

        output_rows.append({
            "card_type": card_type,
            "card_count": count,
            "pct_of_total": pct,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CardTypeDistributionProcessor", execute)
