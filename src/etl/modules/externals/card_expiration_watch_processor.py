"""CardExpirationWatchProcessor — find cards expiring within 90 days with weekend fallback."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "card_id", "customer_id", "first_name", "last_name", "card_type",
        "expiration_date", "days_until_expiry", "ifw_effective_date",
    ]

    max_date: date = shared_state[ETL_EFFECTIVE_DATE_KEY]  # type: ignore[assignment]

    # W2: Weekend fallback — use Friday's date on Sat/Sun
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    cards: pd.DataFrame | None = shared_state.get("cards")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if cards is None or cards.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    if customers is not None and not customers.empty:
        for _, c in customers.iterrows():
            cust_id = int(c["id"])
            first = str(c["first_name"]) if pd.notna(c["first_name"]) else ""
            last = str(c["last_name"]) if pd.notna(c["last_name"]) else ""
            customer_lookup[cust_id] = (first, last)

    # AP6: Row-by-row iteration to find cards expiring within 90 days
    output_rows = []
    for _, card in cards.iterrows():
        expiration_date = card["expiration_date"]
        # C# uses DateOnly.DayNumber difference
        # In Python, use date subtraction to get days
        if isinstance(expiration_date, date):
            exp_date = expiration_date
        else:
            exp_date = pd.Timestamp(expiration_date).date()

        days_until_expiry = (exp_date - target_date).days

        if 0 <= days_until_expiry <= 90:
            cust_id = int(card["customer_id"])
            first, last = customer_lookup.get(cust_id, ("", ""))

            output_rows.append({
                "card_id": card["card_id"],
                "customer_id": cust_id,
                "first_name": first,
                "last_name": last,
                "card_type": card["card_type"],
                "expiration_date": exp_date,
                "days_until_expiry": days_until_expiry,
                "ifw_effective_date": target_date,
            })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CardExpirationWatchProcessor", execute)
