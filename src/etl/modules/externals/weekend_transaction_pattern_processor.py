"""WeekendTransactionPatternProcessor — classifies daily transactions as weekday/weekend with weekly summary on Sundays."""
from __future__ import annotations

from datetime import date as date_type
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def _to_date(raw: object) -> date_type:
    """Convert ifw_effective_date value to a date object."""
    if isinstance(raw, date_type):
        return raw
    # String or other — parse yyyy-mm-dd
    return date_type.fromisoformat(str(raw))


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "day_type", "txn_count", "total_amount", "avg_amount", "ifw_effective_date",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]

    if transactions is None or transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    date_str = str(max_date)

    # AP10: Over-sourced full date range via config; classify today's txns as weekend/weekday
    weekend_count = 0
    weekend_total = Decimal(0)
    weekday_count = 0
    weekday_total = Decimal(0)

    for _, row in transactions.iterrows():
        as_of = _to_date(row["ifw_effective_date"])
        if as_of != max_date:
            continue

        amount = Decimal(str(row["amount"]))

        # Python weekday(): 5=Saturday, 6=Sunday
        if as_of.weekday() in (5, 6):
            weekend_count += 1
            weekend_total += amount
        else:
            weekday_count += 1
            weekday_total += amount

    output_rows = []

    output_rows.append({
        "day_type": "Weekday",
        "txn_count": weekday_count,
        "total_amount": round(float(weekday_total), 2),
        "avg_amount": round(float(weekday_total / weekday_count), 2) if weekday_count > 0 else 0.0,
        "ifw_effective_date": date_str,
    })

    output_rows.append({
        "day_type": "Weekend",
        "txn_count": weekend_count,
        "total_amount": round(float(weekend_total), 2),
        "avg_amount": round(float(weekend_total / weekend_count), 2) if weekend_count > 0 else 0.0,
        "ifw_effective_date": date_str,
    })

    # W3a: End-of-week boundary -- append weekly summary row on Sundays
    if max_date.weekday() == 6:  # Sunday
        from datetime import timedelta
        monday_of_week = max_date - timedelta(days=6)

        wk_weekend_count = 0
        wk_weekend_total = Decimal(0)
        wk_weekday_count = 0
        wk_weekday_total = Decimal(0)

        for _, row in transactions.iterrows():
            as_of = _to_date(row["ifw_effective_date"])
            if as_of < monday_of_week or as_of > max_date:
                continue

            amount = Decimal(str(row["amount"]))

            if as_of.weekday() in (5, 6):
                wk_weekend_count += 1
                wk_weekend_total += amount
            else:
                wk_weekday_count += 1
                wk_weekday_total += amount

        output_rows.append({
            "day_type": "WEEKLY_TOTAL_Weekday",
            "txn_count": wk_weekday_count,
            "total_amount": round(float(wk_weekday_total), 2),
            "avg_amount": round(float(wk_weekday_total / wk_weekday_count), 2) if wk_weekday_count > 0 else 0.0,
            "ifw_effective_date": date_str,
        })

        output_rows.append({
            "day_type": "WEEKLY_TOTAL_Weekend",
            "txn_count": wk_weekend_count,
            "total_amount": round(float(wk_weekend_total), 2),
            "avg_amount": round(float(wk_weekend_total / wk_weekend_count), 2) if wk_weekend_count > 0 else 0.0,
            "ifw_effective_date": date_str,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.WeekendTransactionPatternProcessor", execute)
