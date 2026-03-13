"""OverdraftDailySummaryProcessor — daily overdraft summary with W3a weekly total row."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "event_date", "overdraft_count", "total_overdraft_amount",
        "total_fees", "ifw_effective_date",
    ]

    effective_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
    if effective_date is not None:
        max_date = effective_date
    else:
        max_date = date.today()

    overdraft_events: pd.DataFrame | None = shared_state.get("overdraft_events")  # type: ignore[assignment]

    # AP1: transactions sourced but never used (dead-end)

    if overdraft_events is None or overdraft_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Group by ifw_effective_date for daily summary
    groups: dict[str, tuple[int, Decimal, Decimal]] = {}

    for _, row in overdraft_events.iterrows():
        as_of = str(row["ifw_effective_date"]) if pd.notna(row["ifw_effective_date"]) else ""
        amount = Decimal(str(row["overdraft_amount"]))
        fee = Decimal(str(row["fee_amount"]))

        if as_of not in groups:
            groups[as_of] = (0, Decimal("0"), Decimal("0"))

        count, total_amount, total_fees = groups[as_of]
        groups[as_of] = (count + 1, total_amount + amount, total_fees + fee)

    rows = []
    for as_of, (count, total_amount, total_fees) in groups.items():
        rows.append({
            "event_date": as_of,
            "overdraft_count": count,
            "total_overdraft_amount": total_amount,
            "total_fees": total_fees,
            "ifw_effective_date": as_of,
        })

    # W3a: End-of-week boundary -- append weekly summary row on Sundays
    # date.weekday(): Sunday=6
    if max_date.weekday() == 6:  # Sunday
        weekly_count = sum(g[0] for g in groups.values())
        weekly_amount = sum(g[1] for g in groups.values())
        weekly_fees = sum(g[2] for g in groups.values())

        rows.append({
            "event_date": "WEEKLY_TOTAL",
            "overdraft_count": weekly_count,
            "total_overdraft_amount": weekly_amount,
            "total_fees": weekly_fees,
            "ifw_effective_date": str(max_date),
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.OverdraftDailySummaryProcessor", execute)
