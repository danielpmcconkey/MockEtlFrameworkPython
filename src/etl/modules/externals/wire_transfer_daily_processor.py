"""WireTransferDailyProcessor — groups wire transfers by date with monthly summary on last day."""
from __future__ import annotations

import calendar
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "wire_date", "wire_count", "total_amount", "avg_amount", "ifw_effective_date",
    ]

    wire_transfers: pd.DataFrame | None = shared_state.get("wire_transfers")  # type: ignore[assignment]

    max_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
    if max_date is None:
        from datetime import date
        max_date = date.today()

    if wire_transfers is None or wire_transfers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # AP3: unnecessary External -- SQL GROUP BY would suffice
    # AP6: row-by-row iteration instead of set-based

    # Group by ifw_effective_date (used as wire_date) row-by-row -- daily rows
    daily_groups: dict[object, tuple[int, Decimal]] = {}
    for _, row in wire_transfers.iterrows():
        as_of = row["ifw_effective_date"]
        amount = Decimal(str(row["amount"]))

        if pd.isna(as_of) if isinstance(as_of, float) else as_of is None:
            continue

        if as_of not in daily_groups:
            daily_groups[as_of] = (0, Decimal(0))

        current_count, current_total = daily_groups[as_of]
        daily_groups[as_of] = (current_count + 1, current_total + amount)

    output_rows = []
    for wire_date, (wire_count, total_amount) in daily_groups.items():
        avg_amount = round(float(total_amount / wire_count), 2) if wire_count > 0 else 0.0

        output_rows.append({
            "wire_date": wire_date,
            "wire_count": wire_count,
            "total_amount": round(float(total_amount), 2),
            "avg_amount": avg_amount,
            "ifw_effective_date": wire_date,
        })

    # W3b: End-of-month boundary -- append monthly summary row
    if max_date.day == calendar.monthrange(max_date.year, max_date.month)[1]:
        total_wires = sum(g[0] for g in daily_groups.values())
        total_amt = sum(g[1] for g in daily_groups.values())

        output_rows.append({
            "wire_date": None,
            "wire_count": total_wires,
            "total_amount": round(float(total_amt), 2),
            "avg_amount": round(float(total_amt / total_wires), 2) if total_wires > 0 else 0.0,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.WireTransferDailyProcessor", execute)
