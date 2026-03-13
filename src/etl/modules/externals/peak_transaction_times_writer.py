"""PeakTransactionTimesWriter — hourly transaction aggregation with direct CSV output."""
from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "hour_of_day", "txn_count", "total_amount", "ifw_effective_date",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]

    if transactions is None or transactions.empty:
        _write_direct_csv([], output_columns, 0, shared_state)
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W7: Count INPUT rows for trailer (before hourly bucketing)
    input_count = len(transactions)

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    date_str = str(max_date)

    # Group by hour of day from txn_timestamp
    hourly_groups: dict[int, tuple[int, Decimal]] = {}
    for _, row in transactions.iterrows():
        timestamp = row["txn_timestamp"]
        hour = 0
        if isinstance(timestamp, datetime):
            hour = timestamp.hour
        elif timestamp is not None:
            try:
                parsed = pd.Timestamp(str(timestamp))
                hour = parsed.hour
            except Exception:
                hour = 0

        if hour not in hourly_groups:
            hourly_groups[hour] = (0, Decimal("0"))

        count, total = hourly_groups[hour]
        hourly_groups[hour] = (count + 1, total + Decimal(str(row["amount"])))

    output_rows = []
    for hour_key in sorted(hourly_groups.keys()):
        count, total = hourly_groups[hour_key]
        output_rows.append({
            "hour_of_day": hour_key,
            "txn_count": count,
            "total_amount": round(total, 2),
            "ifw_effective_date": date_str,
        })

    # W7: External writes CSV directly, trailer uses inputCount (inflated)
    _write_direct_csv(output_rows, output_columns, input_count, shared_state)

    shared_state["output"] = pd.DataFrame(columns=output_columns)
    return shared_state


def _write_direct_csv(
    rows: list[dict],
    columns: list[str],
    input_count: int,
    shared_state: dict[str, object],
) -> None:
    project_root = path_helper.get_project_root()
    max_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
    if max_date is None:
        from datetime import date as _date
        max_date = _date.today()
    date_str = str(max_date)
    output_path = os.path.join(
        project_root, "Output", "curated",
        "peak_transaction_times", "peak_transaction_times",
        date_str, "peak_transaction_times.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        # Header
        f.write(",".join(columns) + "\n")

        # Data rows
        for row in rows:
            values = [str(row.get(c, "")) for c in columns]
            f.write(",".join(values) + "\n")

        # W7: Trailer uses inputCount (inflated) instead of output row count
        f.write(f"TRAILER|{input_count}|{date_str}\n")


register("ExternalModules.PeakTransactionTimesWriter", execute)
