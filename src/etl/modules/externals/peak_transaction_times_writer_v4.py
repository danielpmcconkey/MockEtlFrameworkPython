"""PeakTransactionTimesWriterV4 — V4 external module for PeakTransactionTimes with direct CSV."""
from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    hourly_aggregation: pd.DataFrame | None = shared_state.get("hourly_aggregation")  # type: ignore[assignment]
    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    date_str = str(max_date)

    # Input count for trailer (pre-aggregation transaction count)
    input_count = len(transactions) if transactions is not None else 0

    # Build output rows from the SQL-aggregated data, adding ifw_effective_date
    output_columns = ["hour_of_day", "txn_count", "total_amount", "ifw_effective_date"]
    output_rows: list[dict] = []

    if hourly_aggregation is not None and not hourly_aggregation.empty:
        for _, row in hourly_aggregation.iterrows():
            output_rows.append({
                "hour_of_day": row["hour_of_day"],
                "txn_count": row["txn_count"],
                # Store as Decimal and format to 2dp when writing
                "total_amount": Decimal(str(row["total_amount"])),
                "ifw_effective_date": date_str,
            })

    # Write CSV directly with correct trailer
    _write_csv(output_rows, output_columns, input_count, date_str)

    # Return empty DataFrame to framework (mirrors V1 behavior)
    shared_state["output"] = pd.DataFrame(columns=output_columns)
    return shared_state


def _format_field(column_name: str, value: object) -> str:
    if value is None:
        return ""

    # total_amount must be formatted with exactly 2 decimal places to match V1
    if column_name == "total_amount" and isinstance(value, Decimal):
        return f"{value:.2f}"

    s = str(value)
    # RFC 4180: quote fields containing commas, double quotes, or newlines
    if "," in s or '"' in s or "\n" in s or "\r" in s:
        return '"' + s.replace('"', '""') + '"'
    return s


def _write_csv(
    rows: list[dict],
    columns: list[str],
    input_count: int,
    date_str: str,
) -> None:
    project_root = path_helper.get_project_root()
    output_path = os.path.join(
        project_root, "Output", "double_secret_curated",
        "peak_transaction_times", "peak_transaction_times",
        date_str, "peak_transaction_times.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        # Header
        f.write(",".join(columns) + "\n")

        # Data rows
        for row in rows:
            values = [_format_field(c, row.get(c)) for c in columns]
            f.write(",".join(values) + "\n")

        # Trailer with INPUT count (not output row count)
        f.write(f"TRAILER|{input_count}|{date_str}\n")


register("ExternalModules.PeakTransactionTimesWriterV4", execute)
