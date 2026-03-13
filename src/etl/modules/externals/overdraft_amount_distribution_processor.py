"""OverdraftAmountDistributionProcessor — bucket overdraft amounts and write CSV directly."""
from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "amount_bucket", "event_count", "total_amount", "ifw_effective_date",
    ]

    overdraft_events: pd.DataFrame | None = shared_state.get("overdraft_events")  # type: ignore[assignment]

    effective_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
    if effective_date is not None:
        max_date = effective_date
    else:
        from datetime import date as _date
        max_date = _date.today()

    # W7: Count INPUT rows before bucketing for inflated trailer count
    input_row_count = len(overdraft_events) if overdraft_events is not None else 0

    if overdraft_events is None or overdraft_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    as_of = str(overdraft_events.iloc[0]["ifw_effective_date"]) if pd.notna(overdraft_events.iloc[0]["ifw_effective_date"]) else str(max_date)

    # Bucket overdraft amounts into ranges
    buckets: dict[str, tuple[int, Decimal]] = {
        "0-50": (0, Decimal("0")),
        "50-100": (0, Decimal("0")),
        "100-250": (0, Decimal("0")),
        "250-500": (0, Decimal("0")),
        "500+": (0, Decimal("0")),
    }

    for _, row in overdraft_events.iterrows():
        amount = Decimal(str(row["overdraft_amount"]))
        if amount <= 50:
            bucket = "0-50"
        elif amount <= 100:
            bucket = "50-100"
        elif amount <= 250:
            bucket = "100-250"
        elif amount <= 500:
            bucket = "250-500"
        else:
            bucket = "500+"

        count, total = buckets[bucket]
        buckets[bucket] = (count + 1, total + amount)

    # W7: External writes CSV directly (bypassing CsvFileWriter)
    project_root = path_helper.get_project_root()
    date_str = str(max_date)
    output_path = os.path.join(
        project_root, "Output", "curated",
        "overdraft_amount_distribution", "overdraft_amount_distribution",
        date_str, "overdraft_amount_distribution.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(output_columns) + "\n")

        for bucket_name, (count, total) in buckets.items():
            if count == 0:
                continue
            f.write(f"{bucket_name},{count},{total},{as_of}\n")

        # W7: Trailer uses INPUT row count (inflated), not output bucket count
        f.write(f"TRAILER|{input_row_count}|{date_str}\n")

    # Still set output for framework compatibility
    rows = []
    for bucket_name, (count, total) in buckets.items():
        if count == 0:
            continue
        rows.append({
            "amount_bucket": bucket_name,
            "event_count": count,
            "total_amount": total,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.OverdraftAmountDistributionProcessor", execute)
