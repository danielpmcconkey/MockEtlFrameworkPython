"""WireDirectionSummaryWriter — groups wires by direction and writes CSV directly."""
from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "direction", "wire_count", "total_amount", "avg_amount", "ifw_effective_date",
    ]

    wire_transfers: pd.DataFrame | None = shared_state.get("wire_transfers")  # type: ignore[assignment]

    if wire_transfers is None or wire_transfers.empty:
        # Still need to write empty file
        _write_direct_csv([], output_columns, 0, shared_state)
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W7: count INPUT rows for trailer (before grouping)
    input_count = len(wire_transfers)

    # Group by direction
    groups: dict[str, tuple[int, Decimal]] = {}
    for _, row in wire_transfers.iterrows():
        direction = str(row["direction"]) if pd.notna(row["direction"]) else ""
        amount = Decimal(str(row["amount"]))

        if direction not in groups:
            groups[direction] = (0, Decimal(0))

        current_count, current_total = groups[direction]
        groups[direction] = (current_count + 1, current_total + amount)

    as_of = wire_transfers.iloc[0]["ifw_effective_date"]
    output_rows = []
    for direction, (wire_count, total_amount) in groups.items():
        avg_amount = round(float(total_amount / wire_count), 2) if wire_count > 0 else 0.0

        output_rows.append({
            "direction": direction,
            "wire_count": wire_count,
            "total_amount": round(float(total_amount), 2),
            "avg_amount": avg_amount,
            "ifw_effective_date": as_of,
        })

    # W7: External writes CSV directly (bypassing CsvFileWriter)
    _write_direct_csv(output_rows, output_columns, input_count, shared_state)

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


def _write_direct_csv(
    rows: list[dict],
    columns: list[str],
    input_count: int,
    shared_state: dict[str, object],
) -> None:
    max_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY, None)
    if max_date is None:
        from datetime import date
        max_date = date.today()
    date_str = str(max_date)

    project_root = path_helper.get_project_root()
    output_path = os.path.join(
        project_root, "Output", "curated", "wire_direction_summary",
        "wire_direction_summary", date_str, "wire_direction_summary.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", newline="") as f:
        # Header
        f.write(",".join(columns) + "\n")

        # Data rows
        for row in rows:
            values = [str(row.get(c, "")) for c in columns]
            f.write(",".join(values) + "\n")

        # W7: trailer uses inputCount (inflated) instead of output row count
        f.write(f"TRAILER|{input_count}|{date_str}\n")


register("ExternalModules.WireDirectionSummaryWriter", execute)
