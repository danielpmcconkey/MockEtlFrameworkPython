"""FundAllocationWriter — group holdings by security type and write CSV directly, with W8 stale trailer date."""
from __future__ import annotations

import os
from decimal import Decimal, ROUND_HALF_EVEN

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "security_type", "holding_count", "total_value", "avg_value", "ifw_effective_date",
    ]

    holdings: pd.DataFrame | None = shared_state.get("holdings")  # type: ignore[assignment]
    securities: pd.DataFrame | None = shared_state.get("securities")  # type: ignore[assignment]

    if (holdings is None or holdings.empty
            or securities is None or securities.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    date_str = max_date.isoformat() if hasattr(max_date, "isoformat") else str(max_date)

    # Build security_id -> security_type lookup
    type_lookup: dict[int, str] = {}
    for _, sec_row in securities.iterrows():
        sec_id = int(sec_row["security_id"])
        type_lookup[sec_id] = str(sec_row["security_type"]) if pd.notna(sec_row["security_type"]) else "Unknown"

    # Group by security_type
    type_groups: dict[str, tuple[int, Decimal]] = {}
    for _, row in holdings.iterrows():
        sec_id = int(row["security_id"])
        sec_type = type_lookup.get(sec_id, "Unknown")
        value = Decimal(str(row["current_value"]))

        if sec_type not in type_groups:
            type_groups[sec_type] = (0, Decimal("0"))

        current = type_groups[sec_type]
        type_groups[sec_type] = (current[0] + 1, current[1] + value)

    # Write CSV directly (bypassing CsvFileWriter)
    project_root = path_helper.get_project_root()
    output_path = os.path.join(
        project_root, "Output", "curated",
        "fund_allocation_breakdown", "fund_allocation_breakdown",
        date_str, "fund_allocation_breakdown.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    two_places = Decimal("0.01")
    row_count = 0
    with open(output_path, "w", encoding="utf-8") as writer:
        writer.write(",".join(output_columns) + "\n")

        for sec_type in sorted(type_groups.keys()):
            count, total_value = type_groups[sec_type]
            avg_value = total_value / count if count > 0 else Decimal("0")
            avg_value = avg_value.quantize(two_places, rounding=ROUND_HALF_EVEN)
            total_rounded = total_value.quantize(two_places, rounding=ROUND_HALF_EVEN)

            writer.write(f"{sec_type},{count},{total_rounded},{avg_value},{date_str}\n")
            row_count += 1

        # W8: Trailer stale date -- hardcoded to "2024-10-01" instead of maxDate
        writer.write(f"TRAILER|{row_count}|2024-10-01\n")

    # Set empty output so the framework doesn't complain
    shared_state["output"] = pd.DataFrame(columns=output_columns)
    return shared_state


register("ExternalModules.FundAllocationWriter", execute)
