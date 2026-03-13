"""HoldingsBySectorWriter — group holdings by sector and write CSV directly."""
from __future__ import annotations

import os

import pandas as pd

from etl import path_helper
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = ["sector", "holding_count", "total_value", "ifw_effective_date"]

    holdings: pd.DataFrame | None = shared_state.get("holdings")  # type: ignore[assignment]
    securities: pd.DataFrame | None = shared_state.get("securities")  # type: ignore[assignment]

    if (holdings is None or holdings.empty
            or securities is None or securities.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W7: Count INPUT rows before any grouping (inflated count for trailer)
    input_count = len(holdings)

    max_date = shared_state["__etlEffectiveDate"]
    date_str = max_date.isoformat()  # type: ignore[union-attr]

    # Build security_id -> sector lookup
    sector_lookup: dict[int, str] = {}
    for _, sec_row in securities.iterrows():
        sec_id = int(sec_row["security_id"])
        sector_lookup[sec_id] = str(sec_row["sector"]) if pd.notna(sec_row["sector"]) else "Unknown"

    # Group holdings by sector
    sector_groups: dict[str, tuple[int, float]] = {}
    for _, row in holdings.iterrows():
        sec_id = int(row["security_id"])
        sector = sector_lookup.get(sec_id, "Unknown")
        value = float(row["current_value"])

        if sector not in sector_groups:
            sector_groups[sector] = (0, 0.0)

        count, total = sector_groups[sector]
        sector_groups[sector] = (count + 1, total + value)

    # Write CSV directly (bypassing CsvFileWriter)
    solution_root = path_helper.get_project_root()
    output_path = os.path.join(
        solution_root, "Output", "curated", "holdings_by_sector",
        "holdings_by_sector", date_str, "holdings_by_sector.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(",".join(output_columns) + "\n")

        for sector in sorted(sector_groups.keys()):
            count, total_value = sector_groups[sector]
            f.write(f"{sector},{count},{round(total_value, 2)},{date_str}\n")

        # W7: Trailer uses input count (inflated) instead of output row count
        f.write(f"TRAILER|{input_count}|{date_str}\n")

    # Set empty output so the framework doesn't complain
    shared_state["output"] = pd.DataFrame(columns=output_columns)
    return shared_state


register("ExternalModules.HoldingsBySectorWriter", execute)
