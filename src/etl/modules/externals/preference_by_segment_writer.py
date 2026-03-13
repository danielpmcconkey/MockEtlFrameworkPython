"""PreferenceBySegmentWriter — opt-in rate by segment with direct CSV output."""
from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "segment_name", "preference_type", "opt_in_rate", "ifw_effective_date",
    ]

    prefs: pd.DataFrame | None = shared_state.get("customer_preferences")  # type: ignore[assignment]
    cust_segments: pd.DataFrame | None = shared_state.get("customers_segments")  # type: ignore[assignment]
    segments: pd.DataFrame | None = shared_state.get("segments")  # type: ignore[assignment]

    if (
        prefs is None or prefs.empty
        or cust_segments is None
        or segments is None
    ):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W7: Count INPUT rows before any grouping (inflated count for trailer)
    input_count = len(prefs)

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    date_str = str(max_date)

    # Build segment_id -> segment_name lookup
    segment_lookup: dict[int, str] = {}
    for _, row in segments.iterrows():
        seg_id = int(row["segment_id"])
        segment_lookup[seg_id] = str(row["segment_name"]) if pd.notna(row["segment_name"]) else ""

    # Build customer_id -> segment_name lookup
    cust_seg_lookup: dict[int, str] = {}
    for _, row in cust_segments.iterrows():
        cust_id = int(row["customer_id"])
        seg_id = int(row["segment_id"])
        cust_seg_lookup[cust_id] = segment_lookup.get(seg_id, "Unknown")

    # Group by (segment_name, preference_type) -> (opted_in_count, total_count)
    groups: dict[tuple[str, str], tuple[int, int]] = {}
    for _, row in prefs.iterrows():
        cust_id = int(row["customer_id"])
        pref_type = str(row["preference_type"]) if pd.notna(row["preference_type"]) else ""
        opted_in = bool(row["opted_in"])
        segment = cust_seg_lookup.get(cust_id, "Unknown")

        key = (segment, pref_type)
        if key not in groups:
            groups[key] = (0, 0)

        opted_in_count, total = groups[key]
        if opted_in:
            groups[key] = (opted_in_count + 1, total + 1)
        else:
            groups[key] = (opted_in_count, total + 1)

    # Write CSV directly (bypassing CsvFileWriter)
    project_root = path_helper.get_project_root()
    output_path = os.path.join(
        project_root, "Output", "curated",
        "preference_by_segment", "preference_by_segment",
        date_str, "preference_by_segment.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(output_columns) + "\n")

        for key in sorted(groups.keys(), key=lambda k: (k[0], k[1])):
            segment, pref_type = key
            opted_in_count, total = groups[key]
            # W5: Banker's rounding
            rate = round(Decimal(opted_in_count) / Decimal(total), 2) if total > 0 else Decimal("0")

            f.write(f"{segment},{pref_type},{rate},{date_str}\n")

        # W7: Trailer uses input count (inflated) instead of output row count
        f.write(f"TRAILER|{input_count}|{date_str}\n")

    # Set empty output so the framework doesn't complain
    shared_state["output"] = pd.DataFrame(columns=output_columns)
    return shared_state


register("ExternalModules.PreferenceBySegmentWriter", execute)
