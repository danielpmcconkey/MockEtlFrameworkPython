"""PreferenceSummaryCounter — counts opted-in/out customers per preference type."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "preference_type", "opted_in_count", "opted_out_count",
        "total_customers", "ifw_effective_date",
    ]

    prefs: pd.DataFrame | None = shared_state.get("customer_preferences")  # type: ignore[assignment]

    if prefs is None or prefs.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    as_of = prefs.iloc[0]["ifw_effective_date"]

    # AP6: Row-by-row iteration where SQL GROUP BY would suffice
    counts: dict[str, tuple[int, int]] = {}
    for _, row in prefs.iterrows():
        pref_type = str(row["preference_type"]) if pd.notna(row["preference_type"]) else ""
        opted_in = bool(row["opted_in"])

        if pref_type not in counts:
            counts[pref_type] = (0, 0)

        current = counts[pref_type]
        if opted_in:
            counts[pref_type] = (current[0] + 1, current[1])
        else:
            counts[pref_type] = (current[0], current[1] + 1)

    output_rows = []
    for pref_type, (opted_in, opted_out) in counts.items():
        output_rows.append({
            "preference_type": pref_type,
            "opted_in_count": opted_in,
            "opted_out_count": opted_out,
            "total_customers": opted_in + opted_out,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.PreferenceSummaryCounter", execute)
