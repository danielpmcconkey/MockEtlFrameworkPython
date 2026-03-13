"""ComplianceTransactionRatioWriter — writes CSV directly with W4/W7 bugs."""
from __future__ import annotations

import os
from datetime import date

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "event_type", "event_count", "txn_count", "events_per_1000_txns",
        "ifw_effective_date",
    ]

    compliance_events: pd.DataFrame | None = shared_state.get("compliance_events")  # type: ignore[assignment]
    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]

    if compliance_events is None or compliance_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date: date = shared_state[ETL_EFFECTIVE_DATE_KEY]  # type: ignore[assignment]
    date_str = max_date.isoformat()

    # W7: Count INPUT rows from both DataFrames for trailer (inflated)
    input_count = len(compliance_events) + (len(transactions) if transactions is not None else 0)

    txn_count = len(transactions) if transactions is not None else 0

    # Group compliance events by event_type
    event_groups: dict[str, int] = {}
    for _, row in compliance_events.iterrows():
        event_type = str(row.get("event_type") or "Unknown")
        event_groups[event_type] = event_groups.get(event_type, 0) + 1

    # Write CSV directly (bypassing CsvFileWriter)
    solution_root = path_helper.get_project_root()
    output_path = os.path.join(
        solution_root, "Output", "curated",
        "compliance_transaction_ratio", "compliance_transaction_ratio",
        date_str, "compliance_transaction_ratio.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as writer:
        writer.write(",".join(output_columns) + "\n")

        for event_type in sorted(event_groups.keys()):
            event_count = event_groups[event_type]
            # W4: Integer division — (eventCount * 1000) / txnCount where both are int
            events_per_1000 = (event_count * 1000) // txn_count if txn_count > 0 else 0
            writer.write(f"{event_type},{event_count},{txn_count},{events_per_1000},{date_str}\n")

        # W7: Trailer uses input count (inflated) instead of output row count
        writer.write(f"TRAILER|{input_count}|{date_str}\n")

    # Set empty output so the framework doesn't complain
    shared_state["output"] = pd.DataFrame(columns=output_columns)
    return shared_state


register("ExternalModules.ComplianceTransactionRatioWriter", execute)
