"""AccountVelocityTracker — group transactions by account+date, write CSV directly."""
from __future__ import annotations

import os
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from etl import path_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_id", "customer_id", "txn_date", "txn_count",
        "total_amount", "ifw_effective_date",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    if (transactions is None or transactions.empty
            or accounts is None or accounts.empty):
        _write_direct_csv([], output_columns, shared_state)
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    date_str = str(max_date)  # yyyy-mm-dd format from date object

    # Build account_id -> customer_id lookup
    account_to_customer: dict[int, int] = {}
    for _, acct_row in accounts.iterrows():
        account_id = int(acct_row["account_id"])
        customer_id = int(acct_row["customer_id"])
        account_to_customer[account_id] = customer_id

    # Group by account_id and txn_date (ifw_effective_date)
    groups: dict[tuple[int, str], tuple[int, Decimal]] = {}
    for _, row in transactions.iterrows():
        account_id = int(row["account_id"])
        txn_date = str(row["ifw_effective_date"]) if pd.notna(row["ifw_effective_date"]) else date_str

        key = (account_id, txn_date)
        if key not in groups:
            groups[key] = (0, Decimal("0"))

        current_count, current_total = groups[key]
        groups[key] = (current_count + 1, current_total + Decimal(str(row["amount"])))

    # Sort by txn_date then account_id (matching C# OrderBy/ThenBy)
    sorted_keys = sorted(groups.keys(), key=lambda k: (k[1], k[0]))

    output_rows = []
    for account_id, txn_date in sorted_keys:
        count, total = groups[(account_id, txn_date)]
        customer_id = account_to_customer.get(account_id, 0)

        # C# Math.Round(total, 2) — default MidpointRounding.ToEven (banker's)
        rounded_total = round(float(total), 2)

        output_rows.append({
            "account_id": account_id,
            "customer_id": customer_id,
            "txn_date": txn_date,
            "txn_count": count,
            "total_amount": rounded_total,
            "ifw_effective_date": date_str,
        })

    # W12: External writes CSV directly with header re-emitted on every append
    _write_direct_csv(output_rows, output_columns, shared_state)

    # Return empty DataFrame — C# returns empty after writing CSV
    shared_state["output"] = pd.DataFrame(columns=output_columns)
    return shared_state


def _write_direct_csv(
    rows: list[dict],
    columns: list[str],
    shared_state: dict[str, object],
) -> None:
    from datetime import date as date_type

    max_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY, None)
    if max_date is None:
        from datetime import date
        max_date = date.today()
    date_str = str(max_date)

    project_root = path_helper.get_project_root()
    output_path = os.path.join(
        project_root, "Output", "curated", "account_velocity_tracking",
        "account_velocity_tracking", date_str, "account_velocity_tracking.csv",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # W12: Append mode with header re-emitted on every run
    with open(output_path, "a", newline="") as f:
        # Header re-emitted each time
        f.write(",".join(columns) + "\n")

        for row in rows:
            values = [str(row.get(c, "")) for c in columns]
            f.write(",".join(values) + "\n")


register("ExternalModules.AccountVelocityTracker", execute)
