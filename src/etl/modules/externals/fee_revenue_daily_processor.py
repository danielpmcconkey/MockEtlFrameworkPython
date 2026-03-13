"""FeeRevenueDailyProcessor — daily fee revenue with W3b EOM monthly total row and W6 float arithmetic."""
from __future__ import annotations

import calendar
from datetime import date

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "event_date", "charged_fees", "waived_fees", "net_revenue", "ifw_effective_date",
    ]

    overdraft_events: pd.DataFrame | None = shared_state.get("overdraft_events")  # type: ignore[assignment]

    max_date: date = shared_state.get(ETL_EFFECTIVE_DATE_KEY, date.today())  # type: ignore[assignment]

    if overdraft_events is None or overdraft_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date_str = max_date.isoformat()

    # AP10: Over-sourced full date range via config, but External filters to current date only
    current_date_rows = []
    for _, row in overdraft_events.iterrows():
        ifw = row["ifw_effective_date"]
        ifw_str = ifw.isoformat() if hasattr(ifw, "isoformat") else str(ifw)
        if ifw_str == max_date_str:
            current_date_rows.append(row)

    if len(current_date_rows) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W6: Double epsilon -- use float instead of decimal for accumulation
    charged_fees = 0.0
    waived_fees = 0.0

    for row in current_date_rows:
        fee_amount = float(row["fee_amount"])
        fee_waived = bool(row["fee_waived"])

        if fee_waived:
            waived_fees += fee_amount
        else:
            charged_fees += fee_amount

    net_revenue = charged_fees - waived_fees

    rows = []
    rows.append({
        "event_date": max_date_str,
        "charged_fees": charged_fees,
        "waived_fees": waived_fees,
        "net_revenue": net_revenue,
        "ifw_effective_date": max_date_str,
    })

    # W3b: End-of-month boundary -- append monthly summary row
    days_in_month = calendar.monthrange(max_date.year, max_date.month)[1]
    if max_date.day == days_in_month:
        # Sum ALL rows in the source (full month), not just today's filtered rows
        month_charged = 0.0
        month_waived = 0.0

        for _, row in overdraft_events.iterrows():
            fee_amount = float(row["fee_amount"])
            fee_waived = bool(row["fee_waived"])

            if fee_waived:
                month_waived += fee_amount
            else:
                month_charged += fee_amount

        rows.append({
            "event_date": "MONTHLY_TOTAL",
            "charged_fees": month_charged,
            "waived_fees": month_waived,
            "net_revenue": month_charged - month_waived,
            "ifw_effective_date": max_date_str,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.FeeRevenueDailyProcessor", execute)
