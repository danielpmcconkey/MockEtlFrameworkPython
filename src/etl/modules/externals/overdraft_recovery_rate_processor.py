"""OverdraftRecoveryRateProcessor — recovery rate with W4 integer division and W5 banker's rounding."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "total_events", "charged_count", "waived_count", "recovery_rate",
        "ifw_effective_date",
    ]

    overdraft_events: pd.DataFrame | None = shared_state.get("overdraft_events")  # type: ignore[assignment]

    effective_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
    if effective_date is not None:
        max_date = effective_date
    else:
        max_date = date.today()

    if overdraft_events is None or overdraft_events.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    total_events = 0
    charged_count = 0
    waived_count = 0

    for _, row in overdraft_events.iterrows():
        total_events += 1
        fee_waived = bool(row["fee_waived"])
        if fee_waived:
            waived_count += 1
        else:
            charged_count += 1

    # W4: Integer division -- charged_count / total_events both int -> truncates to 0
    recovery_rate = Decimal(charged_count // total_events)

    # W5: Banker's rounding -- already 0 from integer division, but round anyway
    recovery_rate = round(recovery_rate, 4)

    rows = [{
        "total_events": total_events,
        "charged_count": charged_count,
        "waived_count": waived_count,
        "recovery_rate": recovery_rate,
        "ifw_effective_date": str(max_date),
    }]

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.OverdraftRecoveryRateProcessor", execute)
