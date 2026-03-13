"""AccountSnapshotBuilder — passthrough snapshot of account rows."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_id", "customer_id", "account_type", "account_status",
        "current_balance", "ifw_effective_date",
    ]

    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    if accounts is None or accounts.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    output_df = accounts[output_columns].copy()
    shared_state["output"] = output_df
    return shared_state


register("ExternalModules.AccountSnapshotBuilder", execute)
