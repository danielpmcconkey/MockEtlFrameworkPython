"""DailyBalanceMovementCalculator_re — vectorized debit/credit totals per account using float arithmetic (W6)."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "account_id", "customer_id", "debit_total", "credit_total",
        "net_movement", "ifw_effective_date",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]

    # FSD-004: Empty-Input Guard
    if (transactions is None or len(transactions) == 0
            or accounts is None or len(accounts) == 0):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # FSD-005: Account-to-Customer Lookup (vectorized)
    account_lookup = dict(zip(
        accounts["account_id"].astype(int),
        accounts["customer_id"].astype(int),
    ))

    # FSD-006: Type Casting
    txns = transactions.copy()
    txns["account_id"] = txns["account_id"].astype(int)
    txns["amount"] = txns["amount"].astype(float)  # IEEE 754 double (AP-001: reproduced)
    txns["txn_type"] = txns["txn_type"].fillna("")  # coalesce null/NaN to empty string

    # FSD-007: First-Appearance Order + ifw_effective_date Capture
    unique_accounts = txns["account_id"].drop_duplicates()  # preserves order
    first_dates = txns.drop_duplicates(subset="account_id", keep="first") \
                      .set_index("account_id")["ifw_effective_date"]

    # FSD-008: Per-Account Debit/Credit Accumulation (float, AP-001)
    debit_totals = txns.loc[txns["txn_type"] == "Debit"] \
                       .groupby("account_id", sort=False)["amount"].sum()
    credit_totals = txns.loc[txns["txn_type"] == "Credit"] \
                        .groupby("account_id", sort=False)["amount"].sum()

    # FSD-009: Output DataFrame Construction
    result = pd.DataFrame({"account_id": unique_accounts.values})
    result["customer_id"] = result["account_id"].map(account_lookup) \
                                                 .fillna(0).astype(int)  # FSD-010
    result["debit_total"] = result["account_id"].map(debit_totals).fillna(0.0)
    result["credit_total"] = result["account_id"].map(credit_totals).fillna(0.0)
    result["net_movement"] = result["credit_total"] - result["debit_total"]  # FSD-011
    result["ifw_effective_date"] = result["account_id"].map(first_dates)  # FSD-012

    result = result[output_columns]
    shared_state["output"] = result
    return shared_state


register("ExternalModules.DailyBalanceMovementCalculator_re", execute)
