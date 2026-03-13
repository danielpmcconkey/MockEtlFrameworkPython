"""TransactionAnomalyFlagger — flags transactions deviating >3 stddevs from account mean."""
from __future__ import annotations

import math
from decimal import Decimal

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "transaction_id", "account_id", "customer_id", "amount",
        "account_mean", "account_stddev", "deviation_factor", "ifw_effective_date",
    ]

    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if (transactions is None or transactions.empty
            or accounts is None or accounts.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build account_id -> customer_id lookup
    account_to_customer: dict[int, int] = {}
    for _, acct_row in accounts.iterrows():
        account_id = int(acct_row["account_id"])
        customer_id = int(acct_row["customer_id"])
        account_to_customer[account_id] = customer_id

    # AP6: Row-by-row iteration to collect per-account amounts
    account_amounts: dict[int, list[Decimal]] = {}
    txn_data: list[tuple[int, int, Decimal, object]] = []
    for _, row in transactions.iterrows():
        account_id = int(row["account_id"])
        txn_id = int(row["transaction_id"])
        amount = Decimal(str(row["amount"]))

        if account_id not in account_amounts:
            account_amounts[account_id] = []
        account_amounts[account_id].append(amount)

        txn_data.append((txn_id, account_id, amount, row["ifw_effective_date"]))

    # Compute per-account mean and stddev
    # C# uses decimal Average() -> double variance -> Math.Sqrt -> cast back to decimal
    account_stats: dict[int, tuple[float, float]] = {}
    for acct_id, amounts in account_amounts.items():
        float_amounts = [float(a) for a in amounts]
        mean = sum(float_amounts) / len(float_amounts)
        variance = sum((a - mean) ** 2 for a in float_amounts) / len(float_amounts)
        stddev = math.sqrt(variance)
        account_stats[acct_id] = (mean, stddev)

    # AP6: Row-by-row iteration to flag anomalies
    output_rows = []
    for txn_id, account_id, amount, as_of in txn_data:
        if account_id not in account_stats:
            continue
        mean, stddev = account_stats[account_id]

        if stddev == 0.0:
            continue

        deviation_factor = abs(float(amount) - mean) / stddev

        # AP7: Magic value -- hardcoded 3.0 threshold
        if deviation_factor > 3.0:
            customer_id = account_to_customer.get(account_id, 0)

            # W5: Banker's rounding
            output_rows.append({
                "transaction_id": txn_id,
                "account_id": account_id,
                "customer_id": customer_id,
                "amount": round(float(amount), 2),
                "account_mean": round(mean, 2),
                "account_stddev": round(stddev, 2),
                "deviation_factor": round(deviation_factor, 2),
                "ifw_effective_date": as_of,
            })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.TransactionAnomalyFlagger", execute)
