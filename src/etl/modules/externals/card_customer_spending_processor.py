"""CardCustomerSpendingProcessor — aggregate card spending per customer with weekend fallback."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "txn_count",
        "total_spending", "ifw_effective_date",
    ]

    max_date: date = shared_state[ETL_EFFECTIVE_DATE_KEY]  # type: ignore[assignment]

    # W2: Weekend fallback — use Friday's data on Sat/Sun
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    card_transactions: pd.DataFrame | None = shared_state.get("card_transactions")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if card_transactions is None or card_transactions.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Filter to target date
    # C# compares DateOnly == targetDate
    filtered_txns = card_transactions[
        card_transactions["ifw_effective_date"].apply(lambda d: d == target_date)
    ]

    if filtered_txns.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    if customers is not None and not customers.empty:
        for _, c in customers.iterrows():
            cust_id = int(c["id"])
            first = str(c["first_name"]) if pd.notna(c["first_name"]) else ""
            last = str(c["last_name"]) if pd.notna(c["last_name"]) else ""
            customer_lookup[cust_id] = (first, last)

    # Group by customer
    groups: dict[int, tuple[int, Decimal]] = {}
    for _, txn in filtered_txns.iterrows():
        cust_id = int(txn["customer_id"])
        amount = Decimal(str(txn["amount"]))

        if cust_id not in groups:
            groups[cust_id] = (0, Decimal("0"))

        current_count, current_total = groups[cust_id]
        groups[cust_id] = (current_count + 1, current_total + amount)

    output_rows = []
    for cust_id, (count, total) in groups.items():
        first, last = customer_lookup.get(cust_id, ("", ""))

        output_rows.append({
            "customer_id": cust_id,
            "first_name": first,
            "last_name": last,
            "txn_count": count,
            "total_spending": total,
            "ifw_effective_date": target_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CardCustomerSpendingProcessor", execute)
