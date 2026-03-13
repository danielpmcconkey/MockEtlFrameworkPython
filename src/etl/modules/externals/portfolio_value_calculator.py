"""PortfolioValueCalculator — total portfolio value per customer with W2 weekend fallback."""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "total_portfolio_value", "holding_count", "ifw_effective_date",
    ]

    holdings: pd.DataFrame | None = shared_state.get("holdings")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if (
        holdings is None or holdings.empty
        or customers is None or customers.empty
    ):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W2: Weekend fallback -- use Friday's data on Sat/Sun
    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    # C# casts to DateOnly and compares directly
    filtered_holdings = holdings[
        holdings["ifw_effective_date"].apply(lambda d: d == target_date)
    ]

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        customer_lookup[cust_id] = (
            str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else "",
            str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else "",
        )

    # AP6: Row-by-row iteration to compute totals
    customer_totals: dict[int, tuple[Decimal, int]] = {}
    for _, row in filtered_holdings.iterrows():
        customer_id = int(row["customer_id"])
        value = Decimal(str(row["current_value"]))

        if customer_id not in customer_totals:
            customer_totals[customer_id] = (Decimal("0"), 0)

        total_value, holding_count = customer_totals[customer_id]
        customer_totals[customer_id] = (total_value + value, holding_count + 1)

    rows = []
    for cust_id, (total_value, holding_count) in customer_totals.items():
        first_name, last_name = customer_lookup.get(cust_id, ("", ""))

        rows.append({
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "total_portfolio_value": round(total_value, 2),
            "holding_count": holding_count,
            "ifw_effective_date": target_date,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.PortfolioValueCalculator", execute)
