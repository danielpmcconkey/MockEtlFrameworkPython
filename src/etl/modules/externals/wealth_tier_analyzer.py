"""WealthTierAnalyzer — classifies customers into wealth tiers based on total holdings."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "wealth_tier", "customer_count", "total_wealth",
        "avg_wealth", "pct_of_customers", "ifw_effective_date",
    ]

    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    investments: pd.DataFrame | None = shared_state.get("investments")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if customers is None or customers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]

    # Compute total wealth per customer (accounts + investments)
    wealth_by_customer: dict[int, Decimal] = {}

    if accounts is not None:
        for _, row in accounts.iterrows():
            cust_id = int(row["customer_id"])
            wealth_by_customer[cust_id] = wealth_by_customer.get(cust_id, Decimal(0)) + Decimal(str(row["current_balance"]))

    if investments is not None:
        for _, row in investments.iterrows():
            cust_id = int(row["customer_id"])
            wealth_by_customer[cust_id] = wealth_by_customer.get(cust_id, Decimal(0)) + Decimal(str(row["current_value"]))

    # AP7: Magic value thresholds for tier assignment
    tier_groups: dict[str, tuple[int, Decimal]] = {
        "Bronze": (0, Decimal(0)),
        "Silver": (0, Decimal(0)),
        "Gold": (0, Decimal(0)),
        "Platinum": (0, Decimal(0)),
    }

    for wealth in wealth_by_customer.values():
        if wealth < 10000:
            tier = "Bronze"
        elif wealth < 100000:
            tier = "Silver"
        elif wealth < 500000:
            tier = "Gold"
        else:
            tier = "Platinum"

        count, total = tier_groups[tier]
        tier_groups[tier] = (count + 1, total + wealth)

    total_customers = len(wealth_by_customer)
    output_rows = []

    for tier in ["Bronze", "Silver", "Gold", "Platinum"]:
        count, total_wealth = tier_groups[tier]
        avg_wealth = total_wealth / count if count > 0 else Decimal(0)
        # W5: Banker's rounding
        pct_of_customers = (
            round(float(Decimal(count) / Decimal(total_customers) * 100), 2)
            if total_customers > 0 else 0.0
        )

        output_rows.append({
            "wealth_tier": tier,
            "customer_count": count,
            "total_wealth": round(float(total_wealth), 2),
            "avg_wealth": round(float(avg_wealth), 2),
            "pct_of_customers": pct_of_customers,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.WealthTierAnalyzer", execute)
