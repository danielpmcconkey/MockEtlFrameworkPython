"""CustomerInvestmentSummaryBuilder — aggregates investment counts and values per customer with banker's rounding."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN

import pandas as pd

from etl.modules.external import register
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "investment_count", "total_value", "ifw_effective_date",
    ]

    investments: pd.DataFrame | None = shared_state.get("investments")
    customers: pd.DataFrame | None = shared_state.get("customers")

    if investments is None or len(investments) == 0 or customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        customer_lookup[cust_id] = (
            str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else "",
            str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else "",
        )

    # Aggregate investments per customer
    customer_agg: dict[int, tuple[int, Decimal]] = {}
    for _, row in investments.iterrows():
        customer_id = int(row["customer_id"])
        value = Decimal(str(row["current_value"]))

        if customer_id not in customer_agg:
            customer_agg[customer_id] = (0, Decimal(0))

        count, total_value = customer_agg[customer_id]
        customer_agg[customer_id] = (count + 1, total_value + value)

    output_rows = []
    for cust_id, (count, total_value) in customer_agg.items():
        if cust_id in customer_lookup:
            first_name, last_name = customer_lookup[cust_id]
        else:
            first_name, last_name = "", ""

        # W5: Banker's rounding (MidpointRounding.ToEven)
        rounded_value = total_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)

        output_rows.append({
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "investment_count": count,
            "total_value": rounded_value,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerInvestmentSummaryBuilder", execute)
