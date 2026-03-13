"""PortfolioConcentrationCalculator — sector concentration with W4 integer division and W6 float."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "investment_id", "sector",
        "sector_value", "total_value", "sector_pct", "ifw_effective_date",
    ]

    holdings: pd.DataFrame | None = shared_state.get("holdings")  # type: ignore[assignment]
    securities: pd.DataFrame | None = shared_state.get("securities")  # type: ignore[assignment]
    # investments sourced but used only for presence check in C#
    investments: pd.DataFrame | None = shared_state.get("investments")  # type: ignore[assignment]

    if (
        holdings is None or holdings.empty
        or securities is None or securities.empty
    ):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]

    # Build security_id -> sector lookup
    sector_lookup: dict[int, str] = {}
    for _, sec_row in securities.iterrows():
        sec_id = int(sec_row["security_id"])
        sector_lookup[sec_id] = str(sec_row["sector"]) if pd.notna(sec_row["sector"]) else "Unknown"

    # AP6: Row-by-row iteration with nested loops
    # First pass: compute total value per customer (using float for W6 epsilon errors)
    customer_total_value: dict[int, float] = {}
    for _, row in holdings.iterrows():
        customer_id = int(row["customer_id"])
        # W6: float arithmetic for accumulation (epsilon errors)
        value = float(row["current_value"])

        if customer_id not in customer_total_value:
            customer_total_value[customer_id] = 0.0
        customer_total_value[customer_id] += value

    # Second pass: compute sector value per customer+investment (nested loops, row-by-row)
    sector_values: dict[tuple[int, int, str], float] = {}
    for _, row in holdings.iterrows():
        customer_id = int(row["customer_id"])
        investment_id = int(row["investment_id"])
        sec_id = int(row["security_id"])
        sector = sector_lookup.get(sec_id, "Unknown")
        value = float(row["current_value"])

        key = (customer_id, investment_id, sector)
        if key not in sector_values:
            sector_values[key] = 0.0
        sector_values[key] += value

    # Build output rows
    rows = []
    for (customer_id, investment_id, sector), sector_value in sector_values.items():
        total_value = customer_total_value.get(customer_id, 0.0)

        # W4: Integer division for percentage -- int/int -> 0
        sector_int = int(sector_value)
        total_int = int(total_value)
        sector_pct = Decimal(sector_int // total_int)

        rows.append({
            "customer_id": customer_id,
            "investment_id": investment_id,
            "sector": sector,
            "sector_value": sector_value,
            "total_value": total_value,
            "sector_pct": sector_pct,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.PortfolioConcentrationCalculator", execute)
