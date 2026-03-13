"""BondMaturityScheduleBuilder — aggregate holdings for bond securities."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "security_id", "ticker", "security_name", "sector",
        "total_held_value", "holder_count", "ifw_effective_date",
    ]

    securities: pd.DataFrame | None = shared_state.get("securities")  # type: ignore[assignment]
    holdings: pd.DataFrame | None = shared_state.get("holdings")  # type: ignore[assignment]

    if securities is None or securities.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]

    # AP3: Filter to bonds only
    bonds = securities[securities["security_type"].astype(str) == "Bond"]

    if bonds.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build bond lookup
    bond_lookup: dict[int, tuple[str, str, str]] = {}
    for _, bond in bonds.iterrows():
        sec_id = int(bond["security_id"])
        ticker = str(bond["ticker"]) if pd.notna(bond["ticker"]) else ""
        name = str(bond["security_name"]) if pd.notna(bond["security_name"]) else ""
        sector = str(bond["sector"]) if pd.notna(bond["sector"]) else ""
        bond_lookup[sec_id] = (ticker, name, sector)

    # AP6: Row-by-row iteration to join with holdings
    bond_totals: dict[int, tuple[Decimal, int]] = {}
    if holdings is not None and not holdings.empty:
        for _, row in holdings.iterrows():
            sec_id = int(row["security_id"])
            if sec_id not in bond_lookup:
                continue

            value = Decimal(str(row["current_value"]))

            if sec_id not in bond_totals:
                bond_totals[sec_id] = (Decimal("0"), 0)

            current_total, current_count = bond_totals[sec_id]
            bond_totals[sec_id] = (current_total + value, current_count + 1)

    output_rows = []
    for _, bond in bonds.iterrows():
        sec_id = int(bond["security_id"])
        ticker, name, sector = bond_lookup[sec_id]
        total_value, holder_count = bond_totals.get(sec_id, (Decimal("0"), 0))

        output_rows.append({
            "security_id": sec_id,
            "ticker": ticker,
            "security_name": name,
            "sector": sector,
            "total_held_value": round(float(total_value), 2),
            "holder_count": holder_count,
            "ifw_effective_date": max_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.BondMaturityScheduleBuilder", execute)
