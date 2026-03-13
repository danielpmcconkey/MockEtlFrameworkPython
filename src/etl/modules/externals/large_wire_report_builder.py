"""LargeWireReportBuilder — filter wire transfers > $10,000 with banker's rounding."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "wire_id", "customer_id", "first_name", "last_name",
        "direction", "amount", "counterparty_name", "status", "ifw_effective_date",
    ]

    wire_transfers: pd.DataFrame | None = shared_state.get("wire_transfers")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if wire_transfers is None or wire_transfers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    if customers is not None:
        for _, cust_row in customers.iterrows():
            cust_id = int(cust_row["id"])
            first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
            last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""
            customer_lookup[cust_id] = (first_name, last_name)

    # AP7: magic value — hardcoded $10000 threshold
    rows = []
    for _, row in wire_transfers.iterrows():
        amount = float(row["amount"])

        if amount > 10000:
            customer_id = int(row["customer_id"])
            first_name, last_name = customer_lookup.get(customer_id, ("", ""))

            # W5: banker's rounding
            rounded_amount = round(amount, 2)

            rows.append({
                "wire_id": row["wire_id"],
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "direction": row["direction"],
                "amount": rounded_amount,
                "counterparty_name": row["counterparty_name"],
                "status": row["status"],
                "ifw_effective_date": row["ifw_effective_date"],
            })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.LargeWireReportBuilder", execute)
