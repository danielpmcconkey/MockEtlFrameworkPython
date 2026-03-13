"""RegulatoryExposureCalculator — computes per-customer regulatory exposure scores."""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "account_count", "total_balance", "compliance_events", "wire_count",
        "exposure_score", "ifw_effective_date",
    ]

    compliance_events: pd.DataFrame | None = shared_state.get("compliance_events")  # type: ignore[assignment]
    wire_transfers: pd.DataFrame | None = shared_state.get("wire_transfers")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if customers is None or customers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W2: Weekend fallback -- use Friday's data on Sat/Sun
    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    # Filter all data to target date
    target_customers = customers[customers["ifw_effective_date"] == target_date]
    if target_customers.empty:
        # Fall back to all rows if no exact date match
        target_customers = customers

    # Count compliance events per customer (row-by-row)
    compliance_count_by_customer: dict[int, int] = {}
    if compliance_events is not None:
        for _, row in compliance_events.iterrows():
            customer_id = int(row["customer_id"])
            if customer_id not in compliance_count_by_customer:
                compliance_count_by_customer[customer_id] = 0
            compliance_count_by_customer[customer_id] += 1

    # Count wires per customer (row-by-row)
    wire_count_by_customer: dict[int, int] = {}
    if wire_transfers is not None:
        for _, row in wire_transfers.iterrows():
            customer_id = int(row["customer_id"])
            if customer_id not in wire_count_by_customer:
                wire_count_by_customer[customer_id] = 0
            wire_count_by_customer[customer_id] += 1

    # Compute account count and total balance per customer (row-by-row)
    account_count_by_customer: dict[int, int] = {}
    balance_by_customer: dict[int, Decimal] = {}
    if accounts is not None:
        for _, row in accounts.iterrows():
            customer_id = int(row["customer_id"])
            balance = Decimal(str(row["current_balance"]))

            if customer_id not in account_count_by_customer:
                account_count_by_customer[customer_id] = 0
            account_count_by_customer[customer_id] += 1

            if customer_id not in balance_by_customer:
                balance_by_customer[customer_id] = Decimal(0)
            balance_by_customer[customer_id] += balance

    # Build output per customer
    output_rows = []
    for _, cust_row in target_customers.iterrows():
        customer_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""

        account_count = account_count_by_customer.get(customer_id, 0)
        total_balance = balance_by_customer.get(customer_id, Decimal(0))
        compliance_count = compliance_count_by_customer.get(customer_id, 0)
        wire_count = wire_count_by_customer.get(customer_id, 0)

        # Exposure = (compliance_events * 30) + (wire_count * 20) + (total_balance / 10000)
        exposure_score = round(
            float(Decimal(compliance_count * 30) + Decimal(wire_count * 20) + (total_balance / Decimal(10000))),
            2,
        )

        output_rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "account_count": account_count,
            "total_balance": round(float(total_balance), 2),
            "compliance_events": compliance_count,
            "wire_count": wire_count,
            "exposure_score": exposure_score,
            "ifw_effective_date": target_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.RegulatoryExposureCalculator", execute)
