"""CustomerComplianceRiskCalculator — computes compliance risk scores from events, wires, and high-value transactions."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name",
        "compliance_events", "wire_count", "high_txn_count", "risk_score", "ifw_effective_date",
    ]

    compliance_events: pd.DataFrame | None = shared_state.get("compliance_events")
    wire_transfers: pd.DataFrame | None = shared_state.get("wire_transfers")
    transactions: pd.DataFrame | None = shared_state.get("transactions")
    customers: pd.DataFrame | None = shared_state.get("customers")

    if customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # AP3: unnecessary External — SQL could handle this
    # AP6: row-by-row iteration

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

    # Count high-value transactions per customer (amount > 5000, row-by-row)
    high_txn_count_by_customer: dict[int, int] = {}
    if transactions is not None:
        for _, row in transactions.iterrows():
            amount = Decimal(str(row["amount"]))
            if amount > 5000:
                account_id = int(row["account_id"])
                # Note: transactions don't have customer_id directly; use account_id as proxy
                # In this simplified model we use account_id as customer_id
                if account_id not in high_txn_count_by_customer:
                    high_txn_count_by_customer[account_id] = 0
                high_txn_count_by_customer[account_id] += 1

    # Build output per customer
    output_rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""

        compliance_count = compliance_count_by_customer.get(customer_id, 0)
        wire_count = wire_count_by_customer.get(customer_id, 0)
        high_txn_count = high_txn_count_by_customer.get(customer_id, 0)

        # W6: double epsilon — use float arithmetic instead of decimal
        risk_score = (compliance_count * 30.0) + (wire_count * 20.0) + (high_txn_count * 10.0)

        # W5: banker's rounding
        rounded_score = round(risk_score, 2)

        output_rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "compliance_events": compliance_count,
            "wire_count": wire_count,
            "high_txn_count": high_txn_count,
            "risk_score": rounded_score,
            "ifw_effective_date": cust_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerComplianceRiskCalculator", execute)
