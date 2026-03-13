"""QuarterlyExecutiveKpiBuilder — daily KPIs despite the misleading 'quarterly' name."""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pandas as pd

from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = ["kpi_name", "kpi_value", "ifw_effective_date"]

    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]
    accounts: pd.DataFrame | None = shared_state.get("accounts")  # type: ignore[assignment]
    transactions: pd.DataFrame | None = shared_state.get("transactions")  # type: ignore[assignment]
    investments: pd.DataFrame | None = shared_state.get("investments")  # type: ignore[assignment]
    compliance_events: pd.DataFrame | None = shared_state.get("compliance_events")  # type: ignore[assignment]

    if customers is None or customers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # W2: Weekend fallback to Friday
    max_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    # AP9: Misleading name -- "quarterly" but actually produces daily KPIs
    # AP2: Duplicates logic from executive_dashboard and other summary jobs

    # total_customers
    total_customers = Decimal(len(customers))

    # total_accounts + total_balance
    total_accounts = Decimal(0)
    total_balance = Decimal(0)
    if accounts is not None:
        for _, row in accounts.iterrows():
            total_accounts += 1
            total_balance += Decimal(str(row["current_balance"]))

    # total_transactions + total_txn_amount
    total_transactions = Decimal(0)
    total_txn_amount = Decimal(0)
    if transactions is not None:
        for _, row in transactions.iterrows():
            total_transactions += 1
            total_txn_amount += Decimal(str(row["amount"]))

    # total_investments + total_investment_value
    total_investments = Decimal(0)
    total_investment_value = Decimal(0)
    if investments is not None:
        for _, row in investments.iterrows():
            total_investments += 1
            total_investment_value += Decimal(str(row["current_value"]))

    # compliance_events_count
    compliance_count = Decimal(len(compliance_events)) if compliance_events is not None else Decimal(0)

    # Build KPI rows — C# uses Math.Round(value, 2) which is banker's rounding
    kpis = [
        ("total_customers", round(float(total_customers), 2)),
        ("total_accounts", round(float(total_accounts), 2)),
        ("total_balance", round(float(total_balance), 2)),
        ("total_transactions", round(float(total_transactions), 2)),
        ("total_txn_amount", round(float(total_txn_amount), 2)),
        ("total_investments", round(float(total_investments), 2)),
        ("total_investment_value", round(float(total_investment_value), 2)),
        ("compliance_events", round(float(compliance_count), 2)),
    ]

    output_rows = []
    for name, value in kpis:
        output_rows.append({
            "kpi_name": name,
            "kpi_value": value,
            "ifw_effective_date": target_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.QuarterlyExecutiveKpiBuilder", execute)
