"""CustomerContactabilityProcessor — finds marketing-opted-in customers with valid contact info."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from etl.modules.external import register
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "email_address", "phone_number", "ifw_effective_date",
    ]

    max_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
    if max_date is None:
        max_date = date.today()

    # W2: Weekend fallback — use Friday's data on Sat/Sun
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - timedelta(days=2)

    prefs: pd.DataFrame | None = shared_state.get("customer_preferences")
    customers: pd.DataFrame | None = shared_state.get("customers")
    emails: pd.DataFrame | None = shared_state.get("email_addresses")
    phones: pd.DataFrame | None = shared_state.get("phone_numbers")

    # AP1: segments sourced but never used (dead-end)
    # AP4: unused columns prefix, suffix from customers

    if prefs is None or len(prefs) == 0 or customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    for _, row in customers.iterrows():
        cust_id = int(row["id"])
        customer_lookup[cust_id] = (
            str(row["first_name"]) if pd.notna(row["first_name"]) else "",
            str(row["last_name"]) if pd.notna(row["last_name"]) else "",
        )

    # Build email lookup
    email_lookup: dict[int, str] = {}
    if emails is not None:
        for _, row in emails.iterrows():
            cust_id = int(row["customer_id"])
            email_lookup[cust_id] = str(row["email_address"]) if pd.notna(row["email_address"]) else ""

    # Build phone lookup
    phone_lookup: dict[int, str] = {}
    if phones is not None:
        for _, row in phones.iterrows():
            cust_id = int(row["customer_id"])
            phone_lookup[cust_id] = str(row["phone_number"]) if pd.notna(row["phone_number"]) else ""

    # Find customers with marketing opt-in
    marketing_opt_in: set[int] = set()
    for _, row in prefs.iterrows():
        if target_date != max_date:
            row_date = row["ifw_effective_date"]
            if row_date != target_date:
                continue

        cust_id = int(row["customer_id"])
        pref_type = str(row["preference_type"]) if pd.notna(row["preference_type"]) else ""
        opted_in = bool(row["opted_in"])

        if opted_in and pref_type == "MARKETING_EMAIL":
            marketing_opt_in.add(cust_id)

    output_rows = []
    for cust_id in marketing_opt_in:
        # Must have valid email AND phone AND be in customer lookup
        if cust_id not in customer_lookup:
            continue
        if cust_id not in email_lookup:
            continue
        if cust_id not in phone_lookup:
            continue

        first_name, last_name = customer_lookup[cust_id]

        output_rows.append({
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "email_address": email_lookup[cust_id],
            "phone_number": phone_lookup[cust_id],
            "ifw_effective_date": target_date,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerContactabilityProcessor", execute)
