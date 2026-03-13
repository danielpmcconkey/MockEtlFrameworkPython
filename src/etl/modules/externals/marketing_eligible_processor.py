"""MarketingEligibleProcessor — find customers opted in to all 3 marketing channels."""
from __future__ import annotations

import datetime

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "email_address", "ifw_effective_date",
    ]

    max_date = shared_state.get("__etlEffectiveDate")
    if max_date is None:
        max_date = datetime.date.today()

    # W2: Weekend fallback — use Friday's data on Sat/Sun
    target_date = max_date
    if max_date.weekday() == 5:  # Saturday
        target_date = max_date - datetime.timedelta(days=1)
    elif max_date.weekday() == 6:  # Sunday
        target_date = max_date - datetime.timedelta(days=2)

    prefs: pd.DataFrame | None = shared_state.get("customer_preferences")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]
    emails: pd.DataFrame | None = shared_state.get("email_addresses")  # type: ignore[assignment]

    if (prefs is None or prefs.empty
            or customers is None or customers.empty):
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build customer lookup
    customer_lookup: dict[int, tuple[str, str]] = {}
    for _, row in customers.iterrows():
        cust_id = int(row["id"])
        first_name = str(row["first_name"]) if pd.notna(row["first_name"]) else ""
        last_name = str(row["last_name"]) if pd.notna(row["last_name"]) else ""
        customer_lookup[cust_id] = (first_name, last_name)

    # Build email lookup
    email_lookup: dict[int, str] = {}
    if emails is not None:
        for _, row in emails.iterrows():
            cust_id = int(row["customer_id"])
            email_lookup[cust_id] = str(row["email_address"]) if pd.notna(row["email_address"]) else ""

    # AP6: Row-by-row — find customers opted in to ALL 3 marketing channels
    required_types = {"MARKETING_EMAIL", "MARKETING_SMS", "PUSH_NOTIFICATIONS"}

    # Build customer_id -> set of opted-in preference types
    customer_opt_ins: dict[int, set[str]] = {}
    for _, row in prefs.iterrows():
        if target_date != max_date:
            row_date = row["ifw_effective_date"]
            if row_date != target_date:
                continue

        cust_id = int(row["customer_id"])
        pref_type = str(row["preference_type"]) if pd.notna(row["preference_type"]) else ""
        opted_in = bool(row["opted_in"])

        if opted_in and pref_type in required_types:
            if cust_id not in customer_opt_ins:
                customer_opt_ins[cust_id] = set()
            customer_opt_ins[cust_id].add(pref_type)

    rows = []
    for cust_id, opt_ins in customer_opt_ins.items():
        if len(opt_ins) == len(required_types) and cust_id in customer_lookup:
            first_name, last_name = customer_lookup[cust_id]
            email = email_lookup.get(cust_id, "")

            rows.append({
                "customer_id": cust_id,
                "first_name": first_name,
                "last_name": last_name,
                "email_address": email,
                "ifw_effective_date": target_date,
            })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.MarketingEligibleProcessor", execute)
