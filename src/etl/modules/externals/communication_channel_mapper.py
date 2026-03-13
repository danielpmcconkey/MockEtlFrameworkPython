"""CommunicationChannelMapper — maps customers to preferred communication channel."""
from __future__ import annotations

import pandas as pd

from etl.modules.external import register


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "preferred_channel",
        "email", "phone", "ifw_effective_date",
    ]

    prefs: pd.DataFrame | None = shared_state.get("customer_preferences")  # type: ignore[assignment]
    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]
    emails: pd.DataFrame | None = shared_state.get("email_addresses")  # type: ignore[assignment]
    phones: pd.DataFrame | None = shared_state.get("phone_numbers")  # type: ignore[assignment]

    if customers is None or customers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    # Build email lookup
    email_lookup: dict[int, str] = {}
    if emails is not None:
        for _, row in emails.iterrows():
            cust_id = int(row["customer_id"])
            email_lookup[cust_id] = str(row.get("email_address", "") or "")

    # Build phone lookup
    phone_lookup: dict[int, str] = {}
    if phones is not None:
        for _, row in phones.iterrows():
            cust_id = int(row["customer_id"])
            phone_lookup[cust_id] = str(row.get("phone_number", "") or "")

    # Build preference lookup: customer_id -> set of opted-in types
    pref_lookup: dict[int, set[str]] = {}
    if prefs is not None:
        for _, row in prefs.iterrows():
            cust_id = int(row["customer_id"])
            pref_type = str(row.get("preference_type", "") or "")
            opted_in = bool(row["opted_in"])

            if opted_in:
                if cust_id not in pref_lookup:
                    pref_lookup[cust_id] = set()
                pref_lookup[cust_id].add(pref_type)

    as_of = customers.iloc[0]["ifw_effective_date"]

    # AP6: Row-by-row iteration through customers
    output_rows = []
    for _, cust_row in customers.iterrows():
        cust_id = int(cust_row["id"])
        first_name = str(cust_row.get("first_name", "") or "")
        last_name = str(cust_row.get("last_name", "") or "")

        cust_prefs = pref_lookup.get(cust_id, set())

        # Determine preferred channel
        if "MARKETING_EMAIL" in cust_prefs:
            preferred_channel = "Email"
        elif "MARKETING_SMS" in cust_prefs:
            preferred_channel = "SMS"
        elif "PUSH_NOTIFICATIONS" in cust_prefs:
            preferred_channel = "Push"
        else:
            preferred_channel = "None"

        # AP5: Asymmetric NULL handling — null email -> "N/A" but null phone -> "" (empty string)
        email = email_lookup.get(cust_id, "N/A")
        phone = phone_lookup.get(cust_id, "")

        output_rows.append({
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "preferred_channel": preferred_channel,
            "email": email,
            "phone": phone,
            "ifw_effective_date": as_of,
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CommunicationChannelMapper", execute)
