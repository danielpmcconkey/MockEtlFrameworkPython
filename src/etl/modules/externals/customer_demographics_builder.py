"""CustomerDemographicsBuilder — builds customer demographics with age brackets and primary contact info."""
from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from etl.modules.external import register


def _to_date(val: object) -> date:
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        return date.fromisoformat(val)
    return date.fromisoformat(str(val))


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "birthdate", "age",
        "age_bracket", "primary_phone", "primary_email", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")

    if customers is None or len(customers) == 0:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    phone_numbers: pd.DataFrame | None = shared_state.get("phone_numbers")
    email_addresses: pd.DataFrame | None = shared_state.get("email_addresses")

    # Build customer_id -> first phone lookup
    phone_by_customer: dict[int, str] = {}
    if phone_numbers is not None:
        for _, phone_row in phone_numbers.iterrows():
            cust_id = int(phone_row["customer_id"])
            if cust_id not in phone_by_customer:
                phone_by_customer[cust_id] = str(phone_row["phone_number"]) if pd.notna(phone_row["phone_number"]) else ""

    # Build customer_id -> first email lookup
    email_by_customer: dict[int, str] = {}
    if email_addresses is not None:
        for _, email_row in email_addresses.iterrows():
            cust_id = int(email_row["customer_id"])
            if cust_id not in email_by_customer:
                email_by_customer[cust_id] = str(email_row["email_address"]) if pd.notna(email_row["email_address"]) else ""

    output_rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""

        birthdate = _to_date(cust_row["birthdate"])
        as_of_date = _to_date(cust_row["ifw_effective_date"])

        age = as_of_date.year - birthdate.year
        if birthdate > as_of_date.replace(year=as_of_date.year - age):
            age -= 1

        if age < 26:
            age_bracket = "18-25"
        elif age <= 35:
            age_bracket = "26-35"
        elif age <= 45:
            age_bracket = "36-45"
        elif age <= 55:
            age_bracket = "46-55"
        elif age <= 65:
            age_bracket = "56-65"
        else:
            age_bracket = "65+"

        primary_phone = phone_by_customer.get(customer_id, "")
        primary_email = email_by_customer.get(customer_id, "")

        output_rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "birthdate": cust_row["birthdate"],
            "age": age,
            "age_bracket": age_bracket,
            "primary_phone": primary_phone,
            "primary_email": primary_email,
            "ifw_effective_date": cust_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(output_rows, columns=output_columns)
    return shared_state


register("ExternalModules.CustomerDemographicsBuilder", execute)
