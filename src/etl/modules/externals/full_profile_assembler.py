"""FullProfileAssembler — assemble customer profiles with age, phone, email, and segments."""
from __future__ import annotations

from datetime import date

import pandas as pd

from etl.modules.external import register


def _to_date(val: object) -> date:
    """Convert a value to datetime.date, matching C# ToDateOnly behaviour."""
    if isinstance(val, date):
        return val
    s = str(val)
    return date.fromisoformat(s)


def _add_years(d: date, years: int) -> date:
    """Add years to a date, clamping Feb 29 to Feb 28 in non-leap years (matches C# AddYears)."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        # Feb 29 in a non-leap target year -> Feb 28
        return d.replace(year=d.year + years, day=28)


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    output_columns = [
        "customer_id", "first_name", "last_name", "age", "age_bracket",
        "primary_phone", "primary_email", "segments", "ifw_effective_date",
    ]

    customers: pd.DataFrame | None = shared_state.get("customers")  # type: ignore[assignment]

    if customers is None or customers.empty:
        shared_state["output"] = pd.DataFrame(columns=output_columns)
        return shared_state

    phone_numbers: pd.DataFrame | None = shared_state.get("phone_numbers")  # type: ignore[assignment]
    email_addresses: pd.DataFrame | None = shared_state.get("email_addresses")  # type: ignore[assignment]
    customers_segments: pd.DataFrame | None = shared_state.get("customers_segments")  # type: ignore[assignment]
    segments: pd.DataFrame | None = shared_state.get("segments")  # type: ignore[assignment]

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

    # Build segment_id -> segment_name lookup
    segment_names: dict[int, str] = {}
    if segments is not None:
        for _, seg_row in segments.iterrows():
            seg_id = int(seg_row["segment_id"])
            segment_names[seg_id] = str(seg_row["segment_name"]) if pd.notna(seg_row["segment_name"]) else ""

    # Build customer_id -> list of segment_ids
    customer_segment_ids: dict[int, list[int]] = {}
    if customers_segments is not None:
        for _, cs_row in customers_segments.iterrows():
            cust_id = int(cs_row["customer_id"])
            seg_id = int(cs_row["segment_id"])
            if cust_id not in customer_segment_ids:
                customer_segment_ids[cust_id] = []
            customer_segment_ids[cust_id].append(seg_id)

    rows = []
    for _, cust_row in customers.iterrows():
        customer_id = int(cust_row["id"])
        first_name = str(cust_row["first_name"]) if pd.notna(cust_row["first_name"]) else ""
        last_name = str(cust_row["last_name"]) if pd.notna(cust_row["last_name"]) else ""

        birthdate = _to_date(cust_row["birthdate"])
        as_of_date = _to_date(cust_row["ifw_effective_date"])

        # C#: age = asOfDate.Year - birthdate.Year;
        #     if (birthdate > asOfDate.AddYears(-age)) age--;
        age = as_of_date.year - birthdate.year
        if birthdate > _add_years(as_of_date, -age):
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

        # Build comma-separated segment names
        seg_list = customer_segment_ids.get(customer_id, [])
        seg_names_list = [segment_names[seg_id] for seg_id in seg_list if seg_id in segment_names]
        segments_str = ",".join(seg_names_list)

        rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "age": age,
            "age_bracket": age_bracket,
            "primary_phone": primary_phone,
            "primary_email": primary_email,
            "segments": segments_str,
            "ifw_effective_date": cust_row["ifw_effective_date"],
        })

    shared_state["output"] = pd.DataFrame(rows, columns=output_columns)
    return shared_state


register("ExternalModules.FullProfileAssembler", execute)
