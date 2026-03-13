"""CustomerAddressDeltaProcessor — detects address changes by comparing current vs previous day snapshots from Postgres."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import psycopg

from etl import connection_helper
from etl.modules.external import register
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY

_COMPARE_FIELDS = [
    "customer_id", "address_line1", "city", "state_province",
    "postal_code", "country", "start_date", "end_date",
]

_OUTPUT_COLUMNS = [
    "change_type", "address_id", "customer_id", "customer_name",
    "address_line1", "city", "state_province", "postal_code",
    "country", "start_date", "end_date", "ifw_effective_date", "record_count",
]


def _normalize(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    return s


def _format_date(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _fetch_addresses(conn, as_of_date: date) -> list[dict[str, object]]:
    query = """
        SELECT address_id, customer_id, address_line1, city, state_province,
               postal_code, country, start_date, end_date
        FROM datalake.addresses
        WHERE ifw_effective_date = %s
        ORDER BY address_id"""

    # C# passes asOfDate.ToDateTime(TimeOnly.MinValue) — a datetime with time 00:00:00
    as_of_datetime = datetime(as_of_date.year, as_of_date.month, as_of_date.day)

    with conn.cursor() as cur:
        cur.execute(query, (as_of_datetime,))
        columns = [desc[0] for desc in cur.description]
        rows = []
        for db_row in cur.fetchall():
            row_dict: dict[str, object] = {}
            for i, col in enumerate(columns):
                row_dict[col] = db_row[i]
            rows.append(row_dict)
    return rows


def _fetch_customer_names(conn, as_of_date: date) -> dict[int, str]:
    query = """
        SELECT DISTINCT ON (id) id, first_name, last_name
        FROM datalake.customers
        WHERE ifw_effective_date <= %s
        ORDER BY id, ifw_effective_date DESC"""

    as_of_datetime = datetime(as_of_date.year, as_of_date.month, as_of_date.day)

    with conn.cursor() as cur:
        cur.execute(query, (as_of_datetime,))
        names: dict[int, str] = {}
        for db_row in cur.fetchall():
            cust_id = int(db_row[0])
            first_name = str(db_row[1])
            last_name = str(db_row[2])
            names[cust_id] = f"{first_name} {last_name}"
    return names


def _has_field_changed(current: dict[str, object], previous: dict[str, object]) -> bool:
    for field in _COMPARE_FIELDS:
        current_val = _normalize(current.get(field))
        previous_val = _normalize(previous.get(field))
        if current_val != previous_val:
            return True
    return False


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    current_date = shared_state[ETL_EFFECTIVE_DATE_KEY]
    previous_date = current_date - timedelta(days=1)

    dsn = connection_helper.get_connection_string()
    with psycopg.connect(dsn) as conn:
        current_addresses = _fetch_addresses(conn, current_date)
        previous_addresses = _fetch_addresses(conn, previous_date)
        customer_names = _fetch_customer_names(conn, current_date)

    # Baseline day: no previous snapshot means no meaningful delta
    if len(previous_addresses) == 0:
        null_row = {
            "change_type": None,
            "address_id": None,
            "customer_id": None,
            "customer_name": None,
            "address_line1": None,
            "city": None,
            "state_province": None,
            "postal_code": None,
            "country": None,
            "start_date": None,
            "end_date": None,
            "ifw_effective_date": current_date.strftime("%Y-%m-%d"),
            "record_count": 0,
        }
        shared_state["output"] = pd.DataFrame([null_row], columns=_OUTPUT_COLUMNS)
        return shared_state

    # Build lookup dictionaries keyed by address_id
    current_by_address_id: dict[int, dict[str, object]] = {}
    for row in current_addresses:
        address_id = int(row["address_id"])
        current_by_address_id[address_id] = row

    previous_by_address_id: dict[int, dict[str, object]] = {}
    for row in previous_addresses:
        address_id = int(row["address_id"])
        previous_by_address_id[address_id] = row

    # Detect deltas
    delta_rows: list[dict[str, object]] = []

    for address_id in sorted(current_by_address_id.keys()):
        current = current_by_address_id[address_id]
        change_type: str | None = None

        if address_id not in previous_by_address_id:
            change_type = "NEW"
        elif _has_field_changed(current, previous_by_address_id[address_id]):
            change_type = "UPDATED"

        if change_type is None:
            continue

        customer_id = int(current["customer_id"])
        customer_name = customer_names.get(customer_id, "")

        country_val = current.get("country")
        country_str = str(country_val).strip() if country_val is not None else None

        delta_rows.append({
            "change_type": change_type,
            "address_id": current["address_id"],
            "customer_id": current["customer_id"],
            "customer_name": customer_name,
            "address_line1": current["address_line1"],
            "city": current["city"],
            "state_province": current["state_province"],
            "postal_code": current["postal_code"],
            "country": country_str,
            "start_date": _format_date(current["start_date"]),
            "end_date": _format_date(current["end_date"]),
            "ifw_effective_date": current_date.strftime("%Y-%m-%d"),
            "record_count": len(delta_rows),  # placeholder, updated below
        })

    record_count = len(delta_rows)

    if record_count == 0:
        # No deltas: single row with nulls except ifw_effective_date and record_count
        delta_rows.append({
            "change_type": None,
            "address_id": None,
            "customer_id": None,
            "customer_name": None,
            "address_line1": None,
            "city": None,
            "state_province": None,
            "postal_code": None,
            "country": None,
            "start_date": None,
            "end_date": None,
            "ifw_effective_date": current_date.strftime("%Y-%m-%d"),
            "record_count": 0,
        })
    else:
        # Set correct record_count on every row
        for row in delta_rows:
            row["record_count"] = record_count

    shared_state["output"] = pd.DataFrame(delta_rows, columns=_OUTPUT_COLUMNS)
    return shared_state


register("ExternalModules.CustomerAddressDeltaProcessor", execute)
