"""CoveredTransactionProcessor — joins transactions with checking accounts, customers, addresses, segments via direct Postgres queries."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Sequence

import pandas as pd
import psycopg

from etl import connection_helper
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.modules.external import register

OUTPUT_COLUMNS = [
    "transaction_id", "txn_timestamp", "txn_type", "amount", "description",
    "customer_id", "name_prefix", "first_name", "last_name", "sort_name",
    "name_suffix", "customer_segment", "address_id", "address_line1",
    "city", "state_province", "postal_code", "country",
    "account_id", "account_type", "account_status", "account_opened",
    "ifw_effective_date", "record_count",
]


def _fetch_rows(
    conn: psycopg.Connection,
    query: str,
    params: Sequence[Any],
) -> list[dict[str, object]]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows = []
        for db_row in cur.fetchall():
            row: dict[str, object] = {}
            for i, col in enumerate(columns):
                val = db_row[i]
                row[col] = None if val is None else val
            rows.append(row)
        return rows


def _format_timestamp(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _format_date(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return date(value.year, value.month, value.day).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _trim(val: object) -> str | None:
    if val is None:
        return None
    return str(val).strip()


def execute(shared_state: dict[str, object]) -> dict[str, object]:
    effective_date: date = shared_state[ETL_EFFECTIVE_DATE_KEY]  # type: ignore[assignment]
    date_param = datetime(effective_date.year, effective_date.month, effective_date.day)

    dsn = connection_helper.get_connection_string()
    with psycopg.connect(dsn) as conn:
        # 1. Fetch transactions for the effective date
        transactions = _fetch_rows(conn,
            """SELECT transaction_id, account_id, txn_timestamp, txn_type, amount, description
              FROM datalake.transactions WHERE ifw_effective_date = %s""",
            (date_param,))

        # 2. Fetch accounts with snapshot fallback (most recent <= effective date), Checking only
        account_rows = _fetch_rows(conn,
            """SELECT DISTINCT ON (account_id) account_id, customer_id, account_type, account_status, open_date
              FROM datalake.accounts WHERE ifw_effective_date <= %s
              ORDER BY account_id, ifw_effective_date DESC""",
            (date_param,))

        checking_accounts: dict[int, dict[str, object]] = {}
        for row in account_rows:
            if str(row.get("account_type", "")).strip() == "Checking":
                account_id = int(row["account_id"])  # type: ignore[arg-type]
                checking_accounts[account_id] = row

        # 3. Fetch customers with snapshot fallback
        customer_rows = _fetch_rows(conn,
            """SELECT DISTINCT ON (id) id, prefix, first_name, last_name, sort_name, suffix
              FROM datalake.customers WHERE ifw_effective_date <= %s
              ORDER BY id, ifw_effective_date DESC""",
            (date_param,))

        customers: dict[int, dict[str, object]] = {}
        for row in customer_rows:
            cust_id = int(row["id"])  # type: ignore[arg-type]
            customers[cust_id] = row

        # 4. Fetch active US addresses (ordered by start_date so first = earliest)
        address_rows = _fetch_rows(conn,
            """SELECT address_id, customer_id, address_line1, city, state_province, postal_code, country, start_date
              FROM datalake.addresses
              WHERE ifw_effective_date = %s AND country = 'US' AND (end_date IS NULL OR end_date >= %s)
              ORDER BY customer_id, start_date ASC""",
            (date_param, date_param))

        active_us_addresses: dict[int, dict[str, object]] = {}
        for row in address_rows:
            customer_id = int(row["customer_id"])  # type: ignore[arg-type]
            # First row per customer_id wins (earliest start_date due to ORDER BY)
            if customer_id not in active_us_addresses:
                active_us_addresses[customer_id] = row

        # 5. Fetch segment mappings (deduplicated, first alphabetically)
        segment_rows = _fetch_rows(conn,
            """SELECT DISTINCT ON (cs.customer_id) cs.customer_id, s.segment_code
              FROM datalake.customers_segments cs
              JOIN datalake.segments s ON cs.segment_id = s.segment_id AND s.ifw_effective_date = cs.ifw_effective_date
              WHERE cs.ifw_effective_date = %s
              ORDER BY cs.customer_id, s.segment_code ASC""",
            (date_param,))

        segments: dict[int, str] = {}
        for row in segment_rows:
            customer_id = int(row["customer_id"])  # type: ignore[arg-type]
            segments[customer_id] = str(row.get("segment_code", "") or "")

    # 6. Join and filter: transaction -> checking account -> customer with active US address
    output_rows: list[tuple[int, int, dict[str, object]]] = []

    for txn in transactions:
        account_id = int(txn["account_id"])  # type: ignore[arg-type]

        # Must be a Checking account
        if account_id not in checking_accounts:
            continue
        account = checking_accounts[account_id]

        customer_id = int(account["customer_id"])  # type: ignore[arg-type]

        # Must have an active US address
        if customer_id not in active_us_addresses:
            continue
        address = active_us_addresses[customer_id]

        # Look up customer demographics
        customer = customers.get(customer_id)

        # Look up segment
        segment_code = segments.get(customer_id)

        transaction_id = int(txn["transaction_id"])  # type: ignore[arg-type]

        output_row: dict[str, object] = {
            "transaction_id": txn["transaction_id"],
            "txn_timestamp": _format_timestamp(txn["txn_timestamp"]),
            "txn_type": _trim(txn["txn_type"]),
            "amount": txn["amount"],
            "description": _trim(txn["description"]),
            "customer_id": account["customer_id"],
            "name_prefix": _trim(customer["prefix"]) if customer else None,
            "first_name": _trim(customer["first_name"]) if customer else None,
            "last_name": _trim(customer["last_name"]) if customer else None,
            "sort_name": _trim(customer["sort_name"]) if customer else None,
            "name_suffix": _trim(customer["suffix"]) if customer else None,
            "customer_segment": segment_code,
            "address_id": address["address_id"],
            "address_line1": _trim(address["address_line1"]),
            "city": _trim(address["city"]),
            "state_province": _trim(address["state_province"]),
            "postal_code": _trim(address["postal_code"]),
            "country": _trim(address["country"]),
            "account_id": account["account_id"],
            "account_type": _trim(account["account_type"]),
            "account_status": _trim(account["account_status"]),
            "account_opened": _format_date(account["open_date"]),
            "ifw_effective_date": effective_date.isoformat(),
            "record_count": 0,  # placeholder
        }

        output_rows.append((customer_id, transaction_id, output_row))

    # Sort: customer_id ASC, transaction_id DESC
    output_rows.sort(key=lambda x: (x[0], -x[1]))

    final_rows = [row for _, _, row in output_rows]
    record_count = len(final_rows)

    if record_count == 0:
        # Zero-row case: single null row with ifw_effective_date and record_count = 0
        null_row: dict[str, object] = {col: None for col in OUTPUT_COLUMNS}
        null_row["ifw_effective_date"] = effective_date.isoformat()
        null_row["record_count"] = 0
        final_rows.append(null_row)
    else:
        for row in final_rows:
            row["record_count"] = record_count

    shared_state["output"] = pd.DataFrame(final_rows, columns=OUTPUT_COLUMNS)
    return shared_state


register("ExternalModules.CoveredTransactionProcessor", execute)
