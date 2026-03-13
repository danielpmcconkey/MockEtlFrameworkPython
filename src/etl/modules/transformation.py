"""Transformation module — executes SQL against in-memory SQLite."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime

import pandas as pd

from etl.modules.base import Module


class Transformation(Module):
    def __init__(self, result_name: str, sql: str) -> None:
        self.result_name = result_name
        self.sql = sql

    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        conn = sqlite3.connect(":memory:")
        try:
            for key, value in shared_state.items():
                if isinstance(value, pd.DataFrame):
                    _register_table(conn, key, value)

            result = pd.read_sql_query(self.sql, conn)
            shared_state[self.result_name] = result
        finally:
            conn.close()

        return shared_state


def _register_table(conn: sqlite3.Connection, name: str, df: pd.DataFrame) -> None:
    if df.columns.empty:
        return

    # Convert date/datetime columns to strings for SQLite compatibility
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == "object":
            df_copy[col] = df_copy[col].apply(_to_sqlite_value)

    df_copy.to_sql(name, conn, if_exists="replace", index=False)


def _to_sqlite_value(val: object) -> object:
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%dT%H:%M:%S")
    return val
