"""DataFrameWriter module — writes a DataFrame to Postgres."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import psycopg

from etl import connection_helper
from etl.module_factory import WriteMode
from etl.modules.base import Module


class DataFrameWriter(Module):
    def __init__(
        self,
        source: str,
        target_table: str,
        write_mode: WriteMode,
        target_schema: str = "curated",
    ) -> None:
        self.source = source
        self.target_table = target_table
        self.write_mode = write_mode
        self.target_schema = target_schema

    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        df = shared_state.get(self.source)
        if not isinstance(df, pd.DataFrame):
            raise KeyError(f"DataFrame '{self.source}' not found in shared state.")

        dsn = connection_helper.get_dsn_dict()
        with psycopg.connect(**dsn) as conn:
            self._ensure_table_exists(conn, df)

            if self.write_mode == WriteMode.OVERWRITE:
                conn.execute(
                    f'TRUNCATE TABLE "{self.target_schema}"."{self.target_table}"'
                )

            self._insert_rows(conn, df)
            conn.commit()

        return shared_state

    def _ensure_table_exists(
        self, conn: psycopg.Connection, df: pd.DataFrame
    ) -> None:
        col_defs = []
        for col in df.columns:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            pg_type = _get_postgres_type(sample)
            col_defs.append(f'"{col}" {pg_type}')

        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{self.target_schema}"."{self.target_table}" '
            f'({", ".join(col_defs)})'
        )
        conn.execute(ddl)

    def _insert_rows(self, conn: psycopg.Connection, df: pd.DataFrame) -> None:
        if df.empty:
            return

        col_names = ", ".join(f'"{c}"' for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f'INSERT INTO "{self.target_schema}"."{self.target_table}" ({col_names}) VALUES ({placeholders})'

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                values = [_coerce_value(row[c]) for c in df.columns]
                cur.execute(sql, values)


def _coerce_value(val: object) -> object:
    """Coerce values that lost type during SQLite round-tripping."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, str):
        # Try date
        try:
            return date.fromisoformat(val)
        except ValueError:
            pass
        # Try datetime
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            pass
    return val


def _get_postgres_type(sample: object) -> str:
    if isinstance(sample, (int,)):
        return "INTEGER"
    if isinstance(sample, float):
        return "DOUBLE PRECISION"
    if isinstance(sample, bool):
        return "BOOLEAN"
    if isinstance(sample, date) and not isinstance(sample, datetime):
        return "DATE"
    if isinstance(sample, datetime):
        return "TIMESTAMP"
    return "TEXT"
