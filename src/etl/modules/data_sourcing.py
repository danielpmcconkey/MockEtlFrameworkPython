"""DataSourcing module — reads from Postgres datalake with date resolution."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import psycopg

from etl import connection_helper
from etl.modules.base import Module

ETL_EFFECTIVE_DATE_KEY = "__etlEffectiveDate"


class DataSourcing(Module):
    def __init__(
        self,
        result_name: str,
        schema: str,
        table: str,
        columns: list[str],
        min_effective_date: date | None = None,
        max_effective_date: date | None = None,
        additional_filter: str = "",
        lookback_days: int | None = None,
        most_recent_prior: bool = False,
        most_recent: bool = False,
    ) -> None:
        self.result_name = result_name
        self.schema = schema
        self.table = table
        self.columns = list(columns)
        self.min_effective_date = min_effective_date
        self.max_effective_date = max_effective_date
        self.additional_filter = additional_filter or ""
        self.lookback_days = lookback_days
        self.most_recent_prior = most_recent_prior
        self.most_recent = most_recent

        self._validate_date_modes()

    def _validate_date_modes(self) -> None:
        has_static = self.min_effective_date is not None or self.max_effective_date is not None
        has_lookback = self.lookback_days is not None
        has_mrp = self.most_recent_prior
        has_mr = self.most_recent

        mode_count = sum([has_static, has_lookback, has_mrp, has_mr])
        if mode_count > 1:
            raise ValueError(
                "DataSourcing date modes are mutually exclusive. "
                "Specify only one of: static dates (minEffectiveDate/maxEffectiveDate), "
                "lookbackDays, mostRecentPrior, or mostRecent."
            )

        if self.lookback_days is not None and self.lookback_days < 0:
            raise ValueError("lookbackDays must be >= 0.")

    def resolve_date_range(
        self, shared_state: dict[str, object]
    ) -> tuple[date, date] | None:
        """Resolve min/max date range. Returns None when no matching date found."""
        if self.most_recent_prior:
            t0 = self._get_etl_effective_date(shared_state)
            prior = self._query_most_recent_date(t0, strict=True)
            return (prior, prior) if prior else None

        if self.most_recent:
            t0 = self._get_etl_effective_date(shared_state)
            recent = self._query_most_recent_date(t0, strict=False)
            return (recent, recent) if recent else None

        if self.lookback_days is not None:
            t0 = self._get_etl_effective_date(shared_state)
            min_d = t0 - timedelta(days=self.lookback_days)
            return (min_d, t0)

        # Static dates or fallback to __etlEffectiveDate
        min_d = self.min_effective_date or self._get_etl_effective_date(shared_state)
        max_d = self.max_effective_date or self._get_etl_effective_date(shared_state)
        return (min_d, max_d)

    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        date_range = self.resolve_date_range(shared_state)

        if date_range is None:
            # No matching date — empty DataFrame with correct schema
            cols = list(self.columns)
            if "ifw_effective_date" not in [c.lower() for c in cols]:
                cols.append("ifw_effective_date")
            shared_state[self.result_name] = pd.DataFrame(columns=cols)
        else:
            min_d, max_d = date_range
            shared_state[self.result_name] = self._fetch_data(min_d, max_d)

        return shared_state

    def _get_etl_effective_date(self, shared_state: dict[str, object]) -> date:
        val = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
        if isinstance(val, date):
            return val
        raise RuntimeError(
            f"DataSourcing '{self.result_name}': no effective date available. "
            f"Inject '{ETL_EFFECTIVE_DATE_KEY}' into shared state."
        )

    def _query_most_recent_date(self, as_of: date, *, strict: bool) -> date | None:
        op = "<" if strict else "<="
        query = f"""
            SELECT MAX(ifw_effective_date)
            FROM "{self.schema}"."{self.table}"
            WHERE ifw_effective_date {op} %s
        """
        dsn = connection_helper.get_dsn_dict()
        with psycopg.connect(**dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (as_of,))
                row = cur.fetchone()
                if row is None or row[0] is None:
                    return None
                return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))

    def _fetch_data(self, min_date: date, max_date: date) -> pd.DataFrame:
        includes_as_of = any(
            c.lower() == "ifw_effective_date" for c in self.columns
        )
        col_list = ", ".join(f'"{c}"' for c in self.columns)
        select_clause = col_list if includes_as_of else f"{col_list}, ifw_effective_date"

        query = f"""
            SELECT {select_clause}
            FROM "{self.schema}"."{self.table}"
            WHERE ifw_effective_date >= %s
              AND ifw_effective_date <= %s
        """
        if self.additional_filter.strip():
            query += f" AND ({self.additional_filter})"

        query += " ORDER BY ifw_effective_date"

        dsn = connection_helper.get_dsn_dict()
        with psycopg.connect(**dsn) as conn:
            df = pd.read_sql(query, conn, params=(min_date, max_date))

        if df.empty:
            cols = list(self.columns)
            if not includes_as_of:
                cols.append("ifw_effective_date")
            return pd.DataFrame(columns=cols)

        return df
