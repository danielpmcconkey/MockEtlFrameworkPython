"""CsvFileWriter module — writes date-partitioned CSV files."""

from __future__ import annotations

import csv
import io
import os
from datetime import date, datetime, timezone

import pandas as pd

from etl import date_partition_helper, path_helper
from etl.module_factory import WriteMode
from etl.modules.base import Module
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


class CsvFileWriter(Module):
    def __init__(
        self,
        source: str,
        output_directory: str,
        job_dir_name: str,
        output_table_dir_name: str,
        file_name: str,
        include_header: bool = True,
        trailer_format: str | None = None,
        write_mode: WriteMode = WriteMode.OVERWRITE,
        line_ending: str = "\n",
    ) -> None:
        self.source = source
        self.output_directory = output_directory
        self.job_dir_name = job_dir_name
        self.output_table_dir_name = output_table_dir_name
        self.file_name = file_name
        self.include_header = include_header
        self.trailer_format = trailer_format
        self.write_mode = write_mode
        self.line_ending = line_ending

    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        df = shared_state.get(self.source)
        if not isinstance(df, pd.DataFrame):
            raise KeyError(f"DataFrame '{self.source}' not found in shared state.")

        effective_date = shared_state.get(ETL_EFFECTIVE_DATE_KEY)
        if effective_date is None:
            raise RuntimeError(
                f"'{ETL_EFFECTIVE_DATE_KEY}' not found in shared state."
            )

        date_str = effective_date.isoformat() if hasattr(effective_date, "isoformat") else str(effective_date)
        table_dir = os.path.join(
            path_helper.resolve(self.output_directory),
            self.job_dir_name,
            self.output_table_dir_name,
        )

        # Append mode: union with prior partition's data
        if self.write_mode == WriteMode.APPEND:
            prior_date = date_partition_helper.find_latest_partition(table_dir)
            if prior_date is not None:
                prior_path = os.path.join(table_dir, prior_date, self.file_name)
                if os.path.isfile(prior_path):
                    prior_df = _read_csv_with_trailer(
                        prior_path, has_trailer=self.trailer_format is not None
                    )
                    if "etl_effective_date" in prior_df.columns:
                        prior_df = prior_df.drop(columns=["etl_effective_date"])
                    df = pd.concat([prior_df, df], ignore_index=True)

        # Inject etl_effective_date column
        df = df.copy()
        df["etl_effective_date"] = date_str

        # Build output path
        partition_dir = os.path.join(table_dir, date_str)
        os.makedirs(partition_dir, exist_ok=True)
        output_path = os.path.join(partition_dir, self.file_name)

        with open(output_path, "w", encoding="utf-8", newline="") as f:
            if self.include_header:
                f.write(",".join(df.columns) + self.line_ending)

            for _, row in df.iterrows():
                fields = [_format_field(row[col]) for col in df.columns]
                f.write(",".join(fields) + self.line_ending)

            if self.trailer_format is not None:
                trailer = (
                    self.trailer_format
                    .replace("{row_count}", str(len(df)))
                    .replace("{date}", date_str)
                    .replace(
                        "{timestamp}",
                        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    )
                )
                f.write(trailer + self.line_ending)

        return shared_state


def _format_field(val: object) -> str:
    """RFC 4180: quote fields with commas, double quotes, or newlines. NULLs -> empty."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    # Match C# DateOnly.ToString() → M/d/yyyy (no leading zeros).
    # Use type() not isinstance() — datetime is a subclass of date.
    if type(val) is date:
        return val.strftime("%-m/%-d/%Y")
    s = str(val)
    if "," in s or '"' in s or "\n" in s or "\r" in s:
        return '"' + s.replace('"', '""') + '"'
    return s


def _read_csv_with_trailer(
    path: str, *, has_trailer: bool
) -> pd.DataFrame:
    """Read CSV, stripping the last line if a trailer is present."""
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    if has_trailer and len(lines) > 1:
        lines = lines[:-1]

    if not lines:
        return pd.DataFrame()

    # Parse via pandas from the cleaned lines
    return pd.read_csv(io.StringIO("\n".join(lines)), dtype=str, keep_default_na=False)
