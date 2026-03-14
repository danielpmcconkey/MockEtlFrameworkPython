"""ParquetFileWriter module — writes date-partitioned Parquet files."""

from __future__ import annotations

import decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from etl import date_partition_helper, path_helper
from etl.modules.base import Module, WriteMode
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY


class ParquetFileWriter(Module):
    def __init__(
        self,
        source: str,
        output_directory: str,
        job_dir_name: str,
        output_table_dir_name: str,
        file_name: str,
        num_parts: int = 1,
        write_mode: WriteMode = WriteMode.OVERWRITE,
    ) -> None:
        self.source = source
        self.output_directory = output_directory
        self.job_dir_name = job_dir_name
        self.output_table_dir_name = output_table_dir_name
        self.file_name = file_name
        self.num_parts = max(1, num_parts)
        self.write_mode = write_mode

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
        table_dir = Path(path_helper.resolve(self.output_directory)) / self.job_dir_name / self.output_table_dir_name

        # Append mode: union with prior partition's data
        if self.write_mode == WriteMode.APPEND:
            prior_date = date_partition_helper.find_latest_partition(str(table_dir))
            if prior_date is not None:
                prior_dir = table_dir / prior_date / self.file_name
                if prior_dir.is_dir():
                    prior_df = _read_parquet_dir(prior_dir)
                    if "etl_effective_date" in prior_df.columns:
                        prior_df = prior_df.drop(columns=["etl_effective_date"])
                    df = pd.concat([prior_df, df], ignore_index=True)

        # Inject etl_effective_date
        df = df.copy()
        df["etl_effective_date"] = date_str

        # Output path: {tableDir}/{date}/{fileName}/
        parquet_dir = table_dir / date_str / self.file_name

        # Overwrite: delete existing parquet files
        if self.write_mode == WriteMode.OVERWRITE and parquet_dir.is_dir():
            for f in parquet_dir.glob("*.parquet"):
                f.unlink()

        parquet_dir.mkdir(parents=True, exist_ok=True)

        if df.empty or len(df.columns) == 0:
            return shared_state

        # Build explicit Arrow schema so Decimal columns write as
        # decimal128(38, 18), matching C# Parquet output exactly.
        arrow_schema = _build_arrow_schema(df)

        total = len(df)
        part_size = total // self.num_parts
        remainder = total % self.num_parts

        offset = 0
        for part in range(self.num_parts):
            count = part_size + (1 if part < remainder else 0)
            part_df = df.iloc[offset : offset + count]
            offset += count

            file_path = parquet_dir / f"part-{part:05d}.parquet"
            table = pa.Table.from_pandas(part_df, schema=arrow_schema, preserve_index=False)
            pq.write_table(table, file_path)

        return shared_state


def _build_arrow_schema(df: pd.DataFrame) -> pa.Schema:
    """Infer an Arrow schema from *df*, promoting Decimal columns to decimal128(38, 18)."""
    fields: list[pa.Field] = []
    for col in df.columns:
        if df[col].dtype == object and len(df) > 0:
            sample = df[col].dropna().iloc[0] if df[col].dropna().any() else None
            if isinstance(sample, decimal.Decimal):
                fields.append(pa.field(col, pa.decimal128(38, 18)))
                continue
        # Let pyarrow infer the type for everything else
        fields.append(pa.field(col, pa.Array.from_pandas(df[col]).type))
    return pa.schema(fields)


def _read_parquet_dir(directory: Path) -> pd.DataFrame:
    """Read all .parquet files in a directory and concatenate."""
    files = sorted(directory.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pq.read_table(str(f)).to_pandas() for f in files]
    result = pd.concat(dfs, ignore_index=True)
    return result
