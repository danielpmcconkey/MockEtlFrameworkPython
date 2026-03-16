"""Tests for parquet_file_writer.py — ported from ParquetFileWriterTests.cs (16 tests)."""

import os
import shutil
import tempfile
from datetime import date, datetime
from glob import glob

import pandas as pd
import pyarrow.parquet as pq
import pytest

from etl.modules.parquet_file_writer import ParquetFileWriter
from etl.modules.data_sourcing import ETL_EFFECTIVE_DATE_KEY
from etl.module_factory import WriteMode
from etl import path_helper
from etl.app_config import AppConfig


TEST_DATE = date(2024, 11, 15)
TEST_DATE_STR = "2024-11-15"


@pytest.fixture(autouse=True)
def init_path_helper():
    path_helper.initialize(AppConfig())


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="MockEtlTests_Parquet_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def make_test_frame():
    return pd.DataFrame(
        {
            "Id": [1, 2, 3],
            "Name": ["Alice", "Bob", "Charlie"],
            "Balance": [100.50, 200.75, 0.0],
            "Active": [True, False, True],
        }
    )


def make_writer(temp_dir, num_parts=1, write_mode=WriteMode.OVERWRITE):
    return ParquetFileWriter(
        source="data",
        output_directory=temp_dir,
        job_dir_name="testjob",
        output_table_dir_name="testtable",
        file_name="output",
        num_parts=num_parts,
        write_mode=write_mode,
    )


def make_state(df=None):
    return {"data": df if df is not None else make_test_frame(), ETL_EFFECTIVE_DATE_KEY: TEST_DATE}


def parquet_dir(temp_dir):
    return os.path.join(temp_dir, "testjob", "testtable", TEST_DATE_STR, "output")


def test_execute_single_part_writes_single_file(temp_dir):
    make_writer(temp_dir).execute(make_state())
    files = glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))
    assert len(files) == 1
    assert files[0].endswith("part-00000.parquet")


def test_execute_multiple_parts_splits_rows_across_files(temp_dir):
    make_writer(temp_dir, num_parts=2).execute(make_state())
    files = sorted(glob(os.path.join(parquet_dir(temp_dir), "*.parquet")))
    assert len(files) == 2
    assert files[0].endswith("part-00000.parquet")
    assert files[1].endswith("part-00001.parquet")


def test_execute_multiple_parts_total_row_count_preserved(temp_dir):
    make_writer(temp_dir, num_parts=2).execute(make_state())
    total = 0
    for f in glob(os.path.join(parquet_dir(temp_dir), "*.parquet")):
        table = pq.read_table(f)
        total += len(table)
    assert total == 3


def test_execute_overwrite_mode_deletes_existing_parquet_in_partition(temp_dir):
    pdir = parquet_dir(temp_dir)
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, "old-part.parquet"), "w") as f:
        f.write("stale")
    make_writer(temp_dir).execute(make_state())
    files = glob(os.path.join(pdir, "*.parquet"))
    assert len(files) == 1
    assert files[0].endswith("part-00000.parquet")


def test_execute_creates_directory_structure(temp_dir):
    make_writer(temp_dir).execute(make_state())
    assert os.path.isdir(parquet_dir(temp_dir))
    assert len(glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))) == 1


def test_execute_missing_dataframe_throws(temp_dir):
    writer = ParquetFileWriter("nonexistent", temp_dir, "testjob", "testtable", "output")
    state = {ETL_EFFECTIVE_DATE_KEY: TEST_DATE}
    with pytest.raises(KeyError):
        writer.execute(state)


def test_execute_missing_effective_date_throws(temp_dir):
    writer = make_writer(temp_dir)
    state = {"data": make_test_frame()}
    with pytest.raises(RuntimeError):
        writer.execute(state)


def test_execute_returns_shared_state_unchanged(temp_dir):
    state = make_state()
    state["other"] = "keep me"
    result = make_writer(temp_dir).execute(state)
    assert result is state
    assert result["other"] == "keep me"


def test_execute_null_values_written_successfully(temp_dir):
    df = pd.DataFrame({"Name": ["Alice", None], "Score": [42, None]})
    make_writer(temp_dir).execute(make_state(df))
    assert len(glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))) == 1


def test_execute_injects_etl_effective_date_column(temp_dir):
    make_writer(temp_dir).execute(make_state())
    f = glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))[0]
    table = pq.read_table(f)
    assert "etl_effective_date" in table.column_names


def test_execute_parquet_schema_matches_dataframe_columns_plus_etl_date(temp_dir):
    make_writer(temp_dir).execute(make_state())
    f = glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))[0]
    table = pq.read_table(f)
    assert len(table.column_names) == 5  # 4 original + etl_effective_date
    assert "Id" in table.column_names
    assert "Name" in table.column_names
    assert "Balance" in table.column_names
    assert "Active" in table.column_names
    assert "etl_effective_date" in table.column_names


def test_execute_date_column_writes_correctly(temp_dir):
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "BirthDate": [date(1990, 5, 15), date(1985, 12, 1)],
    })
    make_writer(temp_dir).execute(make_state(df))
    f = glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))[0]
    table = pq.read_table(f)
    assert "BirthDate" in table.column_names


def test_execute_datetime_column_writes_correctly(temp_dir):
    df = pd.DataFrame({
        "Event": ["Login", "Logout"],
        "Timestamp": [datetime(2024, 11, 15, 10, 30), datetime(2024, 11, 15, 17, 0)],
    })
    make_writer(temp_dir).execute(make_state(df))
    f = glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))[0]
    table = pq.read_table(f)
    assert "Timestamp" in table.column_names


def test_execute_date_column_nulls_handled(temp_dir):
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "EndDate": [date(2025, 1, 1), None],
    })
    make_writer(temp_dir).execute(make_state(df))
    assert len(glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))) == 1


def test_execute_append_mode_first_run_writes_normally(temp_dir):
    make_writer(temp_dir, write_mode=WriteMode.APPEND).execute(make_state())
    assert os.path.isdir(parquet_dir(temp_dir))
    assert len(glob(os.path.join(parquet_dir(temp_dir), "*.parquet"))) == 1


def test_execute_append_mode_unions_with_prior_partition(temp_dir):
    # Write prior partition
    prior_writer = ParquetFileWriter("data", temp_dir, "testjob", "testtable", "output",
                                     num_parts=1, write_mode=WriteMode.OVERWRITE)
    prior_df = pd.DataFrame({
        "Id": [1, 2],
        "Name": ["Alice", "Bob"],
        "Balance": [100.50, 200.75],
        "Active": [True, False],
    })
    prior_state = {"data": prior_df, ETL_EFFECTIVE_DATE_KEY: date(2024, 11, 14)}
    prior_writer.execute(prior_state)

    # Append with new data
    new_df = pd.DataFrame({"Id": [3], "Name": ["Charlie"], "Balance": [0.0], "Active": [True]})
    make_writer(temp_dir, write_mode=WriteMode.APPEND).execute(make_state(new_df))

    total = 0
    for f in glob(os.path.join(parquet_dir(temp_dir), "*.parquet")):
        table = pq.read_table(f)
        total += len(table)
    assert total == 3


def test_execute_append_mode_rerun_ignores_future_partitions(temp_dir):
    """Re-running Oct 1 when Oct 31 already exists must NOT pull in Oct 31 data."""
    table_base = os.path.join(temp_dir, "testjob", "testtable")

    # Simulate a previous full run: write partitions for Oct 1 and Oct 31
    for day_date, name in [(date(2024, 10, 1), "Alice"), (date(2024, 10, 31), "Zara")]:
        prior_writer = ParquetFileWriter(
            "data", temp_dir, "testjob", "testtable", "output",
            num_parts=1, write_mode=WriteMode.OVERWRITE,
        )
        df = pd.DataFrame({"Id": [1], "Name": [name], "Balance": [100.0], "Active": [True]})
        prior_writer.execute({"data": df, ETL_EFFECTIVE_DATE_KEY: day_date})

    # Re-run Oct 1 with corrected data
    rerun_date = date(2024, 10, 1)
    new_df = pd.DataFrame({"Id": [99], "Name": ["Fixed"], "Balance": [0.0], "Active": [True]})
    writer = ParquetFileWriter(
        "data", temp_dir, "testjob", "testtable", "output",
        num_parts=1, write_mode=WriteMode.APPEND,
    )
    writer.execute({"data": new_df, ETL_EFFECTIVE_DATE_KEY: rerun_date})

    # Read Oct 1 output — should contain only the new row
    oct1_dir = os.path.join(table_base, "2024-10-01", "output")
    total = 0
    names = []
    for f in glob(os.path.join(oct1_dir, "*.parquet")):
        table = pq.read_table(f)
        total += len(table)
        names.extend(table.column("Name").to_pylist())
    assert total == 1
    assert "Fixed" in names
    assert "Zara" not in names
