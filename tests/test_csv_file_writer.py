"""Tests for csv_file_writer.py — ported from CsvFileWriterTests.cs (22 tests)."""

import os
import shutil
import tempfile
from datetime import date

import pandas as pd
import pytest

from etl.modules.csv_file_writer import CsvFileWriter
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
    d = tempfile.mkdtemp(prefix="MockEtlTests_Csv_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def make_test_frame():
    return pd.DataFrame(
        {"Id": [1, 2, 3], "Name": ["Alice", "Bob", "Charlie"], "City": ["New York", "London", "Paris"]}
    )


def make_writer(temp_dir, file_name="output.csv", include_header=True,
                trailer_format=None, write_mode=WriteMode.OVERWRITE,
                line_ending="\n"):
    return CsvFileWriter(
        source="data",
        output_directory=temp_dir,
        job_dir_name="testjob",
        output_table_dir_name="testtable",
        file_name=file_name,
        include_header=include_header,
        trailer_format=trailer_format,
        write_mode=write_mode,
        line_ending=line_ending,
    )


def make_state(df=None):
    return {"data": df if df is not None else make_test_frame(), ETL_EFFECTIVE_DATE_KEY: TEST_DATE}


def output_path(temp_dir, file_name="output.csv"):
    return os.path.join(temp_dir, "testjob", "testtable", TEST_DATE_STR, file_name)


def read_output(temp_dir, file_name="output.csv"):
    with open(output_path(temp_dir, file_name), "r", encoding="utf-8") as f:
        return f.read()


def read_lines(temp_dir, file_name="output.csv"):
    with open(output_path(temp_dir, file_name), "r", encoding="utf-8") as f:
        return f.read().splitlines()


def test_execute_writes_header_and_data_rows(temp_dir):
    make_writer(temp_dir).execute(make_state())
    lines = read_lines(temp_dir)
    assert len(lines) == 4  # header + 3 data rows
    assert lines[0] == "Id,Name,City,etl_effective_date"
    assert lines[1] == f"1,Alice,New York,{TEST_DATE_STR}"


def test_execute_injects_etl_effective_date_column(temp_dir):
    make_writer(temp_dir).execute(make_state())
    lines = read_lines(temp_dir)
    for line in lines[1:]:
        assert line.endswith(TEST_DATE_STR)


def test_execute_no_header_skips_header_row(temp_dir):
    make_writer(temp_dir, include_header=False).execute(make_state())
    lines = read_lines(temp_dir)
    assert len(lines) == 3
    assert lines[0] == f"1,Alice,New York,{TEST_DATE_STR}"


def test_execute_field_with_comma_is_quoted(temp_dir):
    df = pd.DataFrame({"Name": ["Last, First"], "Value": [1]})
    make_writer(temp_dir).execute(make_state(df))
    lines = read_lines(temp_dir)
    assert lines[1] == f'"Last, First",1,{TEST_DATE_STR}'


def test_execute_field_with_double_quote_is_escaped(temp_dir):
    df = pd.DataFrame({"Name": ['She said "hello"'], "Value": [1]})
    make_writer(temp_dir).execute(make_state(df))
    lines = read_lines(temp_dir)
    assert lines[1] == f'"She said ""hello""",1,{TEST_DATE_STR}'


def test_execute_null_values_render_as_empty(temp_dir):
    df = pd.DataFrame({"A": ["x"], "B": [None], "C": ["z"]})
    make_writer(temp_dir).execute(make_state(df))
    lines = read_lines(temp_dir)
    assert lines[1] == f"x,,z,{TEST_DATE_STR}"


def test_execute_trailer_format_writes_trailer_line(temp_dir):
    make_writer(temp_dir, trailer_format="TRAILER|{row_count}|{date}").execute(make_state())
    lines = read_lines(temp_dir)
    assert len(lines) == 5  # header + 3 data + trailer
    assert lines[4] == f"TRAILER|3|{TEST_DATE_STR}"


def test_execute_trailer_format_timestamp_token_resolves(temp_dir):
    make_writer(temp_dir, trailer_format="END|{timestamp}").execute(make_state())
    lines = read_lines(temp_dir)
    trailer = lines[-1]
    assert trailer.startswith("END|")
    assert "T" in trailer


def test_execute_overwrite_mode_writes_to_date_partition(temp_dir):
    make_writer(temp_dir).execute(make_state())
    assert os.path.exists(output_path(temp_dir))
    assert os.path.isdir(os.path.join(temp_dir, "testjob", "testtable", TEST_DATE_STR))


def test_execute_overwrite_mode_rerun_overwrites_partition(temp_dir):
    make_writer(temp_dir).execute(make_state())
    df = pd.DataFrame({"Id": [99], "Name": ["Replacement"], "City": ["Nowhere"]})
    make_writer(temp_dir).execute(make_state(df))
    lines = read_lines(temp_dir)
    assert len(lines) == 2  # header + 1 row
    assert "Replacement" in lines[1]


def test_execute_append_mode_first_run_writes_normally(temp_dir):
    make_writer(temp_dir, write_mode=WriteMode.APPEND).execute(make_state())
    lines = read_lines(temp_dir)
    assert len(lines) == 4  # header + 3 rows


def test_execute_append_mode_unions_with_prior_partition(temp_dir):
    prior_dir = os.path.join(temp_dir, "testjob", "testtable", "2024-11-14")
    os.makedirs(prior_dir)
    with open(os.path.join(prior_dir, "output.csv"), "w") as f:
        f.write("Id,Name,City,etl_effective_date\n1,Alice,New York,2024-11-14\n2,Bob,London,2024-11-14\n")

    new_df = pd.DataFrame({"Id": [3], "Name": ["Charlie"], "City": ["Paris"]})
    make_writer(temp_dir, write_mode=WriteMode.APPEND).execute(make_state(new_df))
    lines = read_lines(temp_dir)
    assert len(lines) == 4  # header + 2 prior + 1 new
    for line in lines[1:]:
        assert line.endswith(TEST_DATE_STR)


def test_execute_utf8_no_bom(temp_dir):
    make_writer(temp_dir).execute(make_state())
    with open(output_path(temp_dir), "rb") as f:
        raw = f.read()
    assert not (len(raw) >= 3 and raw[0] == 0xEF and raw[1] == 0xBB and raw[2] == 0xBF)


def test_execute_lf_line_endings(temp_dir):
    make_writer(temp_dir).execute(make_state())
    with open(output_path(temp_dir), "rb") as f:
        content = f.read().decode("utf-8")
    assert "\r\n" not in content
    assert "\n" in content


def test_execute_crlf_line_endings(temp_dir):
    make_writer(temp_dir, line_ending="\r\n").execute(make_state())
    with open(output_path(temp_dir), "rb") as f:
        content = f.read().decode("utf-8")
    assert "\r\n" in content
    stripped = content.replace("\r\n", "")
    assert "\n" not in stripped


def test_execute_default_line_ending_is_lf(temp_dir):
    make_writer(temp_dir).execute(make_state())
    with open(output_path(temp_dir), "rb") as f:
        content = f.read().decode("utf-8")
    assert "\r\n" not in content
    assert "\n" in content


def test_execute_creates_directory_structure(temp_dir):
    make_writer(temp_dir).execute(make_state())
    assert os.path.exists(output_path(temp_dir))


def test_execute_missing_dataframe_throws(temp_dir):
    writer = CsvFileWriter("nonexistent", temp_dir, "testjob", "testtable", "output.csv")
    state = {ETL_EFFECTIVE_DATE_KEY: TEST_DATE}
    with pytest.raises(KeyError):
        writer.execute(state)


def test_execute_missing_effective_date_throws(temp_dir):
    state = {"data": make_test_frame()}
    with pytest.raises(RuntimeError):
        make_writer(temp_dir).execute(state)


def test_execute_returns_shared_state_unchanged(temp_dir):
    state = make_state()
    state["other"] = "keep me"
    result = make_writer(temp_dir).execute(state)
    assert result is state
    assert result["other"] == "keep me"


def test_execute_append_mode_with_trailer_strips_trailer_from_prior(temp_dir):
    prior_dir = os.path.join(temp_dir, "testjob", "testtable", "2024-11-14")
    os.makedirs(prior_dir)
    with open(os.path.join(prior_dir, "output.csv"), "w") as f:
        f.write("Id,Name,City,etl_effective_date\n1,Alice,New York,2024-11-14\n2,Bob,London,2024-11-14\nTRAILER|2|2024-11-14\n")

    new_df = pd.DataFrame({"Id": [3], "Name": ["Charlie"], "City": ["Paris"]})
    make_writer(temp_dir, write_mode=WriteMode.APPEND, trailer_format="TRAILER|{row_count}|{date}").execute(
        make_state(new_df)
    )
    lines = read_lines(temp_dir)
    assert len(lines) == 5  # header + 2 prior + 1 new + trailer
    assert lines[4].startswith("TRAILER|")
    assert lines[4] == f"TRAILER|3|{TEST_DATE_STR}"
    for line in lines[1:4]:
        assert "TRAILER" not in line


def test_execute_append_mode_without_trailer_does_not_strip_last_row(temp_dir):
    prior_dir = os.path.join(temp_dir, "testjob", "testtable", "2024-11-14")
    os.makedirs(prior_dir)
    with open(os.path.join(prior_dir, "output.csv"), "w") as f:
        f.write("Id,Name,City,etl_effective_date\n1,Alice,New York,2024-11-14\n2,Bob,London,2024-11-14\n")

    new_df = pd.DataFrame({"Id": [3], "Name": ["Charlie"], "City": ["Paris"]})
    make_writer(temp_dir, write_mode=WriteMode.APPEND).execute(make_state(new_df))
    lines = read_lines(temp_dir)
    assert len(lines) == 4  # header + 2 prior + 1 new


def test_execute_append_mode_rerun_ignores_future_partitions(temp_dir):
    """Re-running Oct 1 when Oct 31 already exists must NOT pull in Oct 31 data."""
    table_base = os.path.join(temp_dir, "testjob", "testtable")

    # Simulate a previous full run: partitions for Oct 1 and Oct 31
    for day, name in [("2024-10-01", "Alice"), ("2024-10-31", "Zara")]:
        d = os.path.join(table_base, day)
        os.makedirs(d)
        with open(os.path.join(d, "output.csv"), "w") as f:
            f.write(f"Id,Name,City,etl_effective_date\n1,{name},London,{day}\n")

    # Re-run Oct 1 with corrected data
    rerun_date = date(2024, 10, 1)
    new_df = pd.DataFrame({"Id": [99], "Name": ["Fixed"], "City": ["Paris"]})
    state = {"data": new_df, ETL_EFFECTIVE_DATE_KEY: rerun_date}
    writer = CsvFileWriter(
        source="data", output_directory=temp_dir, job_dir_name="testjob",
        output_table_dir_name="testtable", file_name="output.csv",
        write_mode=WriteMode.APPEND,
    )
    writer.execute(state)

    out = os.path.join(table_base, "2024-10-01", "output.csv")
    with open(out) as f:
        lines = f.read().splitlines()

    # Should contain only the new row — no prior partition exists before Oct 1
    assert len(lines) == 2  # header + 1 row
    assert "Fixed" in lines[1]
    assert "Zara" not in lines[1]
