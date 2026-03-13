"""Tests for module_factory.py — ported from ModuleFactoryTests.cs (23 tests)."""

import pytest

from etl import module_factory
from etl.modules.data_sourcing import DataSourcing
from etl.modules.transformation import Transformation
from etl.modules.dataframe_writer import DataFrameWriter
from etl.modules.external import External
from etl.modules.parquet_file_writer import ParquetFileWriter
from etl.modules.csv_file_writer import CsvFileWriter


def test_create_data_sourcing_returns_correct_type():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "minEffectiveDate": "2024-01-01",
        "maxEffectiveDate": "2024-01-31",
    }
    assert isinstance(module_factory.create(config), DataSourcing)


def test_create_data_sourcing_with_optional_filter_returns_correct_type():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id"],
        "minEffectiveDate": "2024-01-01",
        "maxEffectiveDate": "2024-01-31",
        "additionalFilter": "id > 100",
    }
    assert isinstance(module_factory.create(config), DataSourcing)


def test_create_transformation_returns_correct_type():
    config = {"type": "Transformation", "resultName": "result", "sql": "SELECT 1"}
    assert isinstance(module_factory.create(config), Transformation)


def test_create_dataframe_writer_overwrite_returns_correct_type():
    config = {
        "type": "DataFrameWriter",
        "source": "result",
        "targetTable": "output",
        "writeMode": "Overwrite",
    }
    assert isinstance(module_factory.create(config), DataFrameWriter)


def test_create_dataframe_writer_append_returns_correct_type():
    config = {
        "type": "DataFrameWriter",
        "source": "result",
        "targetTable": "output",
        "writeMode": "Append",
    }
    assert isinstance(module_factory.create(config), DataFrameWriter)


def test_create_dataframe_writer_with_target_schema_returns_correct_type():
    config = {
        "type": "DataFrameWriter",
        "source": "result",
        "targetTable": "output",
        "writeMode": "Overwrite",
        "targetSchema": "double_secret_curated",
    }
    assert isinstance(module_factory.create(config), DataFrameWriter)


def test_create_external_returns_correct_type():
    config = {
        "type": "External",
        "assemblyPath": "/some/path.dll",
        "typeName": "MyNamespace.MyClass",
    }
    assert isinstance(module_factory.create(config), External)


def test_create_parquet_file_writer_returns_correct_type():
    config = {
        "type": "ParquetFileWriter",
        "source": "output",
        "outputDirectory": "Output/poc4",
        "jobDirName": "test_job",
        "outputTableDirName": "test_table",
        "fileName": "test_output",
        "numParts": 2,
        "writeMode": "Overwrite",
    }
    assert isinstance(module_factory.create(config), ParquetFileWriter)


def test_create_parquet_file_writer_default_num_parts_returns_correct_type():
    config = {
        "type": "ParquetFileWriter",
        "source": "output",
        "outputDirectory": "Output/poc4",
        "jobDirName": "test_job",
        "outputTableDirName": "test_table",
        "fileName": "test_output",
        "writeMode": "Overwrite",
    }
    assert isinstance(module_factory.create(config), ParquetFileWriter)


def test_create_csv_file_writer_returns_correct_type():
    config = {
        "type": "CsvFileWriter",
        "source": "output",
        "outputDirectory": "Output/poc4",
        "jobDirName": "test_job",
        "outputTableDirName": "test_table",
        "fileName": "test.csv",
        "writeMode": "Overwrite",
    }
    assert isinstance(module_factory.create(config), CsvFileWriter)


def test_create_csv_file_writer_with_trailer_returns_correct_type():
    config = {
        "type": "CsvFileWriter",
        "source": "output",
        "outputDirectory": "Output/poc4",
        "jobDirName": "test_job",
        "outputTableDirName": "test_table",
        "fileName": "test.csv",
        "trailerFormat": "TRAILER|{row_count}",
        "writeMode": "Overwrite",
    }
    assert isinstance(module_factory.create(config), CsvFileWriter)


def test_create_csv_file_writer_with_crlf_line_ending_returns_correct_type():
    config = {
        "type": "CsvFileWriter",
        "source": "output",
        "outputDirectory": "Output/poc4",
        "jobDirName": "test_job",
        "outputTableDirName": "test_table",
        "fileName": "test.csv",
        "writeMode": "Overwrite",
        "lineEnding": "CRLF",
    }
    assert isinstance(module_factory.create(config), CsvFileWriter)


def test_create_data_sourcing_with_lookback_days_returns_correct_type():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "lookbackDays": 3,
    }
    assert isinstance(module_factory.create(config), DataSourcing)


def test_create_data_sourcing_with_most_recent_prior_returns_correct_type():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "mostRecentPrior": True,
    }
    assert isinstance(module_factory.create(config), DataSourcing)


def test_create_data_sourcing_lookback_and_most_recent_prior_throws():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "lookbackDays": 3,
        "mostRecentPrior": True,
    }
    with pytest.raises(ValueError):
        module_factory.create(config)


def test_create_data_sourcing_lookback_and_static_dates_throws():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "lookbackDays": 3,
        "minEffectiveDate": "2024-01-01",
    }
    with pytest.raises(ValueError):
        module_factory.create(config)


def test_create_data_sourcing_with_most_recent_returns_correct_type():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "mostRecent": True,
    }
    assert isinstance(module_factory.create(config), DataSourcing)


def test_create_data_sourcing_most_recent_and_most_recent_prior_throws():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "mostRecent": True,
        "mostRecentPrior": True,
    }
    with pytest.raises(ValueError):
        module_factory.create(config)


def test_create_data_sourcing_most_recent_and_lookback_throws():
    config = {
        "type": "DataSourcing",
        "resultName": "customers",
        "schema": "datalake",
        "table": "customers",
        "columns": ["id", "first_name"],
        "mostRecent": True,
        "lookbackDays": 3,
    }
    with pytest.raises(ValueError):
        module_factory.create(config)


def test_create_csv_file_writer_missing_output_table_dir_name_throws():
    config = {
        "type": "CsvFileWriter",
        "source": "output",
        "outputDirectory": "Output/poc4",
        "jobDirName": "test_job",
        "fileName": "test.csv",
        "writeMode": "Overwrite",
    }
    with pytest.raises(KeyError):
        module_factory.create(config)


def test_create_parquet_file_writer_missing_output_table_dir_name_throws():
    config = {
        "type": "ParquetFileWriter",
        "source": "output",
        "outputDirectory": "Output/poc4",
        "jobDirName": "test_job",
        "fileName": "test_output",
        "writeMode": "Overwrite",
    }
    with pytest.raises(KeyError):
        module_factory.create(config)


def test_create_unknown_type_throws():
    config = {"type": "UnknownModule"}
    with pytest.raises(ValueError):
        module_factory.create(config)


def test_create_missing_type_field_throws():
    config = {"resultName": "foo"}
    with pytest.raises(ValueError):
        module_factory.create(config)
