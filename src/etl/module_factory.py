"""Reads a module's JSON config dict and instantiates the appropriate Module."""

from __future__ import annotations

from datetime import date

from etl.modules.base import Module, WriteMode  # noqa: F401 — re-export WriteMode


def create(config: dict) -> Module:
    module_type = config.get("type")
    if not module_type:
        raise ValueError("Module config is missing the required 'type' field.")

    match module_type:
        case "DataSourcing":
            return _create_data_sourcing(config)
        case "Transformation":
            return _create_transformation(config)
        case "DataFrameWriter":
            return _create_dataframe_writer(config)
        case "External":
            return _create_external(config)
        case "ParquetFileWriter":
            return _create_parquet_file_writer(config)
        case "CsvFileWriter":
            return _create_csv_file_writer(config)
        case _:
            raise ValueError(f"Unknown module type: '{module_type}'.")


def _create_data_sourcing(c: dict) -> Module:
    from etl.modules.data_sourcing import DataSourcing

    min_date = None
    if "minEffectiveDate" in c and c["minEffectiveDate"]:
        min_date = date.fromisoformat(c["minEffectiveDate"])

    max_date = None
    if "maxEffectiveDate" in c and c["maxEffectiveDate"]:
        max_date = date.fromisoformat(c["maxEffectiveDate"])

    return DataSourcing(
        result_name=c["resultName"],
        schema=c["schema"],
        table=c["table"],
        columns=c["columns"],
        min_effective_date=min_date,
        max_effective_date=max_date,
        additional_filter=c.get("additionalFilter", ""),
        lookback_days=c.get("lookbackDays"),
        most_recent_prior=c.get("mostRecentPrior", False),
        most_recent=c.get("mostRecent", False),
    )


def _create_transformation(c: dict) -> Module:
    from etl.modules.transformation import Transformation

    return Transformation(
        result_name=c["resultName"],
        sql=c["sql"],
    )


def _create_dataframe_writer(c: dict) -> Module:
    from etl.modules.dataframe_writer import DataFrameWriter

    return DataFrameWriter(
        source=c["source"],
        target_table=c["targetTable"],
        write_mode=WriteMode(c["writeMode"]),
        target_schema=c.get("targetSchema", "curated"),
    )


def _create_external(c: dict) -> Module:
    from etl.modules.external import External

    return External(
        assembly_path=c["assemblyPath"],
        type_name=c["typeName"],
    )


def _create_parquet_file_writer(c: dict) -> Module:
    from etl.modules.parquet_file_writer import ParquetFileWriter

    return ParquetFileWriter(
        source=c["source"],
        output_directory=c["outputDirectory"],
        job_dir_name=c["jobDirName"],
        output_table_dir_name=c["outputTableDirName"],
        file_name=c["fileName"],
        num_parts=c.get("numParts", 1),
        write_mode=WriteMode(c["writeMode"]),
    )


def _create_csv_file_writer(c: dict) -> Module:
    from etl.modules.csv_file_writer import CsvFileWriter

    line_ending_raw = c.get("lineEnding", "LF")
    line_ending = "\r\n" if line_ending_raw == "CRLF" else "\n"

    return CsvFileWriter(
        source=c["source"],
        output_directory=c["outputDirectory"],
        job_dir_name=c["jobDirName"],
        output_table_dir_name=c["outputTableDirName"],
        file_name=c["fileName"],
        include_header=c.get("includeHeader", True),
        trailer_format=c.get("trailerFormat"),
        write_mode=WriteMode(c["writeMode"]),
        line_ending=line_ending,
    )
