from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession

from common import (
    CONFORMED_DIR,
    RAW_CSV_DIR,
    build_spark_session,
    conform_to_schema,
    drive_telemetry_schema,
    union_all,
    with_date_partitions,
    write_partitioned_parquet,
)


def list_quarter_directories(spark: SparkSession, raw_dir: str) -> list[str]:
    raw_path = spark._jvm.org.apache.hadoop.fs.Path(raw_dir)
    file_system = raw_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return [
        file_status.getPath().toString()
        for file_status in file_system.listStatus(raw_path)
        if file_status.isDirectory()
    ]


def read_quarter(spark: SparkSession, quarter_dir: str) -> DataFrame:
    # Backblaze adds and reorders columns between quarters, and Spark applies the first
    # file's header to every file in a read, so each quarter is conformed before the union.
    raw_quarter = spark.read.option("header", True).csv(f"{quarter_dir}/*.csv")
    return conform_to_schema(raw_quarter, drive_telemetry_schema())


def main(quarter_names: list[str]) -> None:
    spark = build_spark_session("backblaze-ingest")

    quarter_dirs = (
        [f"{RAW_CSV_DIR}/{quarter_name}" for quarter_name in quarter_names]
        if quarter_names
        else list_quarter_directories(spark, RAW_CSV_DIR)
    )

    telemetry = union_all([read_quarter(spark, quarter_dir) for quarter_dir in quarter_dirs])
    write_partitioned_parquet(with_date_partitions(telemetry), CONFORMED_DIR)

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
