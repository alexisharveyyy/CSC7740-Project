from __future__ import annotations

import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

HDFS_ROOT = os.environ.get("BACKBLAZE_HDFS_ROOT", "hdfs://namenode:9000/backblaze")
RAW_CSV_DIR = f"{HDFS_ROOT}/raw"
CONFORMED_DIR = f"{HDFS_ROOT}/conformed"
CLEANED_DIR = f"{HDFS_ROOT}/cleaned"
FEATURE_DIR = f"{HDFS_ROOT}/features"
MODEL_DIR = f"{HDFS_ROOT}/models/failure_classifier"
MODEL_EVALUATION_DIR = f"{HDFS_ROOT}/model_evaluation"
PREDICTIONS_DIR = f"{HDFS_ROOT}/predictions"

DRIVE_DAY_KEY = ["serial_number", "date"]
PARTITION_COLUMNS = ["year", "month"]

# SMART attributes with documented predictive value for drive failure
SMART_ATTRIBUTES = {
    5: "reallocated_sector_count",
    9: "power_on_hours",
    187: "reported_uncorrectable_errors",
    188: "command_timeout",
    194: "temperature_celsius",
    197: "current_pending_sector_count",
    198: "offline_uncorrectable_count",
}

# Cumulative error counters never decrease, so their growth over time signals degradation
SMART_ERROR_COUNTER_IDS = [5, 187, 188, 197, 198]


def smart_raw_column(attribute_id: int) -> str:
    return f"smart_{attribute_id}_raw"


def smart_normalized_column(attribute_id: int) -> str:
    return f"smart_{attribute_id}_normalized"


def zscore_column(raw_column: str) -> str:
    return f"{raw_column}_model_zscore"


def reported_column(raw_column: str) -> str:
    return f"{raw_column}_reported"


def trend_column(raw_column: str, days: int) -> str:
    return f"{raw_column}_trend_{days}d"


def daily_delta_column(raw_column: str) -> str:
    return f"{raw_column}_daily_delta"


def weekly_increase_column(raw_column: str) -> str:
    return f"{raw_column}_weekly_increase"


SMART_RAW_COLUMNS = [smart_raw_column(attribute_id) for attribute_id in SMART_ATTRIBUTES]
SMART_NORMALIZED_COLUMNS = [smart_normalized_column(attribute_id) for attribute_id in SMART_ATTRIBUTES]
SMART_ERROR_COUNTER_COLUMNS = [smart_raw_column(attribute_id) for attribute_id in SMART_ERROR_COUNTER_IDS]
SMART_REPORTED_COLUMNS = [reported_column(raw_column) for raw_column in SMART_RAW_COLUMNS]


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Re-running one quarter replaces only its own year/month partitions
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def drive_telemetry_schema() -> StructType:
    identity_fields = [
        StructField("date", DateType(), nullable=False),
        StructField("serial_number", StringType(), nullable=False),
        StructField("model", StringType(), nullable=False),
        StructField("capacity_bytes", LongType(), nullable=True),
        StructField("failure", IntegerType(), nullable=False),
    ]
    smart_fields = [
        StructField(column_name, LongType(), nullable=True)
        for column_name in SMART_RAW_COLUMNS + SMART_NORMALIZED_COLUMNS
    ]
    return StructType(identity_fields + smart_fields)


def conform_to_schema(raw_frame: DataFrame, target_schema: StructType) -> DataFrame:
    available_columns = set(raw_frame.columns)
    conformed_columns = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        if field.name in available_columns
        else F.lit(None).cast(field.dataType).alias(field.name)
        for field in target_schema.fields
    ]
    return raw_frame.select(conformed_columns)


def union_all(frames: list[DataFrame]) -> DataFrame:
    return reduce(DataFrame.unionByName, frames)


def with_date_partitions(frame: DataFrame) -> DataFrame:
    return frame.withColumn("year", F.year("date")).withColumn("month", F.month("date"))


def write_partitioned_parquet(frame: DataFrame, output_dir: str) -> None:
    frame.write.mode("overwrite").partitionBy(*PARTITION_COLUMNS).parquet(output_dir)