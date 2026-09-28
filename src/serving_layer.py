from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DateType, DoubleType, IntegerType, LongType, StringType
from pyspark.sql.window import Window

from common import (
    FEATURE_DIR,
    SMART_ATTRIBUTES,
    SMART_ERROR_COUNTER_IDS,
    build_spark_session,
    reported_column,
    smart_raw_column,
    trend_column,
    union_all,
    zscore_column,
)

# Written through Spark's JDBC source so the load stays distributed across executors.
# Needs the driver on the classpath: spark-submit --packages com.clickhouse:clickhouse-jdbc:0.6.3:all
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/backblaze")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

# Sort key for each table's MergeTree. Key columns are never null; every other column is
# created Nullable because some summaries (e.g. mean age at failure) are legitimately null
REPORTING_TABLES = {
    "daily_fleet_health": "date",
    "model_failure_summary": "model",
    "smart_failure_signal": "(attribute_id, cohort)",
    "drive_risk_watchlist": "(risk_score, serial_number)",
}

# Per-model z-score above which an error counter reading is treated as anomalous
RISK_ZSCORE_THRESHOLD = 2.0

TEMPERATURE_COLUMN = smart_raw_column(194)


def summarize_daily_fleet_health(features: DataFrame) -> DataFrame:
    return (
        features.groupBy("date")
        .agg(
            F.countDistinct("serial_number").alias("drive_count"),
            F.sum("failure").alias("failure_count"),
            F.sum("failure_label").alias("at_risk_count"),
            F.avg("age_days").alias("mean_age_days"),
            F.avg(TEMPERATURE_COLUMN).alias("mean_temperature_celsius"),
        )
        .withColumn("failure_rate_pct", F.col("failure_count") / F.col("drive_count") * 100)
        .withColumn("at_risk_rate_pct", F.col("at_risk_count") / F.col("drive_count") * 100)
        .orderBy("date")
    )


def summarize_model_failures(features: DataFrame) -> DataFrame:
    # Annualized failure rate (failures per drive-year) is the metric Backblaze reports, so
    # it can be checked against their published numbers for the same quarter
    return (
        features.groupBy("model")
        .agg(
            F.countDistinct("serial_number").alias("drive_count"),
            F.count("*").alias("drive_days"),
            F.sum("failure").alias("failure_count"),
            (F.avg("capacity_bytes") / F.lit(1e12)).alias("mean_capacity_tb"),
            F.avg(F.when(F.col("failure") == 1, F.col("age_days"))).alias("mean_age_at_failure_days"),
            F.avg(TEMPERATURE_COLUMN).alias("mean_temperature_celsius"),
        )
        .withColumn(
            "annualized_failure_rate_pct",
            F.col("failure_count") / (F.col("drive_days") / F.lit(365.0)) * 100,
        )
        .orderBy(F.col("annualized_failure_rate_pct").desc(), "model")
    )


def _attribute_long_frame(features: DataFrame, attribute_id: int) -> DataFrame:
    raw_column = smart_raw_column(attribute_id)
    return features.select(
        F.lit(attribute_id).alias("attribute_id"),
        F.lit(SMART_ATTRIBUTES[attribute_id]).alias("attribute_name"),
        F.when(F.col("failure_label") == 1, F.lit("pre_failure")).otherwise(F.lit("healthy")).alias("cohort"),
        F.col(zscore_column(raw_column)).alias("zscore"),
        F.col(reported_column(raw_column)).cast("double").alias("reported"),
        F.col(trend_column(raw_column, 30)).alias("trend_30d"),
    )


def summarize_smart_failure_signal(features: DataFrame) -> DataFrame:
    # Pivoting attributes into rows lets a dashboard compare them on one axis. Contrasting the
    # pre-failure cohort against everything else shows which attributes actually separate the two.
    long_frame = union_all([_attribute_long_frame(features, attribute_id) for attribute_id in SMART_ATTRIBUTES])
    return (
        long_frame.groupBy("attribute_id", "attribute_name", "cohort")
        .agg(
            F.count("*").alias("row_count"),
            F.avg("reported").alias("reported_share"),
            F.avg("zscore").alias("mean_zscore"),
            F.percentile_approx("zscore", 0.5).alias("median_zscore"),
            F.percentile_approx("zscore", 0.95).alias("p95_zscore"),
            F.avg("trend_30d").alias("mean_trend_30d"),
        )
        .orderBy("attribute_id", "cohort")
    )


def latest_observation_per_drive(features: DataFrame) -> DataFrame:
    recency = Window.partitionBy("serial_number").orderBy(F.col("date").desc())
    return (
        features.withColumn("_recency_rank", F.row_number().over(recency))
        .filter(F.col("_recency_rank") == 1)
        .drop("_recency_rank")
    )


def build_drive_risk_watchlist(features: DataFrame) -> DataFrame:
    error_counters = [smart_raw_column(attribute_id) for attribute_id in SMART_ERROR_COUNTER_IDS]

    anomalous_flags = [
        F.when(F.col(zscore_column(raw_column)) > RISK_ZSCORE_THRESHOLD, 1).otherwise(0)
        for raw_column in error_counters
    ]
    rising_flags = [
        F.when(
            (F.col(raw_column) > 0) & (F.col(trend_column(raw_column, 7)) > F.col(trend_column(raw_column, 30))),
            1,
        ).otherwise(0)
        for raw_column in error_counters
    ]

    # A counter is anomalous when its z-score clears the threshold and rising when its 7-day trend
    # sits above its 30-day trend with a nonzero reading. Drives that already failed have nothing left to watch.
    latest = latest_observation_per_drive(features.filter(F.col("failure_date").isNull()))
    return (
        latest.withColumn("anomalous_counter_count", sum(anomalous_flags, F.lit(0)))
        .withColumn("rising_counter_count", sum(rising_flags, F.lit(0)))
        .withColumn("risk_score", F.col("anomalous_counter_count") + F.col("rising_counter_count"))
        .filter(F.col("risk_score") > 0)
        .select(
            "serial_number",
            "model",
            F.col("date").alias("as_of_date"),
            "age_days",
            F.col(TEMPERATURE_COLUMN).alias("temperature_celsius"),
            "risk_score",
            "anomalous_counter_count",
            "rising_counter_count",
            *[F.col(raw_column) for raw_column in error_counters],
            *[F.col(zscore_column(raw_column)) for raw_column in error_counters],
        )
        .orderBy(F.col("risk_score").desc(), "serial_number")
    )


def build_reporting_tables(features: DataFrame) -> dict[str, DataFrame]:
    return {
        "daily_fleet_health": summarize_daily_fleet_health(features),
        "model_failure_summary": summarize_model_failures(features),
        "smart_failure_signal": summarize_smart_failure_signal(features),
        "drive_risk_watchlist": build_drive_risk_watchlist(features),
    }


def clickhouse_type(data_type: DataType, nullable: bool) -> str:
    type_names = {
        DateType: "Date",
        StringType: "String",
        IntegerType: "Int32",
        LongType: "Int64",
        DoubleType: "Float64",
    }
    type_name = type_names[type(data_type)]
    return f"Nullable({type_name})" if nullable else type_name


def create_table_statement(frame: DataFrame, table_name: str) -> str:
    order_by = REPORTING_TABLES[table_name]
    key_columns = set(order_by.strip("()").replace(" ", "").split(","))
    column_definitions = ", ".join(
        f"{field.name} {clickhouse_type(field.dataType, field.name not in key_columns)}"
        for field in frame.schema.fields
    )
    return f"CREATE TABLE {table_name} ({column_definitions}) ENGINE = MergeTree ORDER BY {order_by}"


def execute_clickhouse_statement(spark: SparkSession, statement: str) -> None:
    # Spark's JDBC writer cannot express Nullable or ENGINE, so DDL goes through the driver's own JDBC
    # connection. Jars passed with --jars are invisible to java.sql.DriverManager, so the driver class is
    # loaded through Spark's classloader and used directly.
    jvm = spark._jvm
    driver = jvm.org.apache.spark.util.Utils.classForName(CLICKHOUSE_DRIVER, True, False).newInstance()
    properties = jvm.java.util.Properties()
    properties.setProperty("user", CLICKHOUSE_USER)
    properties.setProperty("password", CLICKHOUSE_PASSWORD)
    connection = driver.connect(CLICKHOUSE_URL, properties)
    try:
        connection.createStatement().execute(statement)
    finally:
        connection.close()


def write_to_clickhouse(spark: SparkSession, frame: DataFrame, table_name: str) -> None:
    # Spark has no ClickHouse dialect, so its overwrite mode would recreate the table without
    # ORDER BY or Nullable columns. Rebuild the table ourselves from the frame's schema, then append.
    execute_clickhouse_statement(spark, f"DROP TABLE IF EXISTS {table_name}")
    execute_clickhouse_statement(spark, create_table_statement(frame, table_name))
    (
        frame.write.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("dbtable", table_name)
        .option("batchsize", 10000)
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )


def main() -> None:
    spark = build_spark_session("backblaze-serving")

    # All four summaries scan the same feature table, so it is cached after the first read
    features = spark.read.parquet(FEATURE_DIR).cache()

    for table_name, table in build_reporting_tables(features).items():
        print(f"Loading {table_name} into ClickHouse")
        write_to_clickhouse(spark, table, table_name)

    features.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
