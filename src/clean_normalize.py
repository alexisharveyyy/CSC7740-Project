from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import (
    CLEANED_DIR,
    CONFORMED_DIR,
    DRIVE_DAY_KEY,
    SMART_RAW_COLUMNS,
    build_spark_session,
    reported_column,
    write_partitioned_parquet,
    zscore_column,
)


def normalize_model_names(telemetry: DataFrame) -> DataFrame:
    return telemetry.withColumn("model", F.upper(F.trim("model")))


def drop_invalid_rows(telemetry: DataFrame) -> DataFrame:
    return (
        telemetry.na.drop(subset=["date", "serial_number", "model", "failure"])
        # Backblaze writes -1 when a drive's capacity could not be read
        .filter(F.col("capacity_bytes") > 0)
        .filter(F.col("failure").isin(0, 1))
    )


def deduplicate_drive_days(telemetry: DataFrame) -> DataFrame:
    return telemetry.dropDuplicates(DRIVE_DAY_KEY)


def add_per_model_zscores(telemetry: DataFrame) -> DataFrame:
    # Raw SMART counters use vendor-specific scales, so values are only comparable within a model
    mean_columns = {raw_column: f"{raw_column}_model_mean" for raw_column in SMART_RAW_COLUMNS}
    stddev_columns = {raw_column: f"{raw_column}_model_stddev" for raw_column in SMART_RAW_COLUMNS}

    model_statistics = telemetry.groupBy("model").agg(
        *[F.mean(raw_column).alias(mean_columns[raw_column]) for raw_column in SMART_RAW_COLUMNS],
        *[F.stddev(raw_column).alias(stddev_columns[raw_column]) for raw_column in SMART_RAW_COLUMNS],
    )

    # A few hundred models at most, so broadcasting avoids shuffling the full telemetry table
    with_statistics = telemetry.join(F.broadcast(model_statistics), on="model", how="left")

    zscore_expressions = [
        # Many drive models never report a given attribute. Leaving those z-scores null keeps them
        # distinguishable from a genuinely average reading, which 0.0 would silently impersonate.
        F.when(F.col(raw_column).isNull(), F.lit(None).cast("double"))
        .when(
            F.col(stddev_columns[raw_column]) > 0,
            (F.col(raw_column) - F.col(mean_columns[raw_column])) / F.col(stddev_columns[raw_column]),
        )
        .otherwise(F.lit(0.0))
        .alias(zscore_column(raw_column))
        for raw_column in SMART_RAW_COLUMNS
    ]
    reported_expressions = [
        F.col(raw_column).isNotNull().alias(reported_column(raw_column))
        for raw_column in SMART_RAW_COLUMNS
    ]
    return with_statistics.select(*telemetry.columns, *zscore_expressions, *reported_expressions)


def main() -> None:
    spark = build_spark_session("backblaze-clean")

    cleaned = (
        spark.read.parquet(CONFORMED_DIR)
        .transform(normalize_model_names)
        .transform(drop_invalid_rows)
        .transform(deduplicate_drive_days)
        .transform(add_per_model_zscores)
    )
    write_partitioned_parquet(cleaned, CLEANED_DIR)

    spark.stop()


if __name__ == "__main__":
    main()