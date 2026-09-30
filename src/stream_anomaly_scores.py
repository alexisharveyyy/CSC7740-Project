from __future__ import annotations

import sys

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from clickhouse import append_to_table, path_exists
from common import (
    CLEANED_DIR,
    FEATURE_DIR,
    HDFS_ROOT,
    MODEL_DIR,
    SMART_ERROR_COUNTER_IDS,
    SMART_RAW_COLUMNS,
    build_spark_session,
    conform_to_schema,
    drive_telemetry_schema,
    reported_column,
    smart_raw_column,
    trend_column,
    zscore_column,
)
from serving_layer import RISK_ZSCORE_THRESHOLD
from train_model import feature_columns

STREAM_INCOMING_DIR = f"{HDFS_ROOT}/stream/incoming"
STREAM_CHECKPOINT_DIR = f"{HDFS_ROOT}/stream/checkpoints"
SCORES_TABLE = "streaming_anomaly_scores"
SCORES_ORDER_BY = "(date, serial_number)"
TREND_WINDOWS_DAYS = (7, 30)
ERROR_COUNTER_COLUMNS = [smart_raw_column(attribute_id) for attribute_id in SMART_ERROR_COUNTER_IDS]


def model_statistics(cleaned: DataFrame) -> DataFrame:
    # Raw SMART counters use vendor-specific scales, so incoming readings are scored against the
    # per-model mean and stddev of the cleaned history rather than against the batch they arrive in
    return cleaned.groupBy("model").agg(
        *[F.mean(raw_column).alias(f"{raw_column}_model_mean") for raw_column in SMART_RAW_COLUMNS],
        *[F.stddev(raw_column).alias(f"{raw_column}_model_stddev") for raw_column in SMART_RAW_COLUMNS],
    )


def drive_context(features: DataFrame) -> DataFrame:
    # The classifier needs drive age and rolling trends, which a single incoming row cannot provide.
    # Each drive's latest feature row supplies them; for a drive never seen before they are null.
    recency = Window.partitionBy("serial_number").orderBy(F.col("date").desc())
    trend_columns = [
        trend_column(raw_column, days) for raw_column in SMART_RAW_COLUMNS for days in TREND_WINDOWS_DAYS
    ]
    return (
        features.withColumn("_recency_rank", F.row_number().over(recency))
        .filter(F.col("_recency_rank") == 1)
        .select("serial_number", "first_seen", *trend_columns)
    )


def add_zscores(telemetry: DataFrame, statistics: DataFrame) -> DataFrame:
    with_statistics = telemetry.join(F.broadcast(statistics), on="model", how="left")
    zscore_expressions = [
        F.when(F.col(raw_column).isNull(), F.lit(None).cast("double"))
        .when(
            F.col(f"{raw_column}_model_stddev") > 0,
            (F.col(raw_column) - F.col(f"{raw_column}_model_mean")) / F.col(f"{raw_column}_model_stddev"),
        )
        .otherwise(F.lit(0.0))
        .alias(zscore_column(raw_column))
        for raw_column in SMART_RAW_COLUMNS
    ]
    reported_expressions = [
        F.col(raw_column).isNotNull().alias(reported_column(raw_column)) for raw_column in SMART_RAW_COLUMNS
    ]
    return with_statistics.select(*telemetry.columns, *zscore_expressions, *reported_expressions)


def add_rule_scores(telemetry: DataFrame) -> DataFrame:
    # Same rule as the batch watchlist, applied per reading
    anomalous_flags = [
        F.when(F.col(zscore_column(raw_column)) > RISK_ZSCORE_THRESHOLD, 1).otherwise(0)
        for raw_column in ERROR_COUNTER_COLUMNS
    ]
    return telemetry.withColumn("anomalous_counter_count", sum(anomalous_flags, F.lit(0))).withColumn(
        "max_error_zscore",
        F.greatest(*[F.coalesce(F.col(zscore_column(raw_column)), F.lit(0.0)) for raw_column in ERROR_COUNTER_COLUMNS]),
    )


def add_model_probability(telemetry: DataFrame, context: DataFrame, model: PipelineModel | None) -> DataFrame:
    with_context = telemetry.join(context, on="serial_number", how="left").withColumn(
        "age_days", F.coalesce(F.datediff("date", "first_seen"), F.lit(0))
    )
    if model is None:
        return with_context.withColumn("failure_probability", F.lit(None).cast("double"))

    # Same null handling as train_model.prepare_model_rows, so the model sees inputs it was trained on
    model_inputs = with_context.select(
        *with_context.columns,
        *[
            F.coalesce(F.col(column).cast("double"), F.lit(0.0)).alias(f"_input_{column}")
            for column in feature_columns()
        ],
    )
    renamed = model_inputs.drop(*feature_columns())
    for column in feature_columns():
        renamed = renamed.withColumnRenamed(f"_input_{column}", column)
    scored = model.transform(renamed).withColumn("failure_probability", vector_to_array("probability")[1])
    return scored.drop("raw_features", "features", "rawPrediction", "probability", "prediction")


def score_batch(telemetry: DataFrame, statistics: DataFrame, context: DataFrame, model: PipelineModel | None) -> DataFrame:
    scored = telemetry.transform(lambda frame: add_zscores(frame, statistics)).transform(add_rule_scores)
    scored = add_model_probability(scored, context, model)
    # Explicit casts keep the table schema identical whether or not a model was available
    return scored.select(
        "date",
        "serial_number",
        "model",
        "failure",
        F.col("age_days").cast("int").alias("age_days"),
        "anomalous_counter_count",
        "max_error_zscore",
        "failure_probability",
        *[F.col(raw_column).cast("long").alias(raw_column) for raw_column in ERROR_COUNTER_COLUMNS],
        *[F.col(zscore_column(raw_column)) for raw_column in ERROR_COUNTER_COLUMNS],
    )


def read_arrived_files(spark: SparkSession, batch: DataFrame) -> DataFrame | None:
    # The stream only tracks which CSV files arrived. Backblaze reorders columns between files, so each
    # file is read with its own header in batch mode and conformed, exactly as ingest_to_hdfs does.
    paths = [row["path"] for row in batch.select(F.input_file_name().alias("path")).distinct().collect()]
    if not paths:
        return None
    raw = spark.read.option("header", True).csv(paths)
    return conform_to_schema(raw, drive_telemetry_schema())


def main(run_once: bool) -> None:
    spark = build_spark_session("backblaze-stream-anomaly-scores")

    statistics = model_statistics(spark.read.parquet(CLEANED_DIR)).cache()
    context = drive_context(spark.read.parquet(FEATURE_DIR)).cache()
    model = PipelineModel.load(MODEL_DIR) if path_exists(spark, MODEL_DIR) else None
    if model is None:
        print(f"No trained model at {MODEL_DIR}; scoring with the z-score rule only")

    def load_batch(batch: DataFrame, batch_id: int) -> None:
        telemetry = read_arrived_files(spark, batch)
        if telemetry is None:
            return
        scored = score_batch(telemetry, statistics, context, model).withColumn("batch_id", F.lit(batch_id)).withColumn(
            "scored_at", F.current_timestamp()
        )
        append_to_table(spark, scored, SCORES_TABLE, SCORES_ORDER_BY)
        print(f"Batch {batch_id}: scored {scored.count()} readings")

    arrivals = spark.readStream.text(STREAM_INCOMING_DIR)
    writer = arrivals.writeStream.foreachBatch(load_batch).option(
        "checkpointLocation", f"{STREAM_CHECKPOINT_DIR}/{SCORES_TABLE}"
    )
    # --once drains whatever has landed and exits, which is how the deploy workflow and tests run it;
    # without it the job polls the incoming directory until stopped
    query = writer.trigger(availableNow=True).start() if run_once else writer.trigger(processingTime="30 seconds").start()
    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main(run_once="--once" in sys.argv[1:])
