from __future__ import annotations

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import (
    FEATURE_DIR,
    MODEL_DIR,
    MODEL_EVALUATION_DIR,
    PREDICTIONS_DIR,
    SMART_RAW_COLUMNS,
    build_spark_session,
    reported_column,
    trend_column,
    zscore_column,
)

LABEL_COLUMN = "failure_label"
LABEL_HORIZON_DAYS = 30
TREND_WINDOWS_DAYS = (7, 30)

# Drives are hashed into 100 buckets; 0-69 train, 70-79 validation, 80-99 test
TRAIN_BUCKET_LIMIT = 70
VALIDATION_BUCKET_LIMIT = 80

NEGATIVE_TO_POSITIVE_RATIO = 20
THRESHOLD_CANDIDATES = [round(0.05 * step, 2) for step in range(1, 20)]
RANDOM_SEED = 7740


def feature_columns() -> list[str]:
    smart_features = [
        column
        for raw_column in SMART_RAW_COLUMNS
        for column in (
            raw_column,
            zscore_column(raw_column),
            reported_column(raw_column),
            *[trend_column(raw_column, days) for days in TREND_WINDOWS_DAYS],
        )
    ]
    return ["age_days", "capacity_bytes", *smart_features]


def prepare_model_rows(features: DataFrame) -> DataFrame:
    # A healthy drive near the end of the data may fail right after it, so its label is not yet known
    last_date = features.agg(F.max("date")).first()[0]
    unknown_outcome = F.col("failure_date").isNull() & (
        F.col("date") > F.date_sub(F.lit(last_date), LABEL_HORIZON_DAYS)
    )
    return features.filter(~unknown_outcome).select(
        "serial_number",
        "model",
        "date",
        F.col(LABEL_COLUMN).cast("double").alias(LABEL_COLUMN),
        # Missing readings become 0; the *_reported flags keep them apart from genuine zeros
        *[F.coalesce(F.col(column).cast("double"), F.lit(0.0)).alias(column) for column in feature_columns()],
        F.pmod(F.xxhash64("serial_number"), F.lit(100)).alias("split_bucket"),
    )


def split_by_drive(rows: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    # Splitting rows at random would put days from the same drive on both sides and inflate the scores
    bucket = F.col("split_bucket")
    return (
        rows.filter(bucket < TRAIN_BUCKET_LIMIT),
        rows.filter((bucket >= TRAIN_BUCKET_LIMIT) & (bucket < VALIDATION_BUCKET_LIMIT)),
        rows.filter(bucket >= VALIDATION_BUCKET_LIMIT),
    )


def downsample_healthy_rows(train: DataFrame) -> DataFrame:
    label_counts = {row[LABEL_COLUMN]: row["count"] for row in train.groupBy(LABEL_COLUMN).count().collect()}
    positive_count = label_counts.get(1.0, 0)
    negative_count = label_counts.get(0.0, 0)
    if positive_count == 0:
        raise ValueError("Training split has no pre-failure rows; load more days of data")
    keep_fraction = min(1.0, positive_count * NEGATIVE_TO_POSITIVE_RATIO / negative_count)
    return train.sampleBy(LABEL_COLUMN, fractions={0.0: keep_fraction, 1.0: 1.0}, seed=RANDOM_SEED)


def candidate_pipelines() -> dict[str, Pipeline]:
    def assembler() -> VectorAssembler:
        return VectorAssembler(inputCols=feature_columns(), outputCol="raw_features")

    logistic_regression = LogisticRegression(
        featuresCol="features",
        labelCol=LABEL_COLUMN,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5,
    )
    boosted_trees = GBTClassifier(
        featuresCol="raw_features",
        labelCol=LABEL_COLUMN,
        maxIter=60,
        maxDepth=5,
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=RANDOM_SEED,
    )
    return {
        "logistic_regression": Pipeline(
            stages=[assembler(), StandardScaler(inputCol="raw_features", outputCol="features"), logistic_regression]
        ),
        # Tree splits ignore feature scale, so no scaler
        "gradient_boosted_trees": Pipeline(stages=[assembler(), boosted_trees]),
    }


def score(model: PipelineModel, rows: DataFrame) -> DataFrame:
    return model.transform(rows).withColumn("failure_probability", vector_to_array("probability")[1])


def confusion_counts(scored: DataFrame, thresholds: list[float]) -> dict[float, dict[str, int]]:
    # Every threshold is counted in a single pass over the data
    is_positive = F.col(LABEL_COLUMN) == 1.0
    aggregates = []
    for index, threshold in enumerate(thresholds):
        flagged = F.col("failure_probability") >= threshold
        aggregates += [
            F.sum(F.when(flagged & is_positive, 1).otherwise(0)).alias(f"tp_{index}"),
            F.sum(F.when(flagged & ~is_positive, 1).otherwise(0)).alias(f"fp_{index}"),
            F.sum(F.when(~flagged & is_positive, 1).otherwise(0)).alias(f"fn_{index}"),
        ]
    totals = scored.agg(*aggregates).first()
    return {
        threshold: {"tp": totals[f"tp_{index}"], "fp": totals[f"fp_{index}"], "fn": totals[f"fn_{index}"]}
        for index, threshold in enumerate(thresholds)
    }


def ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def precision_recall_f1(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    precision = ratio(tp, tp + fp)
    recall = ratio(tp, tp + fn)
    return precision, recall, ratio(2 * precision * recall, precision + recall)


def best_f1_threshold(scored: DataFrame) -> float:
    # Training dropped most healthy rows, which inflates probabilities, so the cut-off is tuned on untouched data
    counts = confusion_counts(scored, THRESHOLD_CANDIDATES)
    return max(THRESHOLD_CANDIDATES, key=lambda threshold: precision_recall_f1(**counts[threshold])[2])


def drive_level_rates(scored: DataFrame, threshold: float) -> dict[str, float]:
    # Operators replace drives, not drive-days, so this is the number that matters in practice
    per_drive = scored.groupBy("serial_number").agg(
        F.max(LABEL_COLUMN).alias("failed"),
        F.max(F.when(F.col("failure_probability") >= threshold, 1.0).otherwise(0.0)).alias("flagged"),
    )
    totals = per_drive.agg(
        F.sum("failed").alias("failed_drives"),
        F.sum(F.col("failed") * F.col("flagged")).alias("caught_drives"),
        F.sum(1 - F.col("failed")).alias("healthy_drives"),
        F.sum((1 - F.col("failed")) * F.col("flagged")).alias("false_alarm_drives"),
    ).first()
    return {
        "drive_recall": ratio(totals["caught_drives"], totals["failed_drives"]),
        "drive_false_alarm_rate": ratio(totals["false_alarm_drives"], totals["healthy_drives"]),
    }


def evaluate(scored: DataFrame, threshold: float) -> dict[str, float]:
    # Area under PR is the headline metric; with so few failures, ROC and accuracy look good for any model
    evaluator = BinaryClassificationEvaluator(labelCol=LABEL_COLUMN, rawPredictionCol="rawPrediction")
    precision, recall, f1 = precision_recall_f1(**confusion_counts(scored, [threshold])[threshold])
    return {
        "area_under_pr": evaluator.setMetricName("areaUnderPR").evaluate(scored),
        "area_under_roc": evaluator.setMetricName("areaUnderROC").evaluate(scored),
        "threshold": threshold,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        **drive_level_rates(scored, threshold),
    }


def feature_importance(spark: SparkSession, model: PipelineModel) -> DataFrame:
    classifier = model.stages[-1]
    if hasattr(classifier, "featureImportances"):
        weights = classifier.featureImportances.toArray()
    else:
        # Inputs were standardized, so coefficient size is comparable across features
        weights = abs(classifier.coefficients.toArray())
    return spark.createDataFrame(
        [(name, float(weight)) for name, weight in zip(feature_columns(), weights)],
        "feature string, importance double",
    ).orderBy(F.col("importance").desc())


def main() -> None:
    spark = build_spark_session("backblaze-train-model")

    rows = prepare_model_rows(spark.read.parquet(FEATURE_DIR))
    train, validation, test = split_by_drive(rows)
    train = downsample_healthy_rows(train).cache()
    validation = validation.cache()

    candidates = {}
    for model_name, pipeline in candidate_pipelines().items():
        print(f"Training {model_name}")
        model = pipeline.fit(train)
        scored_validation = score(model, validation)
        threshold = best_f1_threshold(scored_validation)
        candidates[model_name] = (model, evaluate(scored_validation, threshold))
        print(f"{model_name} validation: {candidates[model_name][1]}")

    best_name = max(candidates, key=lambda model_name: candidates[model_name][1]["area_under_pr"])
    best_model, best_validation_metrics = candidates[best_name]
    best_threshold = best_validation_metrics["threshold"]

    # The test split is only touched once, after the model and threshold are fixed
    scored_test = score(best_model, test).cache()
    test_metrics = evaluate(scored_test, best_threshold)
    print(f"{best_name} test: {test_metrics}")

    metric_rows = [
        (model_name, "validation", metric, float(value))
        for model_name, (_, metrics) in candidates.items()
        for metric, value in metrics.items()
    ] + [(best_name, "test", metric, float(value)) for metric, value in test_metrics.items()]

    best_model.write().overwrite().save(MODEL_DIR)
    spark.createDataFrame(metric_rows, "model_name string, split string, metric string, value double").write.mode(
        "overwrite"
    ).parquet(f"{MODEL_EVALUATION_DIR}/metrics")
    feature_importance(spark, best_model).write.mode("overwrite").parquet(f"{MODEL_EVALUATION_DIR}/feature_importance")
    scored_test.select(
        "serial_number",
        "model",
        "date",
        LABEL_COLUMN,
        "failure_probability",
        (F.col("failure_probability") >= best_threshold).cast("int").alias("predicted_failure"),
    ).write.mode("overwrite").parquet(PREDICTIONS_DIR)

    scored_test.unpersist()
    validation.unpersist()
    train.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
