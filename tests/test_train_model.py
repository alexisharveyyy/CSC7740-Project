import datetime as dt

import pytest
from pyspark.sql import functions as F

from train_model import (
    LABEL_COLUMN,
    best_f1_threshold,
    downsample_healthy_rows,
    feature_columns,
    precision_recall_f1,
    prepare_model_rows,
    split_by_drive,
)

LAST_DAY = dt.date(2026, 3, 31)
OLD_DAY = LAST_DAY - dt.timedelta(days=40)
RECENT_DAY = LAST_DAY - dt.timedelta(days=5)


def feature_table(spark, rows):
    # Only the label columns matter here, so every feature column is left empty
    labels = spark.createDataFrame(
        rows, f"serial_number string, model string, date date, {LABEL_COLUMN} int, failure_date date"
    )
    empty_features = [F.lit(None).cast("double").alias(column) for column in feature_columns()]
    return labels.select("*", *empty_features)


def test_prepare_model_rows_drops_healthy_rows_with_unknown_outcome(spark):
    features = feature_table(
        spark,
        [
            ("HEALTHY", "M", OLD_DAY, 0, None),
            ("HEALTHY", "M", RECENT_DAY, 0, None),
            ("FAILED", "M", LAST_DAY, 1, LAST_DAY),
        ],
    )
    rows = prepare_model_rows(features).collect()
    kept = {(row["serial_number"], row["date"]) for row in rows}

    assert kept == {("HEALTHY", OLD_DAY), ("FAILED", LAST_DAY)}
    # Missing feature values are filled with 0
    assert rows[0]["age_days"] == 0.0


def test_split_by_drive_bucket_boundaries(spark):
    rows = spark.createDataFrame([(bucket,) for bucket in (0, 69, 70, 79, 80, 99)], "split_bucket int")

    assert [split.count() for split in split_by_drive(rows)] == [2, 2, 2]


def test_downsample_requires_positive_rows(spark):
    with pytest.raises(ValueError):
        downsample_healthy_rows(spark.createDataFrame([(0.0,)], f"{LABEL_COLUMN} double"))


def test_best_f1_threshold_separates_classes(spark):
    scored = spark.createDataFrame(
        [(1.0, 0.9), (1.0, 0.7), (0.0, 0.3), (0.0, 0.1)], f"{LABEL_COLUMN} double, failure_probability double"
    )

    assert 0.3 < best_f1_threshold(scored) <= 0.7


def test_precision_recall_f1_handles_zero_counts():
    assert precision_recall_f1(tp=2, fp=2, fn=0) == pytest.approx((0.5, 1.0, 2 / 3))
    assert precision_recall_f1(tp=0, fp=0, fn=0) == (0.0, 0.0, 0.0)
