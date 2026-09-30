from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import Row, SparkSession

from common import SMART_RAW_COLUMNS, smart_raw_column, trend_column
from stream_anomaly_scores import TREND_WINDOWS_DAYS, drive_context, model_statistics, score_batch


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("stream-anomaly-scores-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


REALLOCATED = smart_raw_column(5)
PENDING = smart_raw_column(197)


def telemetry_row(serial_number: str, date: dt.date, model: str = "MODEL_A", **smart) -> Row:
    row = {"date": date, "serial_number": serial_number, "model": model, "capacity_bytes": 4_000_000_000_000, "failure": 0}
    for raw_column in SMART_RAW_COLUMNS:
        row[raw_column] = smart.get(raw_column, 0)
    return Row(**row)


def feature_row(serial_number: str, date: dt.date, first_seen: dt.date) -> Row:
    row = {"serial_number": serial_number, "date": date, "first_seen": first_seen}
    for raw_column in SMART_RAW_COLUMNS:
        for days in TREND_WINDOWS_DAYS:
            row[trend_column(raw_column, days)] = 1.0
    return Row(**row)


@pytest.fixture(scope="module")
def statistics(spark):
    # Ten healthy history readings per model with reallocated sectors 0..9: mean 4.5, stddev ~3.03
    history = [
        telemetry_row(f"H{i}", dt.date(2026, 1, 1), model=model, **{REALLOCATED: i})
        for model in ("MODEL_A", "MODEL_B")
        for i in range(10)
    ]
    return model_statistics(spark.createDataFrame(history))


@pytest.fixture(scope="module")
def context(spark):
    return drive_context(
        spark.createDataFrame(
            [
                feature_row("KNOWN", dt.date(2026, 1, 20), dt.date(2026, 1, 1)),
                feature_row("KNOWN", dt.date(2026, 1, 10), dt.date(2026, 1, 1)),
            ]
        )
    )


def test_zscores_use_per_model_history_not_the_batch(spark, statistics, context):
    batch = spark.createDataFrame(
        [
            telemetry_row("KNOWN", dt.date(2026, 2, 1), **{REALLOCATED: 400}),
            telemetry_row("OTHER", dt.date(2026, 2, 1), **{REALLOCATED: 4}),
        ]
    )
    scored = {row["serial_number"]: row for row in score_batch(batch, statistics, context, model=None).collect()}

    assert scored["KNOWN"]["anomalous_counter_count"] == 1
    assert scored["KNOWN"]["max_error_zscore"] > 100
    assert scored["OTHER"]["anomalous_counter_count"] == 0
    assert abs(scored["OTHER"]["max_error_zscore"]) < 1


def test_age_comes_from_feature_history_and_defaults_for_new_drives(spark, statistics, context):
    batch = spark.createDataFrame(
        [telemetry_row("KNOWN", dt.date(2026, 2, 1)), telemetry_row("BRAND_NEW", dt.date(2026, 2, 1))]
    )
    scored = {row["serial_number"]: row for row in score_batch(batch, statistics, context, model=None).collect()}

    assert scored["KNOWN"]["age_days"] == 31
    assert scored["BRAND_NEW"]["age_days"] == 0


def test_missing_readings_do_not_count_as_anomalies(spark, statistics, context):
    batch = spark.createDataFrame(
        [telemetry_row("KNOWN", dt.date(2026, 2, 1), **{PENDING: None}), telemetry_row("OTHER", dt.date(2026, 2, 1))]
    )
    row = score_batch(batch, statistics, context, model=None).filter("serial_number = 'KNOWN'").collect()[0]

    assert row["anomalous_counter_count"] == 0
    assert row[PENDING] is None
    assert row["failure_probability"] is None


def test_drive_context_keeps_latest_row_per_drive(context):
    rows = context.collect()
    assert len(rows) == 1
    assert rows[0]["first_seen"] == dt.date(2026, 1, 1)
