from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import Row, SparkSession

from common import SMART_RAW_COLUMNS, smart_raw_column, trend_column
from feature_engineering import add_drive_age, add_failure_window_label, add_smart_trends


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("feature-engineering-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


REALLOCATED = smart_raw_column(5)
START = dt.date(2026, 1, 1)


def telemetry_row(serial_number: str, day: int, failure: int = 0, reallocated: int = 0) -> Row:
    row = {
        "date": START + dt.timedelta(days=day),
        "serial_number": serial_number,
        "model": "MODEL_A",
        "capacity_bytes": 4_000_000_000_000,
        "failure": failure,
    }
    for raw_column in SMART_RAW_COLUMNS:
        row[raw_column] = reallocated if raw_column == REALLOCATED else 0
    return Row(**row)


@pytest.fixture(scope="module")
def telemetry(spark):
    rows = (
        # Drive A reports for 50 days and fails on day 49
        [telemetry_row("A", day, failure=int(day == 49), reallocated=day) for day in range(50)]
        # Drive B starts 10 days later and never fails
        + [telemetry_row("B", day) for day in range(10, 50)]
    )
    return spark.createDataFrame(rows)


def test_drive_age_counts_from_each_drives_first_reading(telemetry):
    ages = {(row["serial_number"], row["date"]): row["age_days"] for row in add_drive_age(telemetry).collect()}

    assert ages[("A", START)] == 0
    assert ages[("A", START + dt.timedelta(days=49))] == 49
    assert ages[("B", START + dt.timedelta(days=10))] == 0
    assert ages[("B", START + dt.timedelta(days=49))] == 39


def test_smart_trends_average_the_trailing_window(telemetry):
    trends = {
        row["date"]: row
        for row in add_smart_trends(telemetry).filter("serial_number = 'A'").collect()
    }
    day_20 = trends[START + dt.timedelta(days=20)]

    # Rows 13..20 inclusive for the 7-day window (8 rows), rows 0..20 for the 30-day window
    assert day_20[trend_column(REALLOCATED, 7)] == pytest.approx(sum(range(13, 21)) / 8)
    assert day_20[trend_column(REALLOCATED, 30)] == pytest.approx(sum(range(0, 21)) / 21)
    # Early rows average whatever history exists rather than producing nulls
    assert trends[START][trend_column(REALLOCATED, 30)] == 0.0


def test_failure_window_labels_the_30_days_before_failure_only(telemetry):
    labeled = add_failure_window_label(telemetry).collect()
    by_key = {(row["serial_number"], row["date"]): row for row in labeled}

    failure_day = START + dt.timedelta(days=49)
    assert by_key[("A", failure_day)]["failure_label"] == 1
    assert by_key[("A", failure_day)]["days_until_failure"] == 0
    assert by_key[("A", START + dt.timedelta(days=19))]["failure_label"] == 1
    assert by_key[("A", START + dt.timedelta(days=18))]["failure_label"] == 0
    assert by_key[("A", START)]["failure_date"] == failure_day

    # A drive that never fails has no failure date and no positive labels
    assert by_key[("B", START + dt.timedelta(days=49))]["failure_date"] is None
    assert all(row["failure_label"] == 0 for row in labeled if row["serial_number"] == "B")
