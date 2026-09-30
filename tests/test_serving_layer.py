from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import Row, SparkSession

from common import SMART_ATTRIBUTES, SMART_RAW_COLUMNS, reported_column, smart_raw_column, trend_column, zscore_column
from serving_layer import (
    build_drive_risk_watchlist,
    build_reporting_tables,
    summarize_daily_fleet_health,
    summarize_model_failures,
    summarize_smart_failure_signal,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("serving-layer-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def feature_row(
    serial_number: str,
    date: dt.date,
    model: str = "MODEL_A",
    failure: int = 0,
    failure_label: int = 0,
    failure_date: dt.date | None = None,
    age_days: int = 0,
    temperature: int = 30,
    zscores: dict[str, float] | None = None,
    raw_values: dict[str, int] | None = None,
    trends_7d: dict[str, float] | None = None,
    trends_30d: dict[str, float] | None = None,
) -> Row:
    zscores = zscores or {}
    raw_values = raw_values or {}
    trends_7d = trends_7d or {}
    trends_30d = trends_30d or {}
    row = {
        "date": date,
        "serial_number": serial_number,
        "model": model,
        "capacity_bytes": 4_000_000_000_000,
        "failure": failure,
        "failure_label": failure_label,
        "failure_date": failure_date,
        "age_days": age_days,
    }
    for raw_column in SMART_RAW_COLUMNS:
        row[raw_column] = temperature if raw_column == smart_raw_column(194) else raw_values.get(raw_column, 0)
        row[zscore_column(raw_column)] = zscores.get(raw_column, 0.0)
        row[reported_column(raw_column)] = True
        row[trend_column(raw_column, 7)] = trends_7d.get(raw_column, 0.0)
        row[trend_column(raw_column, 30)] = trends_30d.get(raw_column, 0.0)
    return Row(**row)


DAY_1 = dt.date(2026, 1, 1)
DAY_2 = dt.date(2026, 1, 2)
REALLOCATED = smart_raw_column(5)
PENDING = smart_raw_column(197)


@pytest.fixture(scope="module")
def features(spark):
    rows = [
        # Healthy drive, two days of readings
        feature_row("H1", DAY_1, age_days=10),
        feature_row("H1", DAY_2, age_days=11),
        # Drive that fails on day 2, flagged on both days
        feature_row("F1", DAY_1, failure_label=1, failure_date=DAY_2, age_days=99, zscores={REALLOCATED: 3.0}),
        feature_row("F1", DAY_2, failure=1, failure_label=1, failure_date=DAY_2, age_days=100, zscores={REALLOCATED: 4.0}),
        # Running drive on a second model with an anomalous and rising pending-sector count
        feature_row("R1", DAY_1, model="MODEL_B", age_days=5),
        feature_row(
            "R1",
            DAY_2,
            model="MODEL_B",
            age_days=6,
            temperature=45,
            zscores={PENDING: 2.5},
            raw_values={PENDING: 8},
            trends_7d={PENDING: 5.0},
            trends_30d={PENDING: 1.0},
        ),
    ]
    return spark.createDataFrame(rows)


def test_daily_fleet_health_counts_drives_failures_and_at_risk(features):
    daily = {row["date"]: row for row in summarize_daily_fleet_health(features).collect()}

    assert daily[DAY_1]["drive_count"] == 3
    assert daily[DAY_1]["failure_count"] == 0
    assert daily[DAY_1]["at_risk_count"] == 1
    assert daily[DAY_2]["failure_count"] == 1
    assert daily[DAY_2]["failure_rate_pct"] == pytest.approx(100 / 3)
    assert daily[DAY_2]["mean_temperature_celsius"] == pytest.approx(35.0)


def test_model_failure_summary_computes_annualized_failure_rate(features):
    by_model = {row["model"]: row for row in summarize_model_failures(features).collect()}

    model_a = by_model["MODEL_A"]
    assert model_a["drive_count"] == 2
    assert model_a["drive_days"] == 4
    assert model_a["failure_count"] == 1
    assert model_a["annualized_failure_rate_pct"] == pytest.approx(1 / (4 / 365) * 100)
    assert model_a["mean_age_at_failure_days"] == pytest.approx(100.0)
    assert model_a["mean_capacity_tb"] == pytest.approx(4.0)

    model_b = by_model["MODEL_B"]
    assert model_b["failure_count"] == 0
    assert model_b["annualized_failure_rate_pct"] == 0.0
    assert model_b["mean_age_at_failure_days"] is None


def test_smart_failure_signal_separates_cohorts(features):
    signal = summarize_smart_failure_signal(features)
    rows = {(row["attribute_id"], row["cohort"]): row for row in signal.collect()}

    # Every attribute appears once per cohort
    assert len(rows) == len(SMART_ATTRIBUTES) * 2
    assert rows[(5, "pre_failure")]["attribute_name"] == "reallocated_sector_count"
    assert rows[(5, "pre_failure")]["row_count"] == 2
    assert rows[(5, "pre_failure")]["mean_zscore"] == pytest.approx(3.5)
    assert rows[(5, "healthy")]["row_count"] == 4
    assert rows[(5, "healthy")]["mean_zscore"] == pytest.approx(0.0)
    assert rows[(5, "healthy")]["reported_share"] == pytest.approx(1.0)


def test_risk_watchlist_keeps_only_running_drives_with_a_signal(features):
    watchlist = build_drive_risk_watchlist(features).collect()

    assert [row["serial_number"] for row in watchlist] == ["R1"]
    r1 = watchlist[0]
    # Uses the latest reading, not the first
    assert r1["as_of_date"] == DAY_2
    assert r1["age_days"] == 6
    assert r1["temperature_celsius"] == 45
    assert r1["anomalous_counter_count"] == 1
    assert r1["rising_counter_count"] == 1
    assert r1["risk_score"] == 2


def test_failed_drives_are_excluded_from_watchlist_even_with_high_zscores(features):
    serials = {row["serial_number"] for row in build_drive_risk_watchlist(features).collect()}
    assert "F1" not in serials


def test_build_reporting_tables_returns_all_four(features):
    tables = build_reporting_tables(features)
    assert set(tables) == {
        "daily_fleet_health",
        "model_failure_summary",
        "smart_failure_signal",
        "drive_risk_watchlist",
    }
    for table in tables.values():
        assert table.count() > 0
