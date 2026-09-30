import datetime as dt

import pytest
from pyspark.sql import Row

from clean_normalize import add_per_model_zscores, deduplicate_drive_days, drop_invalid_rows, normalize_model_names
from common import conform_to_schema, drive_telemetry_schema, reported_column, smart_raw_column, zscore_column

REALLOCATED = smart_raw_column(5)
PENDING = smart_raw_column(197)


def drive_day(serial_number, model="MODEL_A", capacity_bytes=4_000_000_000_000, failure=0, reallocated=0, pending=0):
    return Row(
        serial_number=serial_number,
        date=dt.date(2026, 1, 1),
        model=model,
        capacity_bytes=capacity_bytes,
        failure=failure,
        **{REALLOCATED: reallocated, PENDING: pending},
    )


def telemetry(spark, rows):
    # SMART columns not set in drive_day come back null, like an attribute the drive never reports
    return conform_to_schema(spark.createDataFrame(rows), drive_telemetry_schema())


def test_normalize_model_names_trims_and_uppercases(spark):
    frame = telemetry(spark, [drive_day("S1", model="  wdc wd40 ")])

    assert normalize_model_names(frame).first()["model"] == "WDC WD40"


def test_drop_invalid_rows(spark):
    frame = telemetry(
        spark,
        [
            drive_day("GOOD"),
            drive_day("BAD_CAPACITY", capacity_bytes=-1),
            drive_day("BAD_FAILURE", failure=2),
            drive_day(None),
        ],
    )

    assert [row["serial_number"] for row in drop_invalid_rows(frame).collect()] == ["GOOD"]


def test_deduplicate_drive_days(spark):
    frame = telemetry(spark, [drive_day("S1"), drive_day("S1")])

    assert deduplicate_drive_days(frame).count() == 1


def test_zscores_are_per_model_and_keep_missing_readings_null(spark):
    frame = telemetry(
        spark,
        [
            drive_day("A1", reallocated=0, pending=5),
            drive_day("A2", reallocated=10, pending=5),
            drive_day("B1", model="MODEL_B", reallocated=1000, pending=None),
        ],
    )
    rows = {row["serial_number"]: row for row in add_per_model_zscores(frame).collect()}

    # MODEL_B's outlier must not shift MODEL_A's mean (5) or stddev (~7.07)
    assert rows["A1"][zscore_column(REALLOCATED)] == pytest.approx(-0.7071, abs=1e-4)
    assert rows["A2"][zscore_column(REALLOCATED)] == pytest.approx(0.7071, abs=1e-4)
    # Zero variance within a model gives 0.0 rather than dividing by zero
    assert rows["A1"][zscore_column(PENDING)] == 0.0
    assert rows["B1"][zscore_column(PENDING)] is None
    assert rows["B1"][reported_column(PENDING)] is False
    assert rows["A1"][reported_column(PENDING)] is True
