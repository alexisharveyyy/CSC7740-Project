import datetime as dt

from pyspark.sql import Row

from common import conform_to_schema, drive_telemetry_schema, smart_raw_column, union_all, with_date_partitions


def test_conform_to_schema_casts_fills_missing_and_drops_extra(spark):
    raw = spark.createDataFrame([Row(failure="1", serial_number="S1", date="2026-01-05", model="M", datacenter="phx")])
    conformed = conform_to_schema(raw, drive_telemetry_schema())
    row = conformed.first()

    assert conformed.columns == drive_telemetry_schema().fieldNames()
    assert row["date"] == dt.date(2026, 1, 5)
    assert row["failure"] == 1
    assert row["capacity_bytes"] is None
    assert row[smart_raw_column(5)] is None


def test_union_all_matches_columns_by_name(spark):
    first = spark.createDataFrame([(1, "a")], "x int, y string")
    second = spark.createDataFrame([("b", 2)], "y string, x int")

    rows = union_all([first, second]).orderBy("x").collect()

    assert [(row["x"], row["y"]) for row in rows] == [(1, "a"), (2, "b")]


def test_with_date_partitions_adds_year_and_month(spark):
    frame = spark.createDataFrame([Row(date=dt.date(2026, 3, 15))])
    row = with_date_partitions(frame).first()

    assert row["year"] == 2026
    assert row["month"] == 3
