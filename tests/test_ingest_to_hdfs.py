import datetime as dt
from pathlib import PurePosixPath

from common import drive_telemetry_schema, smart_raw_column
from ingest_to_hdfs import list_quarter_directories, read_quarter


def test_list_quarter_directories_skips_files(spark, tmp_path):
    (tmp_path / "data_Q1_2026").mkdir()
    (tmp_path / "data_Q2_2026").mkdir()
    (tmp_path / "notes.txt").write_text("not a quarter")

    quarter_dirs = list_quarter_directories(spark, tmp_path.as_posix())

    assert {PurePosixPath(path).name for path in quarter_dirs} == {"data_Q1_2026", "data_Q2_2026"}


def test_read_quarter_conforms_csv_to_telemetry_schema(spark, tmp_path):
    (tmp_path / "2026-01-01.csv").write_text(
        f"serial_number,date,model,capacity_bytes,failure,{smart_raw_column(5)},datacenter\n"
        "S1,2026-01-01,MODEL_A,4000,0,12,phx\n"
    )
    quarter = read_quarter(spark, tmp_path.as_posix())
    row = quarter.first()

    assert quarter.columns == drive_telemetry_schema().fieldNames()
    assert row["date"] == dt.date(2026, 1, 1)
    assert row[smart_raw_column(5)] == 12
    assert row[smart_raw_column(9)] is None
