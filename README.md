# CSC 7740 Project: Backblaze Drive Failure Prediction

A PySpark pipeline that predicts hard drive failures from Backblaze SMART telemetry data.

## Pipeline

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `src/ingest_to_hdfs.py` | Loads raw CSVs into HDFS as Parquet |
| 2 | `src/clean_normalize.py` | Cleans and normalizes the data |
| 3 | `src/feature_engineering.py` | Builds model features |
| 4 | `src/train_model.py` | Trains and evaluates the failure classifier |
| 5 | `src/serving_layer.py` | Loads reporting tables into ClickHouse |
| 6 | `src/load_model_tables.py` | Loads model predictions and evaluation into ClickHouse |
| 7 | `src/stream_anomaly_scores.py` | Streams anomaly scores into ClickHouse (`--once` drains and exits) |

`src/common.py` holds the shared paths, schema, and helper functions. `src/clickhouse.py` holds the ClickHouse helpers.

## Setup

Requires Java 17 for Spark. If your system Python blocks `pip install` (e.g. Homebrew), use a venv (`.venv/` is gitignored):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running Tests

```bash
pytest tests
```

On Windows, use Python 3.9–3.11 (PySpark 3.5 does not support newer versions) and point Spark at the venv's Python, since plain `python` opens the Microsoft Store alias:

```powershell
py -3.9 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:PYSPARK_PYTHON = (Resolve-Path .venv\Scripts\python.exe).Path
.venv\Scripts\python.exe -m pytest tests
```

## Running a Step

From `src/`:

```bash
spark-submit --py-files common.py ingest_to_hdfs.py
```

## Data

The data (for example `data_Q1_2026/`) is not stored in this repo. Download the daily CSVs from [Backblaze Hard Drive Data](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data).
