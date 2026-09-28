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

`src/common.py` holds the shared paths, schema, and helper functions.

## Setup

```bash
pip install -r requirements.txt
```

## Running a Step

From `src/`:

```bash
spark-submit --py-files common.py ingest_to_hdfs.py
```

## Data

The data (for example `data_Q1_2026/`) is not stored in this repo. Download the daily CSVs from [Backblaze Hard Drive Data](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data).
