# Backblaze Drive Failure Prediction Pipeline

A PySpark pipeline that ingests Backblaze hard drive telemetry into HDFS, engineers SMART-attribute features, trains a drive failure classifier, and publishes reporting tables to ClickHouse.

## Project Structure

```
.
├── .github/workflows/
│   ├── ci.yml                  # Installs dependencies and verifies all modules import
│   └── deploy.yml              # Deploys to the Spark cluster and runs pipeline stages
├── src/
│   ├── common.py               # Shared config, HDFS paths, schema, and helpers
│   ├── ingest_to_hdfs.py       # Stage 1: raw CSV -> conformed Parquet
│   ├── clean_normalize.py      # Stage 2: conformed -> cleaned Parquet
│   ├── feature_engineering.py  # Stage 3: cleaned -> feature table
│   ├── train_model.py          # Stage 4: train, evaluate, and save the classifier
│   └── serving_layer.py        # Stage 5: load reporting tables into ClickHouse
└── requirements.txt
```

## Pipeline Stages

| Stage      | Script                   | Input (HDFS)  | Output                                             |
|------------|--------------------------|---------------|----------------------------------------------------|
| `ingest`   | `ingest_to_hdfs.py`      | `raw/`        | `conformed/`                                       |
| `clean`    | `clean_normalize.py`     | `conformed/`  | `cleaned/`                                         |
| `features` | `feature_engineering.py` | `cleaned/`    | `features/`                                        |
| `train`    | `train_model.py`         | `features/`   | `models/`, `model_evaluation/`, `predictions/`     |
| `serving`  | `serving_layer.py`       | `features/`   | ClickHouse tables                                  |

All paths are relative to `BACKBLAZE_HDFS_ROOT`.

## Configuration

| Variable              | Default                                      |
|-----------------------|----------------------------------------------|
| `BACKBLAZE_HDFS_ROOT` | `hdfs://namenode:9000/backblaze`             |
| `CLICKHOUSE_JDBC_URL` | `jdbc:clickhouse://clickhouse:8123/backblaze` |
| `CLICKHOUSE_USER`     | `default`                                    |
| `CLICKHOUSE_PASSWORD` | *(empty)*                                    |

## Running a Stage

From `src/`:

```bash
spark-submit --master "$SPARK_MASTER_URL" --py-files common.py ingest_to_hdfs.py
```

The serving stage also needs the ClickHouse JDBC driver:

```bash
spark-submit --master "$SPARK_MASTER_URL" --py-files common.py \
  --packages com.clickhouse:clickhouse-jdbc:0.6.3:all serving_layer.py
```

## CI/CD

- **CI** runs on every push to `main` and on pull requests.
- **Deploy** runs after CI succeeds on `main`, or manually from the Actions tab, where a stage (or `all`) can be selected. It requires a self-hosted runner labeled `spark-master`, the repository variables `SPARK_MASTER_URL`, `BACKBLAZE_HDFS_ROOT`, `CLICKHOUSE_JDBC_URL`, `CLICKHOUSE_USER`, and the secret `CLICKHOUSE_PASSWORD`.
