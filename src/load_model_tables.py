from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from clickhouse import path_exists, replace_table
from common import MODEL_EVALUATION_DIR, PREDICTIONS_DIR, build_spark_session

# train_model.py outputs and the sort key for each one's MergeTree
MODEL_TABLES = {
    "drive_failure_predictions": (PREDICTIONS_DIR, "(model, serial_number, date)"),
    "model_evaluation_metrics": (f"{MODEL_EVALUATION_DIR}/metrics", "(model_name, split, metric)"),
    "model_feature_importance": (f"{MODEL_EVALUATION_DIR}/feature_importance", "feature"),
}


def load_model_tables(spark: SparkSession) -> dict[str, DataFrame]:
    tables = {}
    for table_name, (path, _) in MODEL_TABLES.items():
        if path_exists(spark, path):
            tables[table_name] = spark.read.parquet(path)
        else:
            print(f"Skipping {table_name}: {path} does not exist yet, run train_model.py first")
    return tables


def main() -> None:
    spark = build_spark_session("backblaze-load-model-tables")

    for table_name, table in load_model_tables(spark).items():
        print(f"Loading {table_name} into ClickHouse")
        replace_table(spark, table, table_name, MODEL_TABLES[table_name][1])

    spark.stop()


if __name__ == "__main__":
    main()
