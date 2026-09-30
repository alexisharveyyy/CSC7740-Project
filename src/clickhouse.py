from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

# Written through Spark's JDBC source so the load stays distributed across executors.
# Needs the driver on the classpath: spark-submit --packages com.clickhouse:clickhouse-jdbc:0.6.3:all
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/backblaze")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
CLICKHOUSE_JDBC_PACKAGE = "com.clickhouse:clickhouse-jdbc:0.6.3:all"

CLICKHOUSE_TYPES = {
    DateType: "Date",
    TimestampType: "DateTime",
    StringType: "String",
    BooleanType: "Bool",
    IntegerType: "Int32",
    LongType: "Int64",
    DoubleType: "Float64",
}


def path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(hadoop_path)


def clickhouse_type(data_type: DataType, nullable: bool) -> str:
    type_name = CLICKHOUSE_TYPES[type(data_type)]
    return f"Nullable({type_name})" if nullable else type_name


def create_table_statement(frame: DataFrame, table_name: str, order_by: str) -> str:
    # Sort key columns are never null; every other column is created Nullable because some
    # summaries (e.g. mean age at failure) are legitimately null
    key_columns = set(order_by.strip("()").replace(" ", "").split(","))
    column_definitions = ", ".join(
        f"{field.name} {clickhouse_type(field.dataType, field.name not in key_columns)}"
        for field in frame.schema.fields
    )
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions}) ENGINE = MergeTree ORDER BY {order_by}"


def execute_statement(spark: SparkSession, statement: str) -> None:
    # Spark's JDBC writer cannot express Nullable or ENGINE, so DDL goes through the driver's own JDBC
    # connection. Jars passed with --jars or --packages are invisible to java.sql.DriverManager, so the
    # driver class is loaded through Spark's classloader and used directly.
    jvm = spark._jvm
    driver = jvm.org.apache.spark.util.Utils.classForName(CLICKHOUSE_DRIVER, True, False).newInstance()
    properties = jvm.java.util.Properties()
    properties.setProperty("user", CLICKHOUSE_USER)
    properties.setProperty("password", CLICKHOUSE_PASSWORD)
    connection = driver.connect(CLICKHOUSE_URL, properties)
    try:
        connection.createStatement().execute(statement)
    finally:
        connection.close()


def append_rows(frame: DataFrame, table_name: str) -> None:
    (
        frame.write.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("dbtable", table_name)
        .option("batchsize", 10000)
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )


def replace_table(spark: SparkSession, frame: DataFrame, table_name: str, order_by: str) -> None:
    # Spark has no ClickHouse dialect, so its overwrite mode would recreate the table without
    # ORDER BY or Nullable columns. Rebuild the table ourselves from the frame's schema, then append.
    execute_statement(spark, f"DROP TABLE IF EXISTS {table_name}")
    execute_statement(spark, create_table_statement(frame, table_name, order_by))
    append_rows(frame, table_name)


def append_to_table(spark: SparkSession, frame: DataFrame, table_name: str, order_by: str) -> None:
    # For tables that accumulate over time (streaming scores): create once, never drop
    execute_statement(spark, create_table_statement(frame, table_name, order_by))
    append_rows(frame, table_name)
