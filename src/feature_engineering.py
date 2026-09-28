from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    CLEANED_DIR,
    FEATURE_DIR,
    SMART_RAW_COLUMNS,    # list of SMART raw columns
    build_spark_session,
)
# ------------------------------------------------------------
# DRIVE AGE FEATURES
# ------------------------------------------------------------
# For each drive (serial_number), we want to know:
#   - When it first appeared in the dataset
#   - How many days old it is on each date
#
# This helps model long‑term wear and life cycle behavior.

def add_drive_age(df):
    # Window ordered by date for each drive
    w_drive = Window.partitionBy("serial_number").orderBy("date")
    
    # First day this drive ever appeared
    df = df.withColumn(
        "first_seen",
        F.first("date").over(w_drive)
    )
    
    # Age in days = current date minus first_seen
    df = df.withColumn(
        "age_days",
        F.datediff("date", "first_seen")
    )

    return df
# ------------------------------------------------------------
# SMART TREND FEATURES
# ------------------------------------------------------------
# SMART raw attributes often increase over time.
# We compute rolling averages over:
#   - The last 7 days
#   - The last 30 days
#
# These trends help detect accelerating error behavior.

def add_smart_trends(df):
    # Rolling windows for 7‑day and 30‑day lookbacks
    w_7 = Window.partitionBy("serial_number").orderBy("date").rowsBetween(-7, 0)
    w_30 = Window.partitionBy("serial_number").orderBy("date").rowsBetween(-30, 0)
    
    # Compute trends for each SMART raw column
    for col in SMART_RAW_COLUMNS:
        df = df.withColumn(f"{col}_trend_7d", F.avg(F.col(col)).over(w_7))
        df = df.withColumn(f"{col}_trend_30d", F.avg(F.col(col)).over(w_30))

    return df

# ------------------------------------------------------------
# FAILURE WINDOW LABEL
# ------------------------------------------------------------
# We want to label rows that fall within the 30 days
# leading up to a failure event.
#
# This creates the supervised learning target:
#   failure_label = 1 if row is within 30 days before failure
#   failure_label = 0 otherwise

def add_failure_window_label(df):
    # Partition by drive so we can find its failure date
    w_fail = Window.partitionBy("serial_number")
    
    # The last failure date for each drive
    df = df.withColumn(
        "failure_date",
        F.max(F.when(F.col("failure") == 1, F.col("date"))).over(w_fail)
    )
    # Days until failure = failure_date - current date
    df = df.withColumn(
        "days_until_failure",
        F.datediff("failure_date", "date")
    )
    # Label rows that fall within the 30‑day pre-failure window
    df = df.withColumn(
        "failure_label",
        F.when(
            (F.col("days_until_failure") >= 0) &
            (F.col("days_until_failure") <= 30),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    return df
    
# ------------------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------------------
# We process each drive model separately to reduce memory load.
# This avoids huge window sorts and prevents OOM crashes.    
def main():
    spark = build_spark_session("backblaze-feature-engineering")

    # Identify all drive models present in the cleaned dataset
    all_models = (
        spark.read.parquet(CLEANED_DIR)
        .select("model")
        .distinct()
        .collect()
    )
    # Process each model independently
    for row in all_models:
        model = row["model"]
        print(f"Processing model = {model}")
        
        # Load only this model's data
        df = (
            spark.read.parquet(CLEANED_DIR)
            .where(F.col("model") == model)
        )

        # Re-partition by serial_number so window functions
        # operate on smaller, more manageable partitions
        df = df.repartition("serial_number")
        
        # Apply feature engineering steps
        df = add_drive_age(df)
        df = add_smart_trends(df)
        df = add_failure_window_label(df)

        # Append results to the feature directory
        (
            df.write
            .mode("append")
            .parquet(FEATURE_DIR)
        )

    spark.stop()

if __name__ == "__main__":
    main()