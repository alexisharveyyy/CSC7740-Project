#!/usr/bin/env bash
# Runs the pipeline stages in order on the cluster. Run from anywhere on the Spark master.
#
# Usage: scripts/run_pipeline.sh [stage ...]
#   scripts/run_pipeline.sh                     all seven stages
#   scripts/run_pipeline.sh serving model_tables only the ClickHouse loads
#   scripts/run_pipeline.sh ingest data_Q1_2026  ingest a named quarter (extra args go to the stage)
#
# Reads BACKBLAZE_HDFS_ROOT, CLICKHOUSE_JDBC_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_JDBC_JAR from the
# environment (see .env.example). spark.master comes from spark-defaults.conf unless SPARK_MASTER_URL is set.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
src_dir="$repo_root/src"
clickhouse_jar="${CLICKHOUSE_JDBC_JAR:-$HOME/jars/clickhouse-jdbc-0.6.3-all.jar}"

declare -A SCRIPTS=(
  [ingest]=ingest_to_hdfs.py
  [clean]=clean_normalize.py
  [features]=feature_engineering.py
  [train]=train_model.py
  [serving]=serving_layer.py
  [model_tables]=load_model_tables.py
  [stream]=stream_anomaly_scores.py
)
ORDER=(ingest clean features train serving model_tables stream)

run_stage() {
  local stage="$1"; shift
  local args=()
  [ -n "${SPARK_MASTER_URL:-}" ] && args+=(--master "$SPARK_MASTER_URL")
  case "$stage" in
    serving|model_tables|stream)
      [ -f "$clickhouse_jar" ] || { echo "ClickHouse JDBC jar not found at $clickhouse_jar (see .env.example)" >&2; exit 1; }
      args+=(--jars "$clickhouse_jar") ;;
  esac
  args+=(--py-files "$src_dir/common.py" "$src_dir/${SCRIPTS[$stage]}")
  # The stream stage drains what has landed and exits; drop --once to keep it running
  [ "$stage" = stream ] && args+=(--once)
  echo "==> $stage"
  (cd "$src_dir" && spark-submit "${args[@]}" "$@")
}

if [ $# -eq 0 ]; then
  for stage in "${ORDER[@]}"; do run_stage "$stage"; done
else
  stage="$1"; shift
  [ -n "${SCRIPTS[$stage]:-}" ] || { echo "Unknown stage: $stage (choose from ${ORDER[*]})" >&2; exit 1; }
  if [ $# -gt 0 ] && [ -n "${SCRIPTS[$1]:-}" ]; then
    # Several stage names: run each with no extra args
    run_stage "$stage"
    for stage in "$@"; do run_stage "$stage"; done
  else
    run_stage "$stage" "$@"
  fi
fi
