#!/usr/bin/env bash
# Copies one downloaded quarter of CSVs into the raw HDFS directory that ingest_to_hdfs.py reads.
#
# Usage: scripts/load_raw_to_hdfs.sh <quarter-name>
#   scripts/load_raw_to_hdfs.sh data_Q1_2026
set -euo pipefail

quarter_name="${1:?quarter name required}"
hdfs_root="${BACKBLAZE_HDFS_ROOT:-hdfs://master:9000/backblaze}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
source_dir="$repo_root/$quarter_name"

[ -d "$source_dir" ] || { echo "$source_dir not found; run scripts/download_backblaze.sh first" >&2; exit 1; }

hdfs dfs -mkdir -p "$hdfs_root/raw/$quarter_name"
hdfs dfs -put -f "$source_dir"/*.csv "$hdfs_root/raw/$quarter_name/"
hdfs dfs -du -s -h "$hdfs_root/raw/$quarter_name"
