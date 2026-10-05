#!/usr/bin/env bash
# Downloads one Backblaze quarterly drive-stats archive and unpacks it next to the repo.
# The CSVs never go in git; scripts/load_raw_to_hdfs.sh pushes them into HDFS.
#
# Usage: scripts/download_backblaze.sh <archive-url> [quarter-name]
#   scripts/download_backblaze.sh https://.../data_Q1_2026.zip data_Q1_2026
# Archive links: https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data
set -euo pipefail

archive_url="${1:?archive URL required}"
quarter_name="${2:-$(basename "$archive_url" .zip)}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
target_dir="$repo_root/$quarter_name"

curl -L -o "$repo_root/$quarter_name.zip" "$archive_url"
unzip -q -o "$repo_root/$quarter_name.zip" -d "$target_dir"
rm "$repo_root/$quarter_name.zip"

# Some archives nest the CSVs one directory deeper; flatten so *.csv works
nested="$(find "$target_dir" -mindepth 2 -name '*.csv' -print -quit)"
if [ -n "$nested" ]; then
  find "$target_dir" -mindepth 2 -name '*.csv' -exec mv {} "$target_dir/" \;
fi

echo "Unpacked $(ls "$target_dir"/*.csv | wc -l) daily files to $target_dir"
