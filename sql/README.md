# ClickHouse queries

Queries used for the dashboard, the video demo, and the report. Run any file with:

```
clickhouse-client --multiquery < sql/<file>.sql
```

or paste into http://localhost:8123/play (through the SSH tunnel described in docs/CLUSTER_SETUP.md).

| File | Question it answers |
| --- | --- |
| `01_table_inventory.sql` | What is loaded and how big is each table |
| `02_fleet_health_over_time.sql` | How did failures and at-risk counts move across the quarter |
| `03_failure_rate_by_model.sql` | Which drive models fail most, by annualized failure rate |
| `04_smart_signal_strength.sql` | Which SMART attributes separate failing drives from healthy ones |
| `05_riskiest_drives.sql` | Top 20 drives to pull now, combining the rule watchlist and the classifier |
| `06_model_quality.sql` | How well the selected model did on validation and test |
| `07_streaming_recent.sql` | What the streaming job scored in the last hour |
| `08_sanity_checks.sql` | Consistency checks between tables; every row should return true |
