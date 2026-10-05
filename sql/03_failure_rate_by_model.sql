-- Annualized failure rate per model, the same metric Backblaze publishes.
-- Models with few drives have noisy rates, so the filter keeps the comparison honest.
SELECT
    model,
    drive_count,
    drive_days,
    failure_count,
    round(annualized_failure_rate_pct, 2) AS afr_pct,
    round(mean_capacity_tb, 1) AS capacity_tb,
    round(mean_age_at_failure_days) AS mean_age_at_failure_days
FROM backblaze.model_failure_summary
WHERE drive_count >= 100
ORDER BY annualized_failure_rate_pct DESC
LIMIT 25;
