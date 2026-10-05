-- For each SMART attribute, how far the pre-failure cohort's z-scores sit above the healthy cohort.
-- A large gap means the attribute carries real failure signal.
SELECT
    attribute_id,
    attribute_name,
    round(anyIf(mean_zscore, cohort = 'healthy'), 3) AS healthy_mean_z,
    round(anyIf(mean_zscore, cohort = 'pre_failure'), 3) AS pre_failure_mean_z,
    round(anyIf(mean_zscore, cohort = 'pre_failure') - anyIf(mean_zscore, cohort = 'healthy'), 3) AS gap,
    round(anyIf(p95_zscore, cohort = 'pre_failure'), 3) AS pre_failure_p95_z,
    round(anyIf(reported_share, cohort = 'healthy'), 3) AS reported_share
FROM backblaze.smart_failure_signal
GROUP BY attribute_id, attribute_name
ORDER BY gap DESC;
