-- Daily fleet size, failures, and drives inside the 30-day pre-failure window
SELECT
    date,
    drive_count,
    failure_count,
    at_risk_count,
    round(failure_rate_pct, 4) AS failure_rate_pct,
    round(at_risk_rate_pct, 2) AS at_risk_rate_pct,
    round(mean_temperature_celsius, 1) AS mean_temp_c
FROM backblaze.daily_fleet_health
ORDER BY date;

-- Weekly rollup for a cleaner chart
SELECT
    toStartOfWeek(date) AS week,
    sum(failure_count) AS failures,
    round(avg(drive_count)) AS avg_drives,
    round(sum(failure_count) / avg(drive_count) * 100, 4) AS weekly_failure_rate_pct
FROM backblaze.daily_fleet_health
GROUP BY week
ORDER BY week;
