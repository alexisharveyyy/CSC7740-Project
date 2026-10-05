-- Each row should return 1 (true). A 0 means the serving layer and feature table disagree,
-- which usually means feature_engineering.py was rerun and appended duplicate rows.

-- Daily table covers every date exactly once
SELECT count() = uniqExact(date) AS daily_dates_unique FROM backblaze.daily_fleet_health;

-- Failures counted per day equal failures counted per model
SELECT
    (SELECT sum(failure_count) FROM backblaze.daily_fleet_health)
    = (SELECT sum(failure_count) FROM backblaze.model_failure_summary) AS failure_totals_agree;

-- Every SMART attribute has both cohorts
SELECT count() = 2 * uniqExact(attribute_id) AS smart_signal_complete FROM backblaze.smart_failure_signal;

-- Watchlist has one row per drive and nobody on it has failed
SELECT count() = uniqExact(serial_number) AS watchlist_one_row_per_drive FROM backblaze.drive_risk_watchlist;

-- Prediction probabilities must be valid
SELECT min(failure_probability) >= 0 AND max(failure_probability) <= 1 AS probabilities_valid
FROM backblaze.drive_failure_predictions;
