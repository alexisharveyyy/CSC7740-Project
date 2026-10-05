-- Drives to pull now: the rule-based watchlist joined with the classifier's latest probability.
-- A drive that appears in both lists with a high probability is the strongest case.
SELECT
    w.serial_number,
    w.model,
    w.as_of_date,
    w.age_days,
    w.risk_score,
    w.anomalous_counter_count,
    w.rising_counter_count,
    w.smart_5_raw AS reallocated,
    w.smart_197_raw AS pending,
    w.smart_187_raw AS uncorrectable,
    round(p.failure_probability, 3) AS failure_probability
FROM backblaze.drive_risk_watchlist AS w
LEFT JOIN
(
    SELECT serial_number, argMax(failure_probability, date) AS failure_probability
    FROM backblaze.drive_failure_predictions
    GROUP BY serial_number
) AS p ON p.serial_number = w.serial_number
ORDER BY w.risk_score DESC, p.failure_probability DESC
LIMIT 20;

-- Classifier-only view: drives the model is most confident about, whether or not a rule fired
SELECT
    serial_number,
    model,
    max(date) AS latest_date,
    round(max(failure_probability), 3) AS max_probability,
    max(failure_label) AS was_actually_pre_failure
FROM backblaze.drive_failure_predictions
GROUP BY serial_number, model
ORDER BY max_probability DESC
LIMIT 20;
