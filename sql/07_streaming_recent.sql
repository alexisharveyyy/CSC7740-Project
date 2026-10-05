-- Everything the streaming job scored in the last hour, worst first
SELECT
    scored_at,
    batch_id,
    date,
    serial_number,
    model,
    anomalous_counter_count,
    round(max_error_zscore, 1) AS max_error_z,
    round(failure_probability, 3) AS failure_probability
FROM backblaze.streaming_anomaly_scores
WHERE scored_at >= now() - INTERVAL 1 HOUR
ORDER BY failure_probability DESC, max_error_zscore DESC
LIMIT 50;

-- Per batch summary, useful while the job is running during the demo
SELECT
    batch_id,
    min(scored_at) AS scored_at,
    count() AS readings,
    countIf(anomalous_counter_count > 0) AS anomalous,
    countIf(failure_probability >= 0.5) AS predicted_failures
FROM backblaze.streaming_anomaly_scores
GROUP BY batch_id
ORDER BY batch_id DESC;
