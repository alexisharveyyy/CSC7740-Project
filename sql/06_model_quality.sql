-- Validation and test metrics for every candidate model, pivoted one row per model and split
SELECT
    model_name,
    split,
    round(anyIf(value, metric = 'area_under_pr'), 4) AS pr_auc,
    round(anyIf(value, metric = 'area_under_roc'), 4) AS roc_auc,
    round(anyIf(value, metric = 'precision'), 4) AS precision,
    round(anyIf(value, metric = 'recall'), 4) AS recall,
    round(anyIf(value, metric = 'f1'), 4) AS f1,
    round(anyIf(value, metric = 'drive_recall'), 4) AS drive_recall,
    round(anyIf(value, metric = 'drive_false_alarm_rate'), 4) AS drive_false_alarm_rate,
    anyIf(value, metric = 'threshold') AS threshold
FROM backblaze.model_evaluation_metrics
GROUP BY model_name, split
ORDER BY model_name, split;

-- What the selected model relies on
SELECT feature, round(importance, 4) AS importance
FROM backblaze.model_feature_importance
ORDER BY importance DESC
LIMIT 15;
