SELECT
    table,
    total_rows,
    formatReadableSize(total_bytes) AS size_on_disk,
    engine
FROM system.tables
WHERE database = 'backblaze'
ORDER BY table;
