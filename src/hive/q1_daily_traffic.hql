-- Q1: Daily Traffic Aggregation in Hive
USE nasadb;

INSERT OVERWRITE TABLE daily_traffic
SELECT 
    log_date,
    COUNT(1) AS total_requests,
    SUM(CASE WHEN bytes = '-' OR bytes IS NULL THEN 0 ELSE CAST(bytes AS BIGINT) END) AS total_bytes
FROM nasa_raw_logs
WHERE host IS NOT NULL AND log_date IS NOT NULL
GROUP BY log_date;
