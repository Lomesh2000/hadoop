-- Q3: Hourly Error Rate Breakdown in Hive
USE nasadb;

INSERT OVERWRITE TABLE hourly_errors
SELECT 
    CONCAT(log_date, ' ', SUBSTR(log_time, 1, 2), ':00:00') AS log_hour,
    COUNT(1) AS total_requests,
    SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS error_requests,
    (CAST(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(1)) * 100.0 AS error_rate
FROM nasa_raw_logs
WHERE host IS NOT NULL AND log_date IS NOT NULL AND log_time IS NOT NULL
GROUP BY CONCAT(log_date, ' ', SUBSTR(log_time, 1, 2), ':00:00');
