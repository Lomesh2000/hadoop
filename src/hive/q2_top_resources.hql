-- Q2: Top Requested Resources Aggregation in Hive
USE nasadb;

INSERT OVERWRITE TABLE top_resources
SELECT 
    resource,
    COUNT(1) AS request_count,
    SUM(CASE WHEN bytes = '-' OR bytes IS NULL THEN 0 ELSE CAST(bytes AS BIGINT) END) AS total_bytes
FROM nasa_raw_logs
WHERE host IS NOT NULL AND resource IS NOT NULL
GROUP BY resource
ORDER BY request_count DESC
LIMIT 20;
