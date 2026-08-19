-- Q1: Daily Traffic Aggregation in Apache Pig

cleaned_logs = LOAD '$INPUT/cleaned_logs' USING PigStorage('\t') AS (
    host:chararray, raw_date:chararray, raw_hour:chararray, method:chararray,
    resource:chararray, status:int, bytes:long, is_error:int
);

grp_by_date = GROUP cleaned_logs BY raw_date;

daily_traffic = FOREACH grp_by_date GENERATE
    group AS log_date,
    COUNT(cleaned_logs) AS total_requests:long,
    SUM(cleaned_logs.bytes) AS total_bytes:long;

daily_traffic_sorted = ORDER daily_traffic BY log_date ASC;

STORE daily_traffic_sorted INTO '$OUTPUT/q1_daily_traffic' USING PigStorage('\t');
