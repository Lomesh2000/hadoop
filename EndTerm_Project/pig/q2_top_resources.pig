-- Q2: Top 20 Requested Resources in Apache Pig

cleaned_logs = LOAD '$INPUT/cleaned_logs' USING PigStorage('\t') AS (
    host:chararray, raw_date:chararray, raw_hour:chararray, method:chararray,
    resource:chararray, status:int, bytes:long, is_error:int
);

grp_by_res = GROUP cleaned_logs BY resource;

res_aggregates = FOREACH grp_by_res GENERATE
    group AS resource,
    COUNT(cleaned_logs) AS request_count:long,
    SUM(cleaned_logs.bytes) AS total_bytes:long;

res_sorted = ORDER res_aggregates BY request_count DESC;

top_20_resources = LIMIT res_sorted 20;

STORE top_20_resources INTO '$OUTPUT/q2_top_resources' USING PigStorage('\t');
