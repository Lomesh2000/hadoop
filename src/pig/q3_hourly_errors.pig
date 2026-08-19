-- Q3: Hourly Error Rates in Apache Pig

cleaned_logs = LOAD '$INPUT/cleaned_logs' USING PigStorage('\t') AS (
    host:chararray, raw_date:chararray, raw_hour:chararray, method:chararray,
    resource:chararray, status:int, bytes:long, is_error:int
);

logs_with_hour = FOREACH cleaned_logs GENERATE
    CONCAT(raw_date, ':', raw_hour, ':00:00') AS log_hour,
    bytes,
    is_error;

grp_by_hour = GROUP logs_with_hour BY log_hour;

hourly_metrics = FOREACH grp_by_hour {
    total_cnt = COUNT(logs_with_hour);
    err_cnt = SUM(logs_with_hour.is_error);
    GENERATE
        group AS log_hour,
        total_cnt AS total_requests:long,
        (err_cnt is null ? 0L : err_cnt) AS error_requests:long,
        ((double)(err_cnt is null ? 0L : err_cnt) / (double)total_cnt) * 100.0 AS error_rate:double;
};

hourly_sorted = ORDER hourly_metrics BY log_hour ASC;

STORE hourly_sorted INTO '$OUTPUT/q3_hourly_errors' USING PigStorage('\t');
