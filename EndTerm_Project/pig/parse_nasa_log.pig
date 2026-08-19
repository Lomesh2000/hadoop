-- Pig ETL Script: Parse NASA Common Log Format using REGEX_EXTRACT_ALL

raw_lines = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- Regex extraction pattern for NASA CLF:
-- 1: host, 2: raw_date (DD/Mon/YYYY), 3: time (HH:mm:ss), 4: tz, 5: method, 6: resource, 7: protocol, 8: status, 9: bytes
parsed_records = FOREACH raw_lines GENERATE 
    FLATTEN(
        REGEX_EXTRACT_ALL(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}:\\d{2}:\\d{2})\\s+([^\\]]+)\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$')
    ) AS (
        host:chararray,
        raw_date:chararray,
        raw_time:chararray,
        tz:chararray,
        method:chararray,
        resource:chararray,
        protocol:chararray,
        status:int,
        bytes_str:chararray
    );

-- Filter valid structured lines
valid_logs = FILTER parsed_records BY host IS NOT NULL AND raw_date IS NOT NULL;

-- Convert bytes '-' to 0 and cast to long
cleaned_logs = FOREACH valid_logs GENERATE
    host,
    raw_date,
    SUBSTRING(raw_time, 0, 2) AS raw_hour,
    method,
    resource,
    status,
    ((bytes_str == '-' OR bytes_str IS NULL) ? 0L : (long)bytes_str) AS bytes:long,
    ((status >= 400) ? 1 : 0) AS is_error:int;

STORE cleaned_logs INTO '$OUTPUT/cleaned_logs' USING PigStorage('\t');
