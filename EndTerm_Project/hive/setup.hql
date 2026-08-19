-- Hive Setup Script: Table Definitions for NASA Log Analytics
-- Creates External Raw Ingestion Table using RegexSerDe and Managed Analytical Target Tables

CREATE DATABASE IF NOT EXISTS nasadb;
USE nasadb;

-- Raw External Log Table with RegexSerDe Parsing
CREATE EXTERNAL TABLE IF NOT EXISTS nasa_raw_logs (
    host STRING,
    log_date STRING,
    log_time STRING,
    timezone STRING,
    method STRING,
    resource STRING,
    protocol STRING,
    status INT,
    bytes STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}:\\d{2}:\\d{2})\\s+([^\\]]+)\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$'
)
LOCATION '/data/nasa/raw_logs';

-- Analytical Table: Q1 Daily Traffic
CREATE TABLE IF NOT EXISTS daily_traffic (
    log_date STRING,
    total_requests BIGINT,
    total_bytes BIGINT
)
STORED AS ORC;

-- Analytical Table: Q2 Top Requested Resources
CREATE TABLE IF NOT EXISTS top_resources (
    resource STRING,
    request_count BIGINT,
    total_bytes BIGINT
)
STORED AS ORC;

-- Analytical Table: Q3 Hourly Error Rates
CREATE TABLE IF NOT EXISTS hourly_errors (
    log_hour STRING,
    total_requests BIGINT,
    error_requests BIGINT,
    error_rate DOUBLE
)
STORED AS ORC;
