-- Master Pig Execution Pipeline Script
-- Orchestrates data cleaning and queries Q1, Q2, Q3

IMPORT 'parse_nasa_log.pig';
IMPORT 'q1_daily_traffic.pig';
IMPORT 'q2_top_resources.pig';
IMPORT 'q3_hourly_errors.pig';
