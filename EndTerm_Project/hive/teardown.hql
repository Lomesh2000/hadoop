-- Teardown Script: Drops Hive Tables and Database
USE nasadb;
DROP TABLE IF EXISTS daily_traffic;
DROP TABLE IF EXISTS top_resources;
DROP TABLE IF EXISTS hourly_errors;
DROP TABLE IF EXISTS nasa_raw_logs;
DROP DATABASE IF EXISTS nasadb CASCADE;
