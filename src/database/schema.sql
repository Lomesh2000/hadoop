-- NASA Log Analytics Pipeline Database Schema
-- Compatible with MySQL and SQLite (standard ANSI SQL)

CREATE TABLE IF NOT EXISTS daily_traffic (
    log_date VARCHAR(10) PRIMARY KEY,
    total_requests BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS top_resources (
    rank_order INT PRIMARY KEY,
    resource VARCHAR(500) NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hourly_errors (
    log_hour VARCHAR(19) PRIMARY KEY,
    total_requests BIGINT NOT NULL,
    error_requests BIGINT NOT NULL,
    error_rate DOUBLE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    execution_id VARCHAR(64) PRIMARY KEY,
    pipeline_mode VARCHAR(32) NOT NULL,
    input_records BIGINT NOT NULL,
    valid_records BIGINT NOT NULL,
    malformed_records BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    duration_seconds DOUBLE NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    check_id VARCHAR(64) PRIMARY KEY,
    check_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL,
    expected_value VARCHAR(256),
    actual_value VARCHAR(256),
    details TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
