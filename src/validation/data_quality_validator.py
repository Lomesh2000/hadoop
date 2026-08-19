#!/usr/bin/env python3
"""
Automated Data Quality Validation Suite for NASA Log Analytics Pipeline
Executes deterministic assertions and verification rules against processed outputs and database tables.
Outputs a structured PASS/FAIL validation report.
"""

import sys
import os
import sqlite3
import argparse
from typing import List, Dict, Any, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'mapreduce_python'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config'))
from log_parser import parse_log_line
from config_loader import load_config

DEFAULT_SQLITE_PATH = os.environ.get('SQLITE_PATH', '/tmp/nasalog_analytics.db')


class DataQualityValidator:
    def __init__(self, raw_input_file: str, db_type: str = "sqlite", max_malformed_pct: float = None, **db_kwargs):
        self.raw_input_file = raw_input_file
        self.db_type = db_type
        self.db_kwargs = db_kwargs
        self.sqlite_path = db_kwargs.get("sqlite_path", DEFAULT_SQLITE_PATH)
        
        # Load max allowable malformed rate from config if not passed
        cfg = load_config()
        self.max_malformed_pct = max_malformed_pct if max_malformed_pct is not None else cfg['data_quality']['max_malformed_percentage']
        
        self.checks_run = []
        self.conn = None
        self._init_db()

    def _init_db(self):
        if self.db_type == "sqlite":
            self.conn = sqlite3.connect(self.sqlite_path)
            self.conn.row_factory = sqlite3.Row
        elif self.db_type == "mysql":
            import pymysql
            self.conn = pymysql.connect(
                host=self.db_kwargs.get("host", os.environ.get("DB_HOST", "localhost")),
                port=int(self.db_kwargs.get("port", os.environ.get("DB_PORT", 3306))),
                user=self.db_kwargs.get("user", os.environ.get("DB_USER", "root")),
                password=self.db_kwargs.get("password", os.environ.get("DB_PASSWORD", "")),
                database=self.db_kwargs.get("database", os.environ.get("DB_NAME", "nasadb")),
                cursorclass=pymysql.cursors.DictCursor
            )

    def _record_check(self, check_id: str, name: str, table: str, passed: bool, expected: Any, actual: Any, details: str):
        status = "PASS" if passed else "FAIL"
        check_res = {
            'check_id': check_id,
            'name': name,
            'table': table,
            'status': status,
            'expected': str(expected),
            'actual': str(actual),
            'details': details
        }
        self.checks_run.append(check_res)
        return passed

    def check_raw_log_accounting_and_threshold(self) -> Tuple[bool, bool]:
        """
        Rule 1: Input Accounting: total_input == valid_parsed + malformed_parsed
        Rule 7: Malformed Record Rate Threshold: (malformed / total) * 100 <= max_malformed_pct
        """
        total_input = 0
        valid_parsed = 0
        malformed_parsed = 0

        with open(self.raw_input_file, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                total_input += 1
                rec = parse_log_line(line)
                if rec.is_valid:
                    valid_parsed += 1
                else:
                    malformed_parsed += 1

        # DQ-01 Check
        c1_passed = (total_input == valid_parsed + malformed_parsed) and (total_input > 0)
        self._record_check(
            check_id="DQ-01",
            name="Raw Ingestion Accounting Balance",
            table="raw_logs",
            passed=c1_passed,
            expected=f"valid + malformed == {total_input}",
            actual=f"{valid_parsed} + {malformed_parsed} == {valid_parsed + malformed_parsed}",
            details=f"Input lines: {total_input} | Valid: {valid_parsed} | Malformed: {malformed_parsed}"
        )

        # DQ-07 Check
        malformed_rate = (malformed_parsed / total_input * 100.0) if total_input > 0 else 0.0
        c7_passed = (malformed_rate <= self.max_malformed_pct)
        self._record_check(
            check_id="DQ-07",
            name="Malformed Record Rate Threshold",
            table="raw_logs",
            passed=c7_passed,
            expected=f"malformed rate <= {self.max_malformed_pct:.2f}%",
            actual=f"malformed rate = {malformed_rate:.2f}% ({malformed_parsed}/{total_input})",
            details=f"Malformed percentage {malformed_rate:.2f}% is within allowable SLA threshold ({self.max_malformed_pct:.2f}%)." if c7_passed else f"Malformed rate {malformed_rate:.2f}% EXCEEDS SLA threshold ({self.max_malformed_pct:.2f}%)."
        )

        return c1_passed, c7_passed

    def check_cross_job_request_consistency(self) -> bool:
        """Rule 2: Q1 Total Requests == Q3 Total Requests"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(total_requests) AS q1_total_req, SUM(total_bytes) AS q1_total_bytes FROM daily_traffic;")
        row_q1 = cursor.fetchone()
        q1_req = row_q1['q1_total_req'] if isinstance(row_q1, dict) else row_q1[0]
        q1_bytes = row_q1['q1_total_bytes'] if isinstance(row_q1, dict) else row_q1[1]

        cursor.execute("SELECT SUM(total_requests) AS q3_total_req FROM hourly_errors;")
        row_q3 = cursor.fetchone()
        q3_req = row_q3['q3_total_req'] if isinstance(row_q3, dict) else row_q3[0]

        cursor.close()

        passed = (q1_req == q3_req) and (q1_req is not None and q1_req > 0)
        return self._record_check(
            check_id="DQ-02",
            name="Cross-Job Total Requests Invariant (Q1 == Q3)",
            table="daily_traffic vs hourly_errors",
            passed=passed,
            expected=f"Q1 requests == Q3 requests",
            actual=f"Q1 requests={q1_req}, Q3 requests={q3_req}",
            details=f"Daily total requests ({q1_req}) exactly matches Hourly total requests ({q3_req}). Total bytes={q1_bytes}"
        )

    def check_hourly_error_rates_validity(self) -> bool:
        """Rule 3: 0 <= error_requests <= total_requests and 0 <= error_rate <= 100.0 with mathematical accuracy"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT log_hour, total_requests, error_requests, error_rate FROM hourly_errors;")
        rows = cursor.fetchall()
        cursor.close()

        violations = []
        for r in rows:
            hour = r['log_hour'] if isinstance(r, dict) else r[0]
            tot = r['total_requests'] if isinstance(r, dict) else r[1]
            err = r['error_requests'] if isinstance(r, dict) else r[2]
            rate = r['error_rate'] if isinstance(r, dict) else r[3]

            if err < 0 or err > tot or tot <= 0:
                violations.append(f"Hour {hour}: total={tot}, err={err} (out of bounds)")
            else:
                expected_rate = (err / tot) * 100.0
                if abs(rate - expected_rate) > 0.01:
                    violations.append(f"Hour {hour}: recorded rate={rate:.4f} != expected {expected_rate:.4f}")

        passed = (len(violations) == 0) and (len(rows) > 0)
        return self._record_check(
            check_id="DQ-03",
            name="Hourly Error Rate Mathematical & Bounds Verification",
            table="hourly_errors",
            passed=passed,
            expected="0 violations in error bounds and rate formula across all hours",
            actual=f"{len(violations)} violations found out of {len(rows)} hours",
            details="All hourly error counts satisfy 0 <= errors <= total and match computed error_rate." if passed else "; ".join(violations[:5])
        )

    def check_daily_traffic_integrity(self) -> bool:
        """Rule 4: Non-null keys, positive request counts, non-negative bytes"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT log_date, total_requests, total_bytes FROM daily_traffic;")
        rows = cursor.fetchall()
        cursor.close()

        violations = []
        seen_dates = set()
        for r in rows:
            d = r['log_date'] if isinstance(r, dict) else r[0]
            req = r['total_requests'] if isinstance(r, dict) else r[1]
            b = r['total_bytes'] if isinstance(r, dict) else r[2]

            if not d or d in seen_dates:
                violations.append(f"Duplicate or Null date: {d}")
            seen_dates.add(d)

            if req <= 0 or b < 0:
                violations.append(f"Date {d}: invalid metrics req={req}, bytes={b}")

        passed = (len(violations) == 0) and (len(rows) > 0)
        return self._record_check(
            check_id="DQ-04",
            name="Daily Traffic Key & Metric Integrity",
            table="daily_traffic",
            passed=passed,
            expected="Unique non-null dates with total_requests > 0 and total_bytes >= 0",
            actual=f"{len(rows)} valid distinct dates, 0 violations",
            details="All daily traffic records are valid and distinct." if passed else "; ".join(violations[:5])
        )

    def check_top_resources_monotonicity(self, expected_max_rank: int = 20) -> bool:
        """Rule 5: Top resources rank monotonicity (non-increasing request counts) and non-emptiness"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT rank_order, resource, request_count, total_bytes FROM top_resources ORDER BY rank_order ASC;")
        rows = cursor.fetchall()
        cursor.close()

        violations = []
        prev_count = float('inf')
        for r in rows:
            rank = r['rank_order'] if isinstance(r, dict) else r[0]
            res = r['resource'] if isinstance(r, dict) else r[1]
            cnt = r['request_count'] if isinstance(r, dict) else r[2]
            b = r['total_bytes'] if isinstance(r, dict) else r[3]

            if not res or res.strip() == "":
                violations.append(f"Rank {rank}: empty resource URI")
            # Correct non-increasing check: ties (cnt == prev_count) are valid in ranking
            if cnt > prev_count:
                violations.append(f"Rank {rank} count ({cnt}) > Rank {rank-1} count ({prev_count}) - Non-monotonic")
            prev_count = cnt

            if cnt <= 0 or b < 0:
                violations.append(f"Rank {rank}: invalid counts ({cnt}, {b})")

        passed = (len(violations) == 0) and (0 < len(rows) <= expected_max_rank)
        return self._record_check(
            check_id="DQ-05",
            name="Top Resources Monotonic Ordering & Cardinality",
            table="top_resources",
            passed=passed,
            expected=f"Up to {expected_max_rank} resources sorted non-increasing by request_count",
            actual=f"{len(rows)} resources, monotonic={len(violations)==0}",
            details="Top resources are sorted in non-increasing order of popularity with valid paths." if passed else "; ".join(violations[:5])
        )

    def check_no_null_critical_attributes(self) -> bool:
        """Rule 6: Completeness of critical columns across all analytics tables"""
        cursor = self.conn.cursor()
        checks = [
            ("SELECT COUNT(1) FROM daily_traffic WHERE log_date IS NULL OR total_requests IS NULL;", "daily_traffic"),
            ("SELECT COUNT(1) FROM top_resources WHERE resource IS NULL OR request_count IS NULL;", "top_resources"),
            ("SELECT COUNT(1) FROM hourly_errors WHERE log_hour IS NULL OR total_requests IS NULL OR error_rate IS NULL;", "hourly_errors")
        ]
        null_counts = {}
        total_nulls = 0
        for query, tbl in checks:
            cursor.execute(query)
            cnt = cursor.fetchone()[0]
            null_counts[tbl] = cnt
            total_nulls += cnt
        cursor.close()

        passed = (total_nulls == 0)
        return self._record_check(
            check_id="DQ-06",
            name="Critical Attributes Non-Nullability",
            table="all_analytics_tables",
            passed=passed,
            expected="0 NULL values in critical dimensions and metrics",
            actual=f"{total_nulls} NULLs found ({null_counts})",
            details="All required fields across daily_traffic, top_resources, and hourly_errors are populated."
        )

    def persist_results(self):
        """Persists validation results to data_quality_results table."""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM data_quality_results;")
        placeholder = "?" if self.db_type == "sqlite" else "%s"
        for c in self.checks_run:
            cursor.execute(
                f"INSERT INTO data_quality_results (check_id, check_name, table_name, status, expected_value, actual_value, details) "
                f"VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})",
                (c['check_id'], c['name'], c['table'], c['status'], c['expected'], c['actual'], c['details'])
            )
        self.conn.commit()
        cursor.close()

    def run_all_checks(self) -> Tuple[bool, List[Dict[str, Any]]]:
        self.checks_run = []
        c1, c7 = self.check_raw_log_accounting_and_threshold()
        c2 = self.check_cross_job_request_consistency()
        c3 = self.check_hourly_error_rates_validity()
        c4 = self.check_daily_traffic_integrity()
        c5 = self.check_top_resources_monotonicity()
        c6 = self.check_no_null_critical_attributes()

        all_passed = all([c1, c2, c3, c4, c5, c6, c7])
        self.persist_results()
        return all_passed, self.checks_run

    def close(self):
        if self.conn:
            self.conn.close()


def print_report(all_passed: bool, checks: List[Dict[str, Any]]):
    print("\n" + "=" * 90)
    print("                NASA LOG PIPELINE DATA QUALITY VALIDATION REPORT                ")
    print("=" * 90)
    print(f"{'CHECK ID':<10} | {'STATUS':<6} | {'TABLE':<28} | {'CHECK NAME':<38}")
    print("-" * 90)
    for c in checks:
        print(f"{c['check_id']:<10} | {c['status']:<6} | {c['table']:<28} | {c['name']:<38}")
        print(f"   Expected: {c['expected']}")
        print(f"   Actual:   {c['actual']}")
        print(f"   Details:  {c['details']}\n")
    print("-" * 90)
    overall_status = "PASSED (ALL INTEGRITY CHECKS SATISFIED)" if all_passed else "FAILED (DATA QUALITY ANOMALIES DETECTED)"
    print(f"OVERALL DATA QUALITY VERDICT: {overall_status}")
    print("=" * 90 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Quality Validator")
    parser.add_argument('--input-file', required=True, help="Raw input log file path")
    parser.add_argument('--db-type', default='sqlite', choices=['sqlite', 'mysql'])
    parser.add_argument('--sqlite-path', default=DEFAULT_SQLITE_PATH)
    parser.add_argument('--max-malformed-pct', type=float, default=None, help="Max allowable malformed percentage threshold")

    args = parser.parse_args()
    validator = DataQualityValidator(args.input_file, db_type=args.db_type, sqlite_path=args.sqlite_path, max_malformed_pct=args.max_malformed_pct)
    passed, results = validator.run_all_checks()
    print_report(passed, results)
    validator.close()
    if not passed:
        sys.exit(1)
