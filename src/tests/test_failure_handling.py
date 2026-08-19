#!/usr/bin/env python3
"""
Unit & Integration Test Suite for Pipeline Failure Handling & Boundary Conditions
Verifies:
1. Resilient parsing of standard lines, hyphen bytes, error codes, and malformed records.
2. Immediate clean abort on non-existent input files.
3. Injected data quality violations (Q1 != Q3, Top-N monotonicity) trigger a HARD FAIL in the Data Quality Gatekeeper.
4. DQ-07 triggers a HARD FAIL when malformed percentage exceeds the SLA threshold.
5. Atomic DB loading transaction safety and missing output file guards.
"""

import sys
import os
import unittest
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, 'mapreduce_python'))
sys.path.insert(0, os.path.join(BASE_DIR, 'database'))
sys.path.insert(0, os.path.join(BASE_DIR, 'validation'))
sys.path.insert(0, os.path.join(BASE_DIR, 'orchestrator'))

from log_parser import parse_log_line
from data_quality_validator import DataQualityValidator
from pipeline_orchestrator import PipelineOrchestrator
from db_loader import DatabaseManager, load_all_results

class TestPipelineFailureHandling(unittest.TestCase):

    def test_01_parser_edge_cases(self):
        """Test parser robustness against diverse malformed lines and standard edge cases."""
        # Valid standard
        rec1 = parse_log_line('host.test.com - - [01/Jul/1995:10:20:30 -0400] "GET /index.html HTTP/1.0" 200 1234')
        self.assertTrue(rec1.is_valid)
        self.assertEqual(rec1.bytes_sent, 1234)
        self.assertEqual(rec1.iso_date, '1995-07-01')
        self.assertEqual(rec1.iso_hour, '1995-07-01 10:00:00')

        # Hyphen bytes (e.g. 304 or 404)
        rec2 = parse_log_line('host.test.com - - [01/Jul/1995:10:20:30 -0400] "GET /image.gif HTTP/1.0" 304 -')
        self.assertTrue(rec2.is_valid)
        self.assertEqual(rec2.bytes_sent, 0)
        self.assertFalse(rec2.is_error)

        # 404 Error
        rec3 = parse_log_line('host.test.com - - [01/Jul/1995:10:20:30 -0400] "GET /missing.html HTTP/1.0" 404 150')
        self.assertTrue(rec3.is_valid)
        self.assertTrue(rec3.is_error)

        # Malformed - empty line
        rec4 = parse_log_line('')
        self.assertFalse(rec4.is_valid)
        self.assertEqual(rec4.error_reason, "EMPTY_LINE")

        # Malformed - gibberish
        rec5 = parse_log_line('corrupted unstructured log payload without formatting')
        self.assertFalse(rec5.is_valid)
        self.assertEqual(rec5.error_reason, "REGEX_MISMATCH")

    def test_02_missing_input_file_handling(self):
        """Pipeline should cleanly fail when input file is missing."""
        orchestrator = PipelineOrchestrator(
            input_log="/nonexistent/path/nasa_missing.log",
            output_dir="/tmp/test_out",
            mode="local",
            db_type="sqlite"
        )
        with self.assertRaises(FileNotFoundError):
            orchestrator.run_stage_1_ingestion()

    def test_03_dq_gatekeeper_catches_injected_error(self):
        """Data Quality Gatekeeper should return False and identify violations when DB data is corrupted."""
        test_db = "/tmp/test_corrupt_dq.db"
        if os.path.exists(test_db):
            os.remove(test_db)

        # Setup schema
        db = DatabaseManager(db_type="sqlite", sqlite_path=test_db)
        schema_path = os.path.join(BASE_DIR, 'database', 'schema.sql')
        db.execute_schema_script(schema_path)

        cursor = db.conn.cursor()
        cursor.execute("INSERT INTO daily_traffic (log_date, total_requests, total_bytes) VALUES ('1995-07-01', 500, 10000);")
        # In Q3 inject 400 requests instead of 500
        cursor.execute("INSERT INTO hourly_errors (log_hour, total_requests, error_requests, error_rate) VALUES ('1995-07-01 00:00:00', 400, 50, 12.5);")
        # Inject non-monotonic top resources (Rank 2 > Rank 1)
        cursor.execute("INSERT INTO top_resources (rank_order, resource, request_count, total_bytes) VALUES (1, '/page1', 10, 100);")
        cursor.execute("INSERT INTO top_resources (rank_order, resource, request_count, total_bytes) VALUES (2, '/page2', 50, 500);")
        db.conn.commit()
        db.close()

        dummy_log = "/tmp/dummy_test.log"
        with open(dummy_log, 'w') as f:
            f.write('host - - [01/Jul/1995:00:00:00 -0400] "GET /page1 HTTP/1.0" 200 100\n')

        validator = DataQualityValidator(raw_input_file=dummy_log, db_type="sqlite", sqlite_path=test_db)
        passed, checks = validator.run_all_checks()
        validator.close()

        self.assertFalse(passed, "DQ Validator should have failed for corrupted dataset!")
        failed_check_ids = [c['check_id'] for c in checks if c['status'] == 'FAIL']
        self.assertIn('DQ-02', failed_check_ids, "DQ-02 (Q1 vs Q3 invariant) should have failed")
        self.assertIn('DQ-05', failed_check_ids, "DQ-05 (Monotonicity) should have failed")

    def test_04_dq_malformed_threshold_enforcement(self):
        """DQ-07 should fail when malformed record percentage exceeds SLA threshold."""
        bad_log = "/tmp/high_malformed.log"
        with open(bad_log, 'w') as f:
            # 1 valid, 9 malformed = 90% malformed (exceeds 5.0% threshold)
            f.write('host - - [01/Jul/1995:00:00:00 -0400] "GET /page1 HTTP/1.0" 200 100\n')
            for _ in range(9):
                f.write('corrupted invalid non-conforming line\n')

        test_db = "/tmp/test_malformed_threshold.db"
        if os.path.exists(test_db):
            os.remove(test_db)
        db = DatabaseManager(db_type="sqlite", sqlite_path=test_db)
        schema_path = os.path.join(BASE_DIR, 'database', 'schema.sql')
        db.execute_schema_script(schema_path)
        db.close()

        validator = DataQualityValidator(raw_input_file=bad_log, db_type="sqlite", sqlite_path=test_db, max_malformed_pct=5.0)
        passed, checks = validator.run_all_checks()
        validator.close()

        self.assertFalse(passed, "DQ Validator must fail when malformed percentage exceeds 5.0%!")
        failed_check_ids = [c['check_id'] for c in checks if c['status'] == 'FAIL']
        self.assertIn('DQ-07', failed_check_ids, "DQ-07 should have failed due to 90% malformed rate")

    def test_05_db_loader_missing_file_guard(self):
        """Database loader must raise FileNotFoundError if any required output is missing."""
        schema_path = os.path.join(BASE_DIR, 'database', 'schema.sql')
        fake_output_dir = "/tmp/incomplete_mr_outputs"
        os.makedirs(fake_output_dir, exist_ok=True)
        # Missing q1, q2, q3 part files
        with self.assertRaises(FileNotFoundError):
            load_all_results(fake_output_dir, schema_path, db_type="sqlite", sqlite_path="/tmp/test_guard.db")

if __name__ == '__main__':
    unittest.main()
