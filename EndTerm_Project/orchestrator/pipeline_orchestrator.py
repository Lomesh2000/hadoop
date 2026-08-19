#!/usr/bin/env python3
"""
Master NASA Log Analytics Pipeline Orchestrator
Coordinates the end-to-end data pipeline:
1. Ingestion & Raw Input Verification
2. MapReduce ETL Processing (Q1 Daily, Q2 Global Top-N, Q3 Hourly Errors)
   - Local Mode: Python Streaming with sort & pipe checks
   - Hadoop Mode: Uploads raw log to HDFS, executes Hadoop Streaming with Q2 single-reducer constraint,
     and retrieves HDFS outputs for downstream DB ingestion.
3. Relational Database Loading (MySQL / SQLite) with atomic multi-table transaction safety
4. Automated Data Quality Gatekeeper (Halts pipeline upon assertion or SLA threshold failure)
5. Analytical & Visual Reporting Generation (Propagates failure if report generation fails)
6. Telemetry and Pipeline Metadata Audit Logging
"""

import sys
import os
import time
import uuid
import argparse
from typing import Dict, Any

# Ensure submodules are discoverable
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, 'config'))
sys.path.insert(0, os.path.join(BASE_DIR, 'mapreduce_python'))
sys.path.insert(0, os.path.join(BASE_DIR, 'database'))
sys.path.insert(0, os.path.join(BASE_DIR, 'validation'))
sys.path.insert(0, os.path.join(BASE_DIR, 'reporting'))

from config_loader import load_config
from mr_runner import run_local_streaming, run_hadoop_streaming, upload_to_hdfs, fetch_from_hdfs, JOBS
from db_loader import DatabaseManager, load_all_results
from data_quality_validator import DataQualityValidator, print_report
from reporter import AnalyticsReporter
from log_parser import parse_log_line


class PipelineOrchestrator:
    def __init__(self, input_log: str, output_dir: str, mode: str = None, db_type: str = None, top_n: int = None, config_path: str = None, **db_kwargs):
        # Load configuration file
        self.cfg = load_config(config_path)
        
        self.input_log = os.path.abspath(input_log)
        self.output_dir = os.path.abspath(output_dir)
        self.mode = (mode or 'local').lower()
        self.db_type = (db_type or self.cfg['database']['backend']).lower()
        self.top_n = top_n or self.cfg['pipeline']['top_n_resources']
        self.db_kwargs = db_kwargs
        
        if 'sqlite_path' not in self.db_kwargs and self.db_type == 'sqlite':
            self.db_kwargs['sqlite_path'] = self.cfg['database']['sqlite']['path']
            
        self.execution_id = str(uuid.uuid4())[:8]
        self.telemetry = {
            'execution_id': self.execution_id,
            'stages': {},
            'start_time': time.time(),
            'total_duration': 0.0,
            'status': 'PENDING'
        }

    def _log_stage(self, stage_name: str, status: str, duration: float, metadata: Dict[str, Any] = None):
        self.telemetry['stages'][stage_name] = {
            'status': status,
            'duration_sec': round(duration, 3),
            'metadata': metadata or {}
        }

    def run_stage_1_ingestion(self) -> Dict[str, int]:
        print("\n" + "=" * 80)
        print(f"STAGE 1: RAW LOG INGESTION & AUDIT [Execution ID: {self.execution_id}]")
        print("=" * 80)
        t0 = time.time()

        if not os.path.exists(self.input_log):
            raise FileNotFoundError(f"Input log file not found: {self.input_log}")

        total_lines = 0
        valid_lines = 0
        malformed_lines = 0

        with open(self.input_log, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                total_lines += 1
                rec = parse_log_line(line)
                if rec.is_valid:
                    valid_lines += 1
                else:
                    malformed_lines += 1

        elapsed = time.time() - t0
        print(f"Total Ingested Records: {total_lines:,}")
        print(f"Valid Structured Lines: {valid_lines:,} ({valid_lines/total_lines*100:.2f}%)")
        print(f"Malformed / Skipped:    {malformed_lines:,} ({malformed_lines/total_lines*100:.2f}%)")
        print(f"Stage 1 Duration:       {elapsed:.3f}s")

        self._log_stage('ingestion', 'SUCCESS', elapsed, {
            'total': total_lines, 'valid': valid_lines, 'malformed': malformed_lines
        })
        return {'total': total_lines, 'valid': valid_lines, 'malformed': malformed_lines}

    def run_stage_2_mapreduce(self) -> bool:
        print("\n" + "=" * 80)
        print(f"STAGE 2: DISTRIBUTED MAPREDUCE COMPUTATION [{self.mode.upper()} MODE]")
        print("=" * 80)
        t0 = time.time()

        mr_out_dir = os.path.join(self.output_dir, 'output_mr')
        os.makedirs(mr_out_dir, exist_ok=True)

        if self.mode == 'hadoop':
            # Hadoop Cluster Workflow:
            # 1. Upload raw log to HDFS
            hdfs_in_dir = self.cfg['hadoop']['hdfs_input_dir']
            hdfs_out_dir = self.cfg['hadoop']['hdfs_output_dir']
            hdfs_input_file = f"{hdfs_in_dir.rstrip('/')}/raw_logs_{self.execution_id}.log"

            print(f"--> [Hadoop Mode] Staging raw log in HDFS: {hdfs_input_file} ...")
            if not upload_to_hdfs(self.input_log, hdfs_input_file):
                print("[FATAL] Failed to upload input log to HDFS. Aborting Hadoop MapReduce.")
                self._log_stage('mapreduce', 'FAILED', time.time() - t0, {'error': 'HDFS upload failure'})
                return False

            # 2. Execute Jobs on Hadoop Cluster
            for job_key in ['q1', 'q2', 'q3']:
                job_name = JOBS[job_key]['name']
                num_red = self.cfg['hadoop'].get(f'num_reducers_{job_key}', 1)
                print(f"\n--> Submitting Hadoop Job {job_key.upper()}: {job_name} (Reducers: {1 if job_key == 'q2' else num_red}) ...")
                res = run_hadoop_streaming(job_key, hdfs_input_file, hdfs_out_dir, self.top_n, num_red)

                if not res['success']:
                    print(f"[FATAL] Hadoop Job {job_key.upper()} failed: {res.get('error')}")
                    self._log_stage('mapreduce', 'FAILED', time.time() - t0, {'failed_job': job_key, 'error': res.get('error')})
                    return False

                # 3. Retrieve HDFS output part-files to local output directory for DB loader
                local_job_out = os.path.join(mr_out_dir, JOBS[job_key]['output_subdir'], 'part-00000')
                print(f"    Retrieving HDFS output from {res['output_path']} -> {local_job_out} ...")
                if not fetch_from_hdfs(res['output_path'], local_job_out):
                    print(f"[FATAL] Failed to fetch HDFS output for job {job_key.upper()}")
                    return False
                print(f"    Status: SUCCESS | Time: {res['duration_sec']}s")

        else:
            # Local Streaming Pipe Workflow:
            for job_key in ['q1', 'q2', 'q3']:
                job_name = JOBS[job_key]['name']
                print(f"\n--> Executing Local Job {job_key.upper()}: {job_name} ...")
                res = run_local_streaming(job_key, self.input_log, mr_out_dir, self.top_n)

                if not res['success']:
                    print(f"[FATAL] Local Job {job_key.upper()} failed: {res.get('error')}")
                    self._log_stage('mapreduce', 'FAILED', time.time() - t0, {'failed_job': job_key, 'error': res.get('error')})
                    return False

                print(f"    Status: SUCCESS | Time: {res['duration_sec']}s | Records: {res.get('record_count', 'N/A')}")

        elapsed = time.time() - t0
        print(f"\nStage 2 Total Duration: {elapsed:.3f}s")
        self._log_stage('mapreduce', 'SUCCESS', elapsed, {'output_dir': mr_out_dir})
        return True

    def run_stage_3_db_load(self) -> bool:
        print("\n" + "=" * 80)
        print(f"STAGE 3: RELATIONAL DATABASE LOADING [{self.db_type.upper()} - ATOMIC TRANSACTION]")
        print("=" * 80)
        t0 = time.time()

        mr_out_dir = os.path.join(self.output_dir, 'output_mr')
        schema_path = os.path.join(BASE_DIR, 'database', 'schema.sql')

        try:
            counts = load_all_results(mr_out_dir, schema_path, db_type=self.db_type, **self.db_kwargs)
            elapsed = time.time() - t0
            print(f"Atomic Transaction Committed Successfully: {counts}")
            print(f"Stage 3 Duration: {elapsed:.3f}s")
            self._log_stage('db_load', 'SUCCESS', elapsed, counts)
            return True
        except Exception as e:
            elapsed = time.time() - t0
            print(f"[FATAL] Database loading failed (Rolled Back): {e}")
            self._log_stage('db_load', 'FAILED', elapsed, {'error': str(e)})
            return False

    def run_stage_4_data_quality(self) -> bool:
        print("\n" + "=" * 80)
        print("STAGE 4: AUTOMATED DATA QUALITY GATEKEEPER")
        print("=" * 80)
        t0 = time.time()

        max_mal_pct = self.cfg['data_quality']['max_malformed_percentage']
        validator = DataQualityValidator(self.input_log, db_type=self.db_type, max_malformed_pct=max_mal_pct, **self.db_kwargs)
        all_passed, checks = validator.run_all_checks()
        print_report(all_passed, checks)
        validator.close()

        elapsed = time.time() - t0
        status = 'SUCCESS' if all_passed else 'FAILED'
        self._log_stage('data_quality', status, elapsed, {'passed': all_passed, 'num_checks': len(checks)})
        return all_passed

    def run_stage_5_reporting(self) -> bool:
        print("\n" + "=" * 80)
        print("STAGE 5: ANALYTICS REPORTING & VISUALIZATION")
        print("=" * 80)
        t0 = time.time()

        try:
            reports_dir = os.path.join(self.output_dir, 'reports')
            reporter = AnalyticsReporter(db_type=self.db_type, **self.db_kwargs)
            ascii_dash = reporter.generate_ascii_tables()
            print(ascii_dash)
            reporter.generate_charts(os.path.join(reports_dir, 'charts'))
            reporter.export_csv_summaries(reports_dir)
            reporter.close()

            elapsed = time.time() - t0
            print(f"Visual charts and CSV files generated in: {reports_dir}")
            print(f"Stage 5 Duration: {elapsed:.3f}s")
            self._log_stage('reporting', 'SUCCESS', elapsed, {'reports_dir': reports_dir})
            return True
        except Exception as e:
            elapsed = time.time() - t0
            print(f"[FATAL] Reporting generation failed: {e}")
            self._log_stage('reporting', 'FAILED', elapsed, {'error': str(e)})
            return False

    def execute_pipeline(self) -> bool:
        pipeline_start = time.time()
        print("\n" + "#" * 80)
        print(f"  LAUNCHING NASA LOG ETL & ANALYTICS PIPELINE [RUN ID: {self.execution_id}]")
        print("#" * 80)

        # Stage 1: Ingestion
        ingest_stats = self.run_stage_1_ingestion()

        # Stage 2: MapReduce
        if not self.run_stage_2_mapreduce():
            print("\n[PIPELINE ABORTED] Downstream execution stopped due to MapReduce failure.")
            self.telemetry['status'] = 'FAILED_MAPREDUCE'
            return False

        # Stage 3: Database Load (Atomic Transaction)
        if not self.run_stage_3_db_load():
            print("\n[PIPELINE ABORTED] Downstream execution stopped due to Database Load failure.")
            self.telemetry['status'] = 'FAILED_DB_LOAD'
            return False

        # Stage 4: Data Quality Validation Gate
        if not self.run_stage_4_data_quality():
            print("\n[PIPELINE ABORTED] Pipeline halted by Data Quality Gatekeeper.")
            self.telemetry['status'] = 'FAILED_DQ_VALIDATION'
            return False

        # Stage 5: Reporting (With Explicit Failure Propagation)
        if not self.run_stage_5_reporting():
            print("\n[PIPELINE ABORTED] Downstream execution stopped due to Reporting failure.")
            self.telemetry['status'] = 'FAILED_REPORTING'
            return False

        total_elapsed = time.time() - pipeline_start
        self.telemetry['total_duration'] = round(total_elapsed, 3)
        self.telemetry['status'] = 'COMPLETED_SUCCESSFULLY'

        # Record execution log in DB
        try:
            db = DatabaseManager(db_type=self.db_type, **self.db_kwargs)
            db.log_execution(
                self.execution_id, self.mode,
                ingest_stats['total'], ingest_stats['valid'], ingest_stats['malformed'],
                self.telemetry['status'], total_elapsed
            )
            db.close()
        except Exception as e:
            print(f"[WARN] Failed to write audit log to database: {e}")

        print("\n" + "#" * 80)
        print(f"  PIPELINE EXECUTION {self.execution_id} COMPLETED SUCCESSFULLY IN {total_elapsed:.2f}s")
        print("#" * 80 + "\n")
        return True


def main():
    parser = argparse.ArgumentParser(description="Master NASA Log Pipeline Orchestrator")
    parser.add_argument('--input', default=os.path.join(BASE_DIR, 'data', 'nasa_sample_logs.log'), help="Input raw log file")
    parser.add_argument('--output-dir', default=BASE_DIR, help="Base output directory")
    parser.add_argument('--mode', default='local', choices=['local', 'hadoop'], help="Execution engine mode")
    parser.add_argument('--db-type', default='sqlite', choices=['sqlite', 'mysql'], help="Database backend")
    parser.add_argument('--sqlite-path', default=os.environ.get('SQLITE_PATH', '/tmp/nasalog_analytics.db'))
    parser.add_argument('--top-n', type=int, default=20, help="Top N limit for Q2")
    parser.add_argument('--config', default=None, help="Path to custom config.yaml")

    args = parser.parse_args()
    orchestrator = PipelineOrchestrator(
        input_log=args.input,
        output_dir=args.output_dir,
        mode=args.mode,
        db_type=args.db_type,
        top_n=args.top_n,
        config_path=args.config,
        sqlite_path=args.sqlite_path
    )
    success = orchestrator.execute_pipeline()
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
