#!/usr/bin/env python3
"""
MapReduce Execution Runner
Supports:
1. Local Streaming Emulation (Python stdin/stdout piped through system sort)
2. Hadoop Streaming (`hadoop jar ... hadoop-streaming.jar`) with explicit HDFS staging and reducer controls

Important Architectural Note for Q2:
For Query 2 (Top-N Most Requested Resources), the number of reducers MUST be set to 1
(`-numReduceTasks 1` or `-D mapreduce.job.reduces=1`) to guarantee a single-stage GLOBAL Top-N.
If multiple reducers were used without a secondary global merge stage, each reducer would only
output its partition-local Top-N.
"""

import sys
import os
import time
import subprocess
import argparse
from typing import Dict, Any, List

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

JOBS = {
    'q1': {
        'name': 'Daily Traffic Analysis',
        'mapper': os.path.join(BASE_DIR, 'q1_mapper.py'),
        'reducer': os.path.join(BASE_DIR, 'q1_reducer.py'),
        'output_subdir': 'q1_daily_traffic',
        'default_reducers': 1
    },
    'q2': {
        'name': 'Top Requested Resources (Global Top-N)',
        'mapper': os.path.join(BASE_DIR, 'q2_mapper.py'),
        'reducer': os.path.join(BASE_DIR, 'q2_reducer.py'),
        'output_subdir': 'q2_top_resources',
        'default_reducers': 1  # Strictly 1 for single-stage global Top-N
    },
    'q3': {
        'name': 'Hourly Error Rates',
        'mapper': os.path.join(BASE_DIR, 'q3_mapper.py'),
        'reducer': os.path.join(BASE_DIR, 'q3_reducer.py'),
        'output_subdir': 'q3_hourly_errors',
        'default_reducers': 1
    }
}


def find_hadoop_streaming_jar() -> str:
    """Searches common Hadoop installation paths for hadoop-streaming.jar."""
    hadoop_home = os.environ.get('HADOOP_HOME', '/usr/local/hadoop')
    search_dirs = [
        os.path.join(hadoop_home, 'share/hadoop/tools/lib'),
        os.path.join(hadoop_home, 'contrib/streaming'),
        '/usr/lib/hadoop/tools/lib',
        '/opt/hadoop/share/hadoop/tools/lib'
    ]
    for d in search_dirs:
        if os.path.exists(d):
            for fname in os.listdir(d):
                if fname.startswith('hadoop-streaming') and fname.endswith('.jar'):
                    return os.path.join(d, fname)
    return ""


def run_local_streaming(job_key: str, input_path: str, output_dir: str, top_n: int = 20) -> Dict[str, Any]:
    """
    Runs MapReduce job locally using Python subprocess pipes and sort.
    Pipeline: cat input | python3 mapper.py | sort | python3 reducer.py > output
    Checks return codes of mapper, sort, and reducer.
    """
    job_cfg = JOBS[job_key]
    job_out_dir = os.path.join(output_dir, job_cfg['output_subdir'])
    os.makedirs(job_out_dir, exist_ok=True)
    out_file_path = os.path.join(job_out_dir, 'part-00000')

    mapper_path = job_cfg['mapper']
    reducer_path = job_cfg['reducer']

    start_time = time.time()

    env = os.environ.copy()
    env['TOP_N_LIMIT'] = str(top_n)
    env['PYTHONPATH'] = BASE_DIR

    cmd_reducer = [sys.executable, reducer_path]
    if job_key == 'q2':
        cmd_reducer.append(str(top_n))

    try:
        with open(input_path, 'rb') as f_in:
            p_map = subprocess.Popen(
                [sys.executable, mapper_path],
                stdin=f_in,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )
            p_sort = subprocess.Popen(
                ['sort'],
                stdin=p_map.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            p_map.stdout.close()

            with open(out_file_path, 'w', encoding='utf-8') as f_out:
                p_red = subprocess.Popen(
                    cmd_reducer,
                    stdin=p_sort.stdout,
                    stdout=f_out,
                    stderr=subprocess.PIPE,
                    env=env
                )
                p_sort.stdout.close()

                _, red_err = p_red.communicate()

            _, map_err = p_map.communicate()
            _, sort_err = p_sort.communicate()

        elapsed = time.time() - start_time

        # Explicitly check mapper, sort, and reducer return codes
        if p_map.returncode != 0:
            return {
                'job': job_key,
                'name': job_cfg['name'],
                'success': False,
                'error': f"Mapper failed (exit {p_map.returncode}): {map_err.decode().strip()}",
                'duration_sec': elapsed,
                'output_file': out_file_path,
                'record_count': 0
            }

        if p_sort.returncode != 0:
            return {
                'job': job_key,
                'name': job_cfg['name'],
                'success': False,
                'error': f"Sort failed (exit {p_sort.returncode}): {sort_err.decode().strip()}",
                'duration_sec': elapsed,
                'output_file': out_file_path,
                'record_count': 0
            }

        if p_red.returncode != 0:
            return {
                'job': job_key,
                'name': job_cfg['name'],
                'success': False,
                'error': f"Reducer failed (exit {p_red.returncode}): {red_err.decode().strip()}",
                'duration_sec': elapsed,
                'output_file': out_file_path,
                'record_count': 0
            }

        # Count output records
        with open(out_file_path, 'r', encoding='utf-8') as f:
            rec_count = sum(1 for line in f if line.strip())

        return {
            'job': job_key,
            'name': job_cfg['name'],
            'success': True,
            'error': None,
            'duration_sec': round(elapsed, 3),
            'output_file': out_file_path,
            'record_count': rec_count
        }

    except Exception as e:
        elapsed = time.time() - start_time
        return {
            'job': job_key,
            'name': job_cfg['name'],
            'success': False,
            'error': str(e),
            'duration_sec': round(elapsed, 3),
            'output_file': out_file_path,
            'record_count': 0
        }


def upload_to_hdfs(local_file: str, hdfs_target_path: str) -> bool:
    """Uploads a local log file to HDFS using 'hdfs dfs -put -f'."""
    # Ensure target parent dir exists
    hdfs_dir = os.path.dirname(hdfs_target_path)
    res_mkdir = subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir], capture_output=True, text=True)
    if res_mkdir.returncode != 0:
        print(f"[ERROR] Failed to create HDFS directory {hdfs_dir}: {res_mkdir.stderr}")
        return False

    res_put = subprocess.run(['hdfs', 'dfs', '-put', '-f', local_file, hdfs_target_path], capture_output=True, text=True)
    if res_put.returncode != 0:
        print(f"[ERROR] Failed to upload {local_file} to HDFS {hdfs_target_path}: {res_put.stderr}")
        return False

    return True


def fetch_from_hdfs(hdfs_source_dir: str, local_target_file: str) -> bool:
    """Fetches output part file from HDFS to local file using 'hdfs dfs -getmerge' or '-get'."""
    os.makedirs(os.path.dirname(local_target_file), exist_ok=True)
    # Remove local target if already exists
    if os.path.exists(local_target_file):
        os.remove(local_target_file)

    res_get = subprocess.run(['hdfs', 'dfs', '-getmerge', hdfs_source_dir, local_target_file], capture_output=True, text=True)
    if res_get.returncode != 0:
        # Fallback to copying part-00000
        res_get_part = subprocess.run(['hdfs', 'dfs', '-get', '-f', f"{hdfs_source_dir}/part-*", local_target_file], capture_output=True, text=True)
        if res_get_part.returncode != 0:
            print(f"[ERROR] Failed to fetch HDFS output from {hdfs_source_dir}: {res_get.stderr}")
            return False

    return True


def run_hadoop_streaming(job_key: str, hdfs_input: str, hdfs_output: str, top_n: int = 20, num_reducers: int = None) -> Dict[str, Any]:
    """
    Submits Hadoop Streaming job to a live Hadoop cluster using HDFS paths.
    Packages Python scripts via -files and enforces 1 reducer for Q2 global Top-N.
    """
    jar_path = find_hadoop_streaming_jar()
    if not jar_path:
        return {
            'job': job_key,
            'name': JOBS[job_key]['name'],
            'success': False,
            'error': "hadoop-streaming.jar not found on system paths. (Hadoop Cluster Execution: NOT VERIFIED in local sandbox environment)",
            'duration_sec': 0
        }

    job_cfg = JOBS[job_key]
    job_hdfs_out = f"{hdfs_output.rstrip('/')}/{job_cfg['output_subdir']}"

    # For Q2, enforce strictly 1 reducer for global Top-N
    if job_key == 'q2':
        actual_reducers = 1
    else:
        actual_reducers = num_reducers or job_cfg['default_reducers']

    # Remove existing HDFS output dir if exists
    subprocess.run(['hdfs', 'dfs', '-rm', '-r', '-f', job_hdfs_out], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    parser_path = os.path.join(BASE_DIR, 'log_parser.py')
    mapper_path = job_cfg['mapper']
    reducer_path = job_cfg['reducer']

    # Package all necessary files so DataNodes have access to log_parser.py
    files_to_ship = f"{mapper_path},{reducer_path},{parser_path}"

    cmd = [
        'hadoop', 'jar', jar_path,
        '-D', f"mapreduce.job.reduces={actual_reducers}",
        '-files', files_to_ship,
        '-mapper', f"python3 {os.path.basename(mapper_path)}",
        '-reducer', f"python3 {os.path.basename(reducer_path)}",
        '-input', hdfs_input,
        '-output', job_hdfs_out,
        '-cmdenv', f"TOP_N_LIMIT={top_n}"
    ]

    start_time = time.time()
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    elapsed = time.time() - start_time

    if res.returncode == 0:
        return {
            'job': job_key,
            'name': job_cfg['name'],
            'success': True,
            'error': None,
            'duration_sec': round(elapsed, 3),
            'output_path': job_hdfs_out,
            'num_reducers': actual_reducers
        }
    else:
        return {
            'job': job_key,
            'name': job_cfg['name'],
            'success': False,
            'error': res.stderr.strip(),
            'duration_sec': round(elapsed, 3),
            'output_path': job_hdfs_out,
            'num_reducers': actual_reducers
        }


def main():
    parser = argparse.ArgumentParser(description="NASA Log MapReduce Runner")
    parser.add_argument('--job', choices=['q1', 'q2', 'q3', 'all'], default='all', help="Job to run")
    parser.add_argument('--mode', choices=['local', 'hadoop'], default='local', help="Execution engine mode")
    parser.add_argument('--input', required=True, help="Input log file path or HDFS path")
    parser.add_argument('--output', required=True, help="Output directory path or HDFS path")
    parser.add_argument('--top-n', type=int, default=20, help="Top N limit for Q2")
    parser.add_argument('--num-reducers', type=int, default=None, help="Number of reducers (Q2 is forced to 1)")

    args = parser.parse_args()

    jobs_to_run = ['q1', 'q2', 'q3'] if args.job == 'all' else [args.job]
    results = []

    print(f"=== Starting NASA Log MapReduce Pipeline [{args.mode.upper()} MODE] ===")
    print(f"Input: {args.input}")
    print(f"Output: {args.output}")

    for j in jobs_to_run:
        print(f"\n--- Running Job {j.upper()}: {JOBS[j]['name']} ---")
        if args.mode == 'local':
            res = run_local_streaming(j, args.input, args.output, args.top_n)
        else:
            res = run_hadoop_streaming(j, args.input, args.output, args.top_n, args.num_reducers)

        results.append(res)
        if res['success']:
            print(f"Status: SUCCESS | Duration: {res['duration_sec']}s | Records: {res.get('record_count', 'N/A')}")
            if 'output_file' in res:
                print(f"Output: {res['output_file']}")
        else:
            print(f"Status: FAILED | Error: {res['error']}")
            print("Aborting downstream jobs due to stage failure.")
            sys.exit(1)

    print("\n=== All MapReduce Jobs Completed Successfully ===")

if __name__ == "__main__":
    main()
