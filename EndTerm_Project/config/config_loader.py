#!/usr/bin/env python3
"""
Configuration Loader Module
Reads config.yaml, merges with environment variables and CLI overrides,
and provides a single source of truth for all pipeline components.
"""

import os
import yaml
from typing import Dict, Any

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')

def load_config(config_path: str = None) -> Dict[str, Any]:
    cfg_file = config_path or os.environ.get('PIPELINE_CONFIG_PATH', DEFAULT_CONFIG_PATH)
    config = {
        'pipeline': {
            'name': 'NASA Web Server Log Analytics',
            'version': '2.1.0',
            'top_n_resources': 20
        },
        'database': {
            'backend': 'sqlite',
            'sqlite': {
                'path': os.environ.get('SQLITE_PATH', '/tmp/nasalog_analytics.db')
            },
            'mysql': {
                'host': os.environ.get('DB_HOST', 'localhost'),
                'port': int(os.environ.get('DB_PORT', 3306)),
                'user': os.environ.get('DB_USER', 'root'),
                'database': os.environ.get('DB_NAME', 'nasadb'),
                'password_env_var': 'DB_PASSWORD'
            }
        },
        'hadoop': {
            'streaming_jar': os.environ.get('HADOOP_STREAMING_JAR', ''),
            'hdfs_input_dir': os.environ.get('HDFS_INPUT_DIR', '/data/nasa/input'),
            'hdfs_output_dir': os.environ.get('HDFS_OUTPUT_DIR', '/data/nasa/output'),
            'num_reducers_q1': 1,
            'num_reducers_q2': 1,  # Strictly 1 for single-stage global Top-N
            'num_reducers_q3': 1
        },
        'data_quality': {
            'enforce_strict_gate': True,
            'max_malformed_percentage': float(os.environ.get('MAX_MALFORMED_PCT', 5.0))
        }
    }

    if os.path.exists(cfg_file):
        try:
            with open(cfg_file, 'r', encoding='utf-8') as f:
                yaml_cfg = yaml.safe_load(f)
                if yaml_cfg and isinstance(yaml_cfg, dict):
                    # Deep merge YAML into defaults
                    for section, values in yaml_cfg.items():
                        if section in config and isinstance(values, dict):
                            config[section].update(values)
                        else:
                            config[section] = values
        except Exception as e:
            print(f"[WARN] Failed to parse {cfg_file}: {e}. Using defaults.")

    return config

if __name__ == '__main__':
    cfg = load_config()
    print("Loaded configuration successfully:")
    print(yaml.dump(cfg, default_flow_style=False))
