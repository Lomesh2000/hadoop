#!/usr/bin/env python3
"""
Q3 Mapper: Hourly Error Rates
Input: Raw log lines
Output: Key = hour (YYYY-MM-DD HH:00:00), Value = "1\t<is_error_flag (1 or 0)>"
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from log_parser import parse_log_line

def main():
    for raw_line in sys.stdin.buffer:
        line = raw_line.decode("utf-8", errors="replace")
        rec = parse_log_line(line)
        if rec.is_valid:
            error_flag = 1 if rec.is_error else 0
            sys.stdout.write(f"{rec.iso_hour}\t1\t{error_flag}\n")

if __name__ == "__main__":
    main()
