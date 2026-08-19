#!/usr/bin/env python3
"""
Q1 Mapper: Daily Traffic
Input: Raw log lines
Output: Key = date (YYYY-MM-DD), Value = "1\t<bytes>"
"""

import sys
import os

# Ensure local imports work in both streaming and standalone execution
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from log_parser import parse_log_line

def main():
    for raw_line in sys.stdin.buffer:
        line = raw_line.decode("utf-8", errors="replace")
        rec = parse_log_line(line)
        if rec.is_valid:
            # Emit: date \t 1 \t bytes
            sys.stdout.write(f"{rec.iso_date}\t1\t{rec.bytes_sent}\n")

if __name__ == "__main__":
    main()
