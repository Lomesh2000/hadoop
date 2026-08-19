#!/usr/bin/env python3
"""
Q2 Mapper: Top Requested Resources
Input: Raw log lines
Output: Key = resource (URI path), Value = "1\t<bytes>"
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from log_parser import parse_log_line

def main():
    for raw_line in sys.stdin.buffer:
        line = raw_line.decode("utf-8", errors="replace")
        rec = parse_log_line(line)
        if rec.is_valid and rec.resource:
            sys.stdout.write(f"{rec.resource}\t1\t{rec.bytes_sent}\n")

if __name__ == "__main__":
    main()
