#!/usr/bin/env python3
"""
Q1 Reducer: Daily Traffic Aggregator
Input: Sorted stream of "date\t1\t<bytes>"
Output: "date\ttotal_requests\ttotal_bytes"
"""

import sys

def main():
    current_date = None
    total_requests = 0
    total_bytes = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) != 3:
            continue

        date, count_str, bytes_str = parts
        try:
            count = int(count_str)
            bytes_val = int(bytes_str)
        except ValueError:
            continue

        if current_date == date:
            total_requests += count
            total_bytes += bytes_val
        else:
            if current_date is not None:
                sys.stdout.write(f"{current_date}\t{total_requests}\t{total_bytes}\n")
            current_date = date
            total_requests = count
            total_bytes = bytes_val

    if current_date is not None:
        sys.stdout.write(f"{current_date}\t{total_requests}\t{total_bytes}\n")

if __name__ == "__main__":
    main()
