#!/usr/bin/env python3
"""
Q3 Reducer: Hourly Error Rates Aggregator
Input: Sorted stream of "hour\t1\t<is_error_flag>"
Output: "hour\ttotal_requests\terror_requests\terror_rate"
"""

import sys

def main():
    current_hour = None
    total_requests = 0
    error_requests = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) != 3:
            continue

        hour, count_str, err_str = parts
        try:
            count = int(count_str)
            err = int(err_str)
        except ValueError:
            continue

        if current_hour == hour:
            total_requests += count
            error_requests += err
        else:
            if current_hour is not None:
                error_rate = (error_requests / total_requests * 100.0) if total_requests > 0 else 0.0
                sys.stdout.write(f"{current_hour}\t{total_requests}\t{error_requests}\t{error_rate:.4f}\n")
            current_hour = hour
            total_requests = count
            error_requests = err

    if current_hour is not None:
        error_rate = (error_requests / total_requests * 100.0) if total_requests > 0 else 0.0
        sys.stdout.write(f"{current_hour}\t{total_requests}\t{error_requests}\t{error_rate:.4f}\n")

if __name__ == "__main__":
    main()
