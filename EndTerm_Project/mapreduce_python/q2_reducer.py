#!/usr/bin/env python3
"""
Q2 Reducer: Top Requested Resources Aggregator & Top-N Selector
Input: Sorted stream of "resource\t1\t<bytes>"
Output: Top N lines of "resource\trequest_count\ttotal_bytes" sorted by request_count DESC
"""

import sys
import os
import heapq

DEFAULT_TOP_N = int(os.environ.get("TOP_N_LIMIT", "20"))

def main():
    top_n = DEFAULT_TOP_N
    # Check if passed via command line argument
    if len(sys.argv) > 1:
        try:
            top_n = int(sys.argv[1])
        except ValueError:
            top_n = DEFAULT_TOP_N

    current_resource = None
    total_requests = 0
    total_bytes = 0

    # Heap will store tuples: (request_count, total_bytes, resource)
    # Using min-heap to retain top N items
    min_heap = []

    def process_entry(res, count, b_sum):
        if len(min_heap) < top_n:
            heapq.heappush(min_heap, (count, b_sum, res))
        else:
            if (count, b_sum) > (min_heap[0][0], min_heap[0][1]):
                heapq.heapreplace(min_heap, (count, b_sum, res))

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) != 3:
            continue

        resource, count_str, bytes_str = parts
        try:
            count = int(count_str)
            bytes_val = int(bytes_str)
        except ValueError:
            continue

        if current_resource == resource:
            total_requests += count
            total_bytes += bytes_val
        else:
            if current_resource is not None:
                process_entry(current_resource, total_requests, total_bytes)
            current_resource = resource
            total_requests = count
            total_bytes = bytes_val

    if current_resource is not None:
        process_entry(current_resource, total_requests, total_bytes)

    # Sort the top N in descending order of count
    top_records = sorted(min_heap, key=lambda x: (x[0], x[1]), reverse=True)

    for count, b_sum, res in top_records:
        sys.stdout.write(f"{res}\t{count}\t{b_sum}\n")

if __name__ == "__main__":
    main()
