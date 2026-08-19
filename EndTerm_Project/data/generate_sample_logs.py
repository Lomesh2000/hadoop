#!/usr/bin/env python3
"""
Synthetic NASA Common Log Format Generator
Generates realistic NASA HTTP server access logs covering July & August 1995,
including normal requests, 304 cache hits, 404 errors, 500 server errors,
and a fixed, trackable count of malformed records for data quality testing.
"""

import sys
import random
import argparse
from datetime import datetime, timedelta

HOSTS = [
    "in24.inetnebr.com", "uplink.hamilton.edu", "d104.aa.net", "199.5.127.18",
    "alyssa.p", "burger.letters.com", "199.120.110.21", "205.189.154.54",
    "pipe1.t2.usa.pipeline.com", "ix-sac6-20.ix.netcom.com", "spruce.it.monash.edu.au",
    "news.ksc.nasa.gov", "163.205.1.45", "dialip-38.ots.utexas.edu", "s510.slip.net",
    "gatekeeper.pica.army.mil", "www-d1.proxy.aol.com", "piweba3y.prodigy.com"
]

RESOURCES = [
    ("/ksc.html", 7074, [200, 304]),
    ("/images/NASA-logosmall.gif", 786, [200, 304, 404]),
    ("/images/KSClogo.gif", 1204, [200, 304]),
    ("/images/MOSAIC-logosmall.gif", 363, [200, 304]),
    ("/images/USA-logosmall.gif", 234, [200, 304]),
    ("/images/WORLD-logosmall.gif", 669, [200, 304]),
    ("/images/ksclogo-medium.gif", 5866, [200, 304]),
    ("/shuttle/countdown/", 3985, [200]),
    ("/shuttle/missions/sts-70/mission-sts-70.html", 4029, [200, 404]),
    ("/shuttle/missions/sts-71/mission-sts-71.html", 5120, [200]),
    ("/history/apollo/apollo-13/apollo-13.html", 6320, [200, 404]),
    ("/shuttle/missions/sts-68/news/sts-68-mcc-05.txt", 1839, [200]),
    ("/facilities/lc39a.html", 2530, [200, 404]),
    ("/software/win95/software.zip", 45890, [200, 500]),
    ("/history/history.html", 3120, [200, 404]),
    ("/facts/faq.html", 2980, [200, 404]),
    ("/shuttle/technology/sts-newsref/stsref-toc.html", 7890, [200]),
    ("/images/launch-pad.jpg", 34200, [200, 304]),
    ("/nonexistent-page.html", 0, [404]),
    ("/secret/admin.cgi", 0, [403, 404])
]

MONTH_NAMES = {7: 'Jul', 8: 'Aug'}

def generate_logs(num_records: int, malformed_count: int, output_file: str, seed: int = 42):
    random.seed(seed)
    valid_count = num_records - malformed_count
    start_dt = datetime(1995, 7, 1, 0, 0, 0)
    total_seconds = 14 * 24 * 3600  # Spread across 14 days (Jul 1-7 and Aug 1-7)

    records = []

    for _ in range(valid_count):
        # Pick July (days 1-7) or August (days 1-7)
        month = random.choice([7, 8])
        day = random.randint(1, 7)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        dt_str = f"{day:02d}/{MONTH_NAMES[month]}/1995:{hour:02d}:{minute:02d}:{second:02d} -0400"
        host = random.choice(HOSTS)
        resource, base_bytes, status_choices = random.choice(RESOURCES)
        status = random.choice(status_choices)

        if status == 304:
            byte_str = "-" if random.random() < 0.3 else "0"
        elif status in (403, 404, 500):
            byte_str = "-" if random.random() < 0.5 else str(random.randint(0, 350))
        else:
            # Vary byte size slightly
            byte_str = str(max(10, base_bytes + random.randint(-150, 250)))

        method = "GET" if random.random() < 0.95 else random.choice(["POST", "HEAD"])
        line = f'{host} - - [{dt_str}] "{method} {resource} HTTP/1.0" {status} {byte_str}'
        records.append(line)

    # Add deliberate malformed records
    malformed_templates = [
        "corrupted invalid line with no standard tokens",
        'bad.host.com - - [99/XYZ/1995:25:99:99 -0400] "GET /bad HTTP/1.0" 999 100',
        'broken.record - - [01/Jul/1995:00:00:00 -0400] missing_quotes 200 100',
        'incomplete.record - - [02/Aug/1995:12:00:00 -0400] "GET /foo" not_a_number 100',
        'empty.request - - [03/Aug/1995:14:30:00 -0400] "" 200 0'
    ]

    for i in range(malformed_count):
        records.append(malformed_templates[i % len(malformed_templates)])

    # Shuffle to distribute malformed lines
    random.shuffle(records)

    with open(output_file, 'w', encoding='utf-8') as f:
        for r in records:
            f.write(r + '\n')

    print(f"Generated {num_records} total log records ({valid_count} valid, {malformed_count} malformed) -> {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate NASA Sample Logs")
    parser.add_argument('--records', type=int, default=25000, help="Total records to generate")
    parser.add_argument('--malformed', type=int, default=25, help="Number of malformed records")
    parser.add_argument('--output', default="/working_dir/c_d4adba4239bd4987/NOSQL-main/EndTerm_Project/data/nasa_sample_logs.log")
    parser.add_argument('--seed', type=int, default=42)

    args = parser.parse_args()
    generate_logs(args.records, args.malformed, args.output, args.seed)
