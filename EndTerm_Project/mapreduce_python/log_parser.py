#!/usr/bin/env python3
"""
NASA Web Server Access Log Parser
Parses Common Log Format (CLF) strings into structured LogRecord instances.
Handles corrupted tokens, missing bytes ('-'), non-standard HTTP methods, and invalid timestamps.
"""

import re
from datetime import datetime
from typing import Optional, Dict, Any

MONTH_MAP = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}

# Standard NASA KSC Log Format:
# host - - [DD/Mon/YYYY:HH:mm:ss -0400] "METHOD /path HTTP/1.0" status bytes
LOG_REGEX = re.compile(
    r'^(\S+)\s+'                   # 1: Host / IP
    r'\S+\s+\S+\s+'                # Ident / Auth (usually "- -")
    r'\[(\d{2}\/(\w{3})\/\d{4}):(\d{2}):(\d{2}):(\d{2})\s+([^\]]+)\]\s+'  # 2: Date, 3: Month, 4: Hour, 5: Min, 6: Sec, 7: Timezone
    r'"([^"]*)"\s+'                # 8: Request line ("METHOD /endpoint HTTP/x.x")
    r'(\d{3})\s+'                  # 9: HTTP Status code
    r'(\S+)$'                      # 10: Bytes sent (integer or "-")
)

REQUEST_REGEX = re.compile(r'^([A-Z]+)\s+(\S+)(?:\s+(HTTP\/\d\.\d))?.*$')


class LogRecord:
    __slots__ = (
        'host', 'raw_date', 'iso_date', 'iso_hour', 'timestamp',
        'method', 'resource', 'protocol', 'status', 'bytes_sent',
        'is_error', 'is_valid', 'raw_line', 'error_reason'
    )

    def __init__(
        self,
        host: str = "",
        raw_date: str = "",
        iso_date: str = "",
        iso_hour: str = "",
        timestamp: str = "",
        method: str = "",
        resource: str = "",
        protocol: str = "",
        status: int = 0,
        bytes_sent: int = 0,
        is_error: bool = False,
        is_valid: bool = True,
        raw_line: str = "",
        error_reason: str = ""
    ):
        self.host = host
        self.raw_date = raw_date
        self.iso_date = iso_date
        self.iso_hour = iso_hour
        self.timestamp = timestamp
        self.method = method
        self.resource = resource
        self.protocol = protocol
        self.status = status
        self.bytes_sent = bytes_sent
        self.is_error = is_error
        self.is_valid = is_valid
        self.raw_line = raw_line
        self.error_reason = error_reason

    def to_dict(self) -> Dict[str, Any]:
        return {
            'host': self.host,
            'raw_date': self.raw_date,
            'iso_date': self.iso_date,
            'iso_hour': self.iso_hour,
            'timestamp': self.timestamp,
            'method': self.method,
            'resource': self.resource,
            'protocol': self.protocol,
            'status': self.status,
            'bytes_sent': self.bytes_sent,
            'is_error': self.is_error,
            'is_valid': self.is_valid,
            'error_reason': self.error_reason
        }

    def __repr__(self) -> str:
        if not self.is_valid:
            return f"<LogRecord INVALID reason='{self.error_reason}'>"
        return (
            f"<LogRecord date='{self.iso_date}' hour='{self.iso_hour}' "
            f"method='{self.method}' resource='{self.resource}' "
            f"status={self.status} bytes={self.bytes_sent}>"
        )


def parse_log_line(line: str) -> LogRecord:
    """
    Parses a single raw NASA log line into a LogRecord.
    Returns an invalid LogRecord on failure without throwing unhandled exceptions.
    """
    line = line.rstrip('\r\n')
    if not line:
        return LogRecord(is_valid=False, raw_line=line, error_reason="EMPTY_LINE")

    match = LOG_REGEX.match(line)
    if not match:
        return LogRecord(is_valid=False, raw_line=line, error_reason="REGEX_MISMATCH")

    host = match.group(1)
    raw_date = match.group(2)     # DD/Mon/YYYY
    month_name = match.group(3)   # Mon
    hour_val = match.group(4)      # HH
    min_val = match.group(5)       # mm
    sec_val = match.group(6)       # ss
    tz_val = match.group(7)        # timezone e.g. -0400
    request_line = match.group(8)
    status_str = match.group(9)
    bytes_str = match.group(10)

    # Convert date DD/Mon/YYYY -> YYYY-MM-DD
    parts = raw_date.split('/')
    if len(parts) == 3 and month_name in MONTH_MAP:
        day, _, year = parts
        iso_date = f"{year}-{MONTH_MAP[month_name]}-{day.zfill(2)}"
    else:
        return LogRecord(is_valid=False, raw_line=line, error_reason="INVALID_DATE_FORMAT")

    iso_hour = f"{iso_date} {hour_val}:00:00"
    full_timestamp = f"{iso_date} {hour_val}:{min_val}:{sec_val}"

    # Parse request line (METHOD /path HTTP/x.x)
    req_match = REQUEST_REGEX.match(request_line)
    if req_match:
        method = req_match.group(1)
        resource = req_match.group(2)
        protocol = req_match.group(3) or "HTTP/1.0"
    else:
        # Fallback for malformed request lines (e.g. just a raw path or empty string)
        tokens = request_line.split()
        if len(tokens) >= 2:
            method = tokens[0]
            resource = tokens[1]
            protocol = tokens[2] if len(tokens) > 2 else "HTTP/1.0"
        elif len(tokens) == 1:
            method = "GET"
            resource = tokens[0]
            protocol = "HTTP/1.0"
        else:
            method = "UNKNOWN"
            resource = "/unknown"
            protocol = "HTTP/1.0"

    # Status parsing
    try:
        status = int(status_str)
    except ValueError:
        return LogRecord(is_valid=False, raw_line=line, error_reason="INVALID_STATUS_CODE")

    # Bytes parsing (handle '-' as 0 bytes, e.g., for 304 Not Modified or 404 Not Found)
    if bytes_str == '-' or bytes_str == '':
        bytes_sent = 0
    else:
        try:
            bytes_sent = int(bytes_str)
            if bytes_sent < 0:
                bytes_sent = 0
        except ValueError:
            bytes_sent = 0

    is_error = (status >= 400)

    return LogRecord(
        host=host,
        raw_date=raw_date,
        iso_date=iso_date,
        iso_hour=iso_hour,
        timestamp=full_timestamp,
        method=method,
        resource=resource,
        protocol=protocol,
        status=status,
        bytes_sent=bytes_sent,
        is_error=is_error,
        is_valid=True,
        raw_line=line,
        error_reason=""
    )


if __name__ == "__main__":
    sample_lines = [
        'in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839',
        'uplink.hamilton.edu - - [01/Jul/1995:00:00:09 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0',
        'd104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985',
        '199.5.127.18 - - [01/Aug/1995:00:00:07 -0400] "GET /ksc.html" 200 7074',
        'alyssa.p - - [01/Aug/1995:00:00:15 -0400] "GET /history/apollo/apollo-13/apollo-13-info.html HTTP/1.0" 404 -',
        'malformed line without proper formatting',
        ''
    ]
    for s in sample_lines:
        rec = parse_log_line(s)
        print(rec)
