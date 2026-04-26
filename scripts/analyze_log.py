#!/usr/bin/env python3
"""
Parse tpms-tracker RESOLVE log lines into structured data.

Usage:
    python3 scripts/analyze_log.py logs/tpms-tracker.log* \
        --fingerprint f4b512b8 \
        --since "2026-04-26 11:00" \
        --stat intervals
"""
import re
import sys
import argparse
import statistics
from datetime import datetime
from pathlib import Path

RESOLVE_RE = re.compile(
    r'(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)'
    r' \| RESOLVE \|'
    r'.*?matched=(?P<matched>\S+)'
    r'.*?pressure=(?P<pressure>[\d.]+)'
)


def parse_resolve_lines(paths):
    rows = []
    for path in paths:
        with open(path) as f:
            for line in f:
                if '| RESOLVE |' not in line:
                    continue
                m = RESOLVE_RE.search(line)
                if m:
                    rows.append({
                        'ts':       datetime.fromisoformat(m['ts']),
                        'matched':  m['matched'],
                        'pressure': float(m['pressure']),
                        'raw':      line.strip(),
                    })
    return sorted(rows, key=lambda r: r['ts'])


def print_interval_stats(rows, fp_id):
    matched = [r for r in rows if fp_id in r['matched']]
    if len(matched) < 2:
        print(f'Not enough matches for {fp_id}')
        return
    intervals = [
        (matched[i]['ts'] - matched[i-1]['ts']).total_seconds() * 1000
        for i in range(1, len(matched))
    ]
    intervals.sort()
    n = len(intervals)
    mean = sum(intervals) / n
    sigma = statistics.stdev(intervals)
    print(f'Fingerprint: {fp_id}')
    print(f'Matches:     {len(matched)}')
    print(f'Intervals:   n={n}  mean={mean:.0f}ms  sigma={sigma:.1f}ms')
    print(f'             min={min(intervals):.0f}ms  '
          f'p50={intervals[n//2]:.0f}ms  max={max(intervals):.0f}ms')


def main():
    parser = argparse.ArgumentParser(
        description='Parse tpms-tracker RESOLVE log lines into structured data.',
    )
    parser.add_argument(
        'files', nargs='+',
        help='Log file(s) to parse (supports glob: logs/tpms-tracker.log*)',
    )
    parser.add_argument(
        '--fingerprint', '-f',
        help='Filter by fingerprint ID substring',
    )
    parser.add_argument(
        '--since',
        help='Only show events after this timestamp (ISO 8601)',
    )
    parser.add_argument(
        '--stat', choices=['intervals', 'count', 'raw'],
        default='raw',
        help='Output mode: intervals (timing stats), count, or raw lines',
    )
    args = parser.parse_args()

    rows = parse_resolve_lines(args.files)

    if args.since:
        cutoff = datetime.fromisoformat(args.since)
        rows = [r for r in rows if r['ts'] >= cutoff]

    if args.fingerprint:
        if args.stat == 'intervals':
            print_interval_stats(rows, args.fingerprint)
            return
        rows = [r for r in rows if args.fingerprint in r['matched']]

    if args.stat == 'count':
        print(f'Total RESOLVE lines: {len(rows)}')
    elif args.stat == 'raw':
        for r in rows:
            print(r['raw'])
    elif args.stat == 'intervals' and not args.fingerprint:
        print('Error: --stat intervals requires --fingerprint', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
