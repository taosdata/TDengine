#!/usr/bin/env python3
"""Parsers for taosBenchmark log output."""

import re

ANSI = re.compile(r'\x1b\[[0-9;]*m')

INS_RE = re.compile(
    r'Spent\s+([\d.]+)\s+\(real\s+([\d.]+)\)\s+seconds to insert rows:\s*(\d+)'
    r'\s+with\s+(\d+)\s+thread\(s\) into\s+(\S+)\s+([\d.]+)\s+\(real\s+([\d.]+)\)\s+records/second'
    r'(?:.*?)(stmt delay1=([\d.]+)s)?')

DELAY_RE = re.compile(
    r'insert delay,\s*min:\s*([\d.]+)ms,\s*avg:\s*([\d.]+)ms,\s*p90:\s*([\d.]+)ms,'
    r'\s*p95:\s*([\d.]+)ms,\s*p99:\s*([\d.]+)ms,\s*max:\s*([\d.]+)ms')

QSQL_RE = re.compile(
    r'complete query with\s+(\d+)\s+threads and\s+(\d+)\s+sql\s+(\d+)\s+spend\s+([\d.]+)s'
    r'\s+QPS:\s*([\d.]+)\s+query delay avg:\s*([\d.]+)s\s+min:\s*([\d.]+)s'
    r'\s+max:\s*([\d.]+)s\s+p90:\s*([\d.]+)s\s+p95:\s*([\d.]+)s\s+p99:\s*([\d.]+)s'
    r'\s+SQL command:\s*(.*)$', re.M)

QMULTI_RE = re.compile(
    r'spend\s+([\d.]+)s using\s+(\d+)\s+threads complete query\s+(\d+)\s+times,'
    r'\s*min delay:\s*([\d.]+)s,\s*avg delay:\s*([\d.]+)s,\s*p90:\s*([\d.]+)s,'
    r'\s*p95:\s*([\d.]+)s,\s*p99:\s*([\d.]+)s,\s*max:\s*([\d.]+)s')

QTOTAL_RE = re.compile(
    r'Spend\s+([\d.]+)\s+second completed total queries:\s*(\d+),'
    r'\s*the QPS of all threads:\s*([\d.]+)')


def clean(text):
    return ANSI.sub('', text or '')


def parse_insert(text):
    """Return insert-benchmark metrics, or {} if no summary line was found."""
    text = clean(text)
    out = {}
    m = None
    for m in INS_RE.finditer(text):
        pass
    if m:
        out.update({
            'seconds': float(m.group(1)),
            'real_seconds': float(m.group(2)),
            'rows': int(m.group(3)),
            'threads': int(m.group(4)),
            'database': m.group(5),
            'rows_per_sec': float(m.group(6)),
            'real_rows_per_sec': float(m.group(7)),
        })
        if m.group(9):
            out['stmt_delay1_s'] = float(m.group(9))
    d = None
    for d in DELAY_RE.finditer(text):
        pass
    if d:
        out.update({
            'delay_min_ms': float(d.group(1)),
            'delay_avg_ms': float(d.group(2)),
            'delay_p90_ms': float(d.group(3)),
            'delay_p95_ms': float(d.group(4)),
            'delay_p99_ms': float(d.group(5)),
            'delay_max_ms': float(d.group(6)),
        })
    return out


def parse_query(text):
    """Return per-SQL and total query metrics."""
    text = clean(text)
    out = {'sqls': [], 'total': {}}
    for m in QSQL_RE.finditer(text):
        out['sqls'].append({
            'threads': int(m.group(1)),
            'sqls': int(m.group(2)),
            'index': int(m.group(3)),
            'spend_s': float(m.group(4)),
            'qps': float(m.group(5)),
            'delay_avg_s': float(m.group(6)),
            'delay_min_s': float(m.group(7)),
            'delay_max_s': float(m.group(8)),
            'delay_p90_s': float(m.group(9)),
            'delay_p95_s': float(m.group(10)),
            'delay_p99_s': float(m.group(11)),
            'sql': m.group(12).strip(),
        })
    for m in QMULTI_RE.finditer(text):
        out['sqls'].append({
            'threads': int(m.group(2)),
            'times': int(m.group(3)),
            'spend_s': float(m.group(1)),
            'delay_avg_s': float(m.group(5)),
            'delay_min_s': float(m.group(4)),
            'delay_p90_s': float(m.group(6)),
            'delay_p95_s': float(m.group(7)),
            'delay_p99_s': float(m.group(8)),
            'delay_max_s': float(m.group(9)),
            'sql': '(multi-thread aggregate)',
        })
    m = None
    for m in QTOTAL_RE.finditer(text):
        pass
    if m:
        out['total'] = {
            'spend_s': float(m.group(1)),
            'queries': int(m.group(2)),
            'qps': float(m.group(3)),
        }
    return out


def read_log(path):
    for enc in ('utf-8', 'utf-16', 'latin-1'):
        try:
            with open(path, 'r', encoding=enc) as f:
                return f.read()
        except (UnicodeDecodeError, UnicodeError):
            continue
        except OSError:
            return ''
    return ''
