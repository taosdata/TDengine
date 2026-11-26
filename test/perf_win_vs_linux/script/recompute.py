#!/usr/bin/env python3
"""Re-derive per-round resource aggregates from the raw sampler CSVs.

Needed when the sampler itself had a defect: the raw CSV keeps cumulative
counters, so the aggregates can be recomputed without re-running the workload.

Usage:
    recompute.py <result.json> [<result.json> ...]

A backup `<result.json>.bak` is written once before the first rewrite.
"""

import csv
import json
import os
import shutil
import sys


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def aggregates(csv_path):
    """Return aggregate resource metrics derived from one samples CSV."""
    if not os.path.exists(csv_path):
        return None
    rows = []
    with open(csv_path, newline='') as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if len(rows) < 2:
        return None

    def col(name):
        return [_f(r.get(name)) for r in rows]

    t = col('t')
    ts = [v for v in t if v is not None]
    if not ts:
        return None
    span = ts[-1] - ts[0]
    if span <= 0:
        return None

    out = {'samples': len(rows), 'span_s': span}

    for key, name in (('taosd_cpu_cores', 'taosd_cpu_s'),
                      ('client_cpu_cores', 'client_cpu_s')):
        vals = [v for v in col(name) if v is not None]
        if len(vals) >= 2:
            out[key] = (vals[-1] - vals[0]) / span

    for key, col_name in (('rss_kb', 'rss_kb'), ('threads', 'threads')):
        vals = [v for v in col(col_name) if v]
        if vals:
            out[key + '_avg'] = sum(vals) / len(vals)
            out[key + '_max'] = max(vals)
    if 'rss_kb_avg' in out:
        out['rss_avg_mb'] = out['rss_kb_avg'] / 1024
        out['rss_max_mb'] = out['rss_kb_max'] / 1024
    if 'threads_avg' in out:
        out['threads_max'] = out['threads_max']

    w = [v for v in col('disk_writes') if v is not None]
    b = [v for v in col('disk_bytes') if v is not None]
    ms = [v for v in col('disk_ms_write') if v is not None]
    if len(w) >= 2:
        dw = w[-1] - w[0]
        out['disk_iops'] = dw / span
        if dw > 0 and len(ms) >= 2:
            out['disk_write_ms'] = (ms[-1] - ms[0]) / dw
    if len(b) >= 2:
        out['disk_mb_s'] = (b[-1] - b[0]) / 1024 ** 2 / span
    return out


def recompute(result_path):
    with open(result_path, encoding='utf-8') as f:
        res = json.load(f)
    base = os.path.dirname(result_path)
    changed = 0
    for section in ('writes', 'queries'):
        for entry in res.get(section) or []:
            csv_path = os.path.join(base, entry.get('label', '') + '.samples.csv')
            agg = aggregates(csv_path)
            if not agg:
                continue
            cur = entry.setdefault('res', {})
            # Keep Windows-only fields (percentages) untouched; only
            # normalised keys are overwritten.
            for k in ('taosd_cpu_cores', 'client_cpu_cores', 'rss_avg_mb',
                      'rss_max_mb', 'threads_avg', 'threads_max',
                      'disk_iops', 'disk_mb_s', 'disk_write_ms', 'samples'):
                if k in agg:
                    cur[k] = agg[k]
            cur['recomputed'] = True
            changed += 1
    bak = result_path + '.bak'
    if not os.path.exists(bak):
        shutil.copy2(result_path, bak)
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(res, f, indent=2)
    return changed


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    for p in sys.argv[1:]:
        n = recompute(p)
        print('%s: %d rounds updated' % (p, n))
