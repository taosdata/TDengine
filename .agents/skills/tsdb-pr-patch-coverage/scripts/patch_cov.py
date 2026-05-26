#!/usr/bin/env python3
"""patch_cov.py — compute patch coverage for a Git diff range using a gcovr
JSON tracefile produced by lcov2gcovr.py.

This script aligns its numbers with the gcovr HTML report (excluded lines —
comments / `{}` / blank — are dropped from the denominator).

Usage:
    python3 patch_cov.py \
        --repo /path/to/repo \
        --base <base-rev> \
        --head <head-rev> \
        --gcovr-json /path/to/coverage.gcovr.json \
        [--patterns '*.c' '*.h' '*.cpp' '*.hpp'] \
        [--out /path/to/patch_coverage.txt]

Output columns:
    Chg  = lines added/modified in the diff
    Hit  = lines that gcovr reports as covered
    Miss = lines that gcovr reports as uncovered
    Excl = lines flagged as excluded by lcov2gcovr.py (comments / `{}` / blank)
    NoDa = lines not present in .gcno (headers, macros, #if-out, etc.)
    Cov% = Hit / (Hit + Miss)
"""
import argparse
import json
import os
import re
import subprocess
import sys


def parse_diff(repo, base, head, patterns):
    """Return {rel_path: set(new_line_numbers)} of additions in the diff."""
    cmd = ['git', '-C', repo, '--no-pager', 'diff', '--unified=0',
           '--no-color', f'{base}..{head}', '--'] + list(patterns)
    diff = subprocess.check_output(cmd, text=True)
    patch = {}
    cur = None
    new_ln = 0
    for line in diff.splitlines():
        if line.startswith('+++ b/'):
            cur = line[6:]
            patch.setdefault(cur, set())
        elif line.startswith('@@') and cur:
            m = re.match(r'@@ -\d+(?:,\d+)? \+(\d+)', line)
            if m:
                new_ln = int(m.group(1))
        elif cur and line.startswith('+') and not line.startswith('+++'):
            patch[cur].add(new_ln)
            new_ln += 1
        elif cur and line.startswith(' '):
            new_ln += 1
    return patch


def load_gcovr(gcovr_json, repo):
    """Return {rel_path: {line: (count, excluded)}}."""
    with open(gcovr_json) as f:
        j = json.load(f)
    cov = {}
    for fobj in j.get('files', []):
        abs_path = fobj['file']
        rel = os.path.relpath(abs_path, repo)
        cov[rel] = {ln['line_number']: (ln.get('count', 0),
                                         ln.get('gcovr/excluded', False))
                    for ln in fobj.get('lines', [])}
    return cov


def collapse_ranges(nums):
    if not nums:
        return ''
    nums = sorted(nums)
    out = []
    s = nums[0]
    p = s
    for x in nums[1:]:
        if x == p + 1:
            p = x
        else:
            out.append((s, p))
            s = x
            p = x
    out.append((s, p))
    return ','.join(f'{a}' if a == b else f'{a}-{b}' for a, b in out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--repo', required=True, help='Git repository root')
    ap.add_argument('--base', required=True, help='Diff base revision')
    ap.add_argument('--head', required=True, help='Diff head revision')
    ap.add_argument('--gcovr-json', required=True,
                    help='gcovr tracefile produced by lcov2gcovr.py')
    ap.add_argument('--patterns', nargs='+',
                    default=['*.c', '*.h', '*.cpp', '*.hpp'],
                    help='Pathspec patterns for git diff')
    ap.add_argument('--out', help='Also write the report to this file')
    args = ap.parse_args()

    patch = parse_diff(args.repo, args.base, args.head, args.patterns)
    cov = load_gcovr(args.gcovr_json, args.repo)

    lines_out = []

    def w(s=''):
        lines_out.append(s)
        print(s)

    header = f"{'File':<70} {'Chg':>5} {'Hit':>5} {'Miss':>5} {'Excl':>5} {'NoDa':>5} {'Cov%':>6}"
    w(header)
    w('-' * 108)

    T_chg = T_hit = T_miss = T_excl = T_noda = 0
    rows = []
    for rel in sorted(patch):
        if not os.path.exists(os.path.join(args.repo, rel)):
            continue
        chg = patch[rel]
        fc = cov.get(rel, {})
        hit = miss = excl = noda = 0
        misses = []
        for ln in chg:
            if ln in fc:
                cnt, ex = fc[ln]
                if ex:
                    excl += 1
                elif cnt > 0:
                    hit += 1
                else:
                    miss += 1
                    misses.append(ln)
            else:
                noda += 1
        exe = hit + miss
        pct = (100.0 * hit / exe) if exe else float('nan')
        rows.append((rel, len(chg), hit, miss, excl, noda, pct, misses))
        T_chg += len(chg); T_hit += hit; T_miss += miss
        T_excl += excl; T_noda += noda

    for r, c, h, m, e, n, p, _ in rows:
        s = f"{p:5.1f}%" if p == p else "  n/a"
        w(f"{r:<70} {c:>5} {h:>5} {m:>5} {e:>5} {n:>5} {s:>6}")
    w('-' * 108)
    Texe = T_hit + T_miss
    Tpct = (100.0 * T_hit / Texe) if Texe else 0.0
    w(f"{'TOTAL':<70} {T_chg:>5} {T_hit:>5} {T_miss:>5} {T_excl:>5} {T_noda:>5} {Tpct:5.1f}%")
    w('')
    w(f"Patch coverage = {T_hit}/{Texe} = {Tpct:.1f}%  "
      f"(HTML-aligned: excluded lines dropped from denominator)")
    w('')
    w('=== Miss lines per file ===')
    for rel, _, _, _, _, _, _, ml in rows:
        if ml:
            w(f'{rel}: {collapse_ranges(ml)}')

    if args.out:
        with open(args.out, 'w') as f:
            f.write('\n'.join(lines_out) + '\n')
        sys.stderr.write(f"[patch_cov] report written to {args.out}\n")


if __name__ == '__main__':
    main()
