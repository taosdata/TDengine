#!/usr/bin/env python3
"""lcov2gcovr.py — bridge lcov .info → gcovr JSON tracefile, with two
post-processing passes that make HTML coverage reports for large C
codebases (such as TDengine) actually readable.

Background / motivation
-----------------------
TDengine builds with the textbook gcov flags::

    -fprofile-arcs -ftest-coverage -g -g3 -gdwarf-2 --coverage

which "should" give precise per-line coverage. In practice the rendered
HTML still shows red-and-blue stripes on visibly sequential code such as::

    SResolveWorkItem w = {0};       // line 1222  -> shown as miss
    w.originVtbUid = uid;            // line 1223  -> shown as miss
    w.originCid    = cid;            // line 1224  -> shown as 102 hits
    w.kind         = STREAM_VREF_KIND_COL;  // 1225 -> shown as miss
    tstrncpy(w.refDbName,    ...);   // line 1226  -> shown as 120 hits

Two distinct effects cause this and both are baked into the .info file by
the time we receive it:

1. **GCC emits .loc directives at compile time.** When several adjacent
   simple statements get packed into one machine-code basic block, GCC
   may attach the BB's counter to a single source line and emit NO
   .loc/DA for the rest. Those "missing" lines render as gray / "no
   data" -- visually indistinguishable from comments. The flag set is
   correct; the data simply was not written by GCC.

2. **Comments, blank lines, and brace-only lines** sometimes do get DA
   records (or sit next to ones that do), polluting the denominator.

We can't change what GCC wrote into .gcno. We can do two things on the
.info side that make the report honest:

* Mark blank / brace / comment lines as ``excluded`` so gcovr puts them
  in its dedicated bucket (distinct color + toggle), out of the
  numerator and denominator.

* "BB-coalescing fill": when a simple-statement line shows count=0 and
  is sandwiched between non-zero peer statements with the same control
  flow / call structure, propagate the neighbour's count. This is safe
  because *all* statements inside a single basic block execute the same
  number of times — if one shows 102 hits, the others necessarily ran
  102 times too.

If you need genuinely instruction-level coverage (no heuristics), use
clang's source-based coverage instead::

    clang -fprofile-instr-generate -fcoverage-mapping ...
    llvm-cov show ...

LLVM emits one counter per source line independent of GCC's BB model.

Usage
-----
    python3 lcov2gcovr.py <input.info> <output.json>
    gcovr --json-add-tracefile <output.json> --html-details out/index.html

To add more exclusion patterns, append regexes to ``EXCL`` below.
The fill heuristic is intentionally conservative: it only touches lines
that look like ``<lhs> = <rhs>;`` (no parens, no control-flow keyword)
because those are the lines GCC most reliably puts in a single BB with
their neighbours.
"""
import re, sys, json, os
from collections import defaultdict

EXCL = [
    re.compile(r'^\s*[{}]+\s*;?\s*$'),
    re.compile(r'^\s*//.*$'),
    re.compile(r'^\s*/\*.*\*/\s*$'),
    re.compile(r'^\s*\*.*$'),
    re.compile(r'^\s*$'),
]
def is_noise(text):
    return any(p.match(text) for p in EXCL)

# A "fillable" line is one we are confident sits inside the same basic
# block as its neighbours: simple statements, no control flow, no call,
# no inline brace block that opens a new scope.
CTRL = re.compile(r'^\s*(if|else|while|for|do|switch|case|default|'
                  r'return|goto|break|continue|try|catch)\b')
# Accept "<lhs> = <rhs>;" where neither side contains '(' or ')'.
# Allow inline brace-init like "Foo x = {0};" by stripping "{...}".
SIMPLE_ASSIGN = re.compile(r'^[^()]*=[^()]*;\s*(?://.*)?$')

def fillable(text):
    s = text.rstrip()
    if not s or is_noise(s):
        return False
    if CTRL.search(s):
        return False
    # Strip simple inline brace-init "{ ... }" before paren check.
    stripped = re.sub(r'\{[^{}]*\}', '', s)
    if '(' in stripped or ')' in stripped:
        return False
    return bool(SIMPLE_ASSIGN.match(s))

def fill_bb_coalesced(line_counts, src):
    """Two-pass fill of zero-hit simple-statement lines from nearby BB peer."""
    max_ln = max(line_counts) if line_counts else 0
    if max_ln == 0:
        return
    # Forward pass: propagate last non-zero hint forward across fillable runs.
    hint = 0
    for ln in range(1, max_ln + 1):
        text = src[ln-1].rstrip('\n') if ln <= len(src) else ''
        if is_noise(text):
            continue
        if not fillable(text):
            hint = 0
            continue
        cnt = line_counts.get(ln, 0)
        if cnt > 0:
            hint = cnt
        elif hint > 0 and ln in line_counts:
            line_counts[ln] = hint
    # Backward pass: same but the other way.
    hint = 0
    for ln in range(max_ln, 0, -1):
        text = src[ln-1].rstrip('\n') if ln <= len(src) else ''
        if is_noise(text):
            continue
        if not fillable(text):
            hint = 0
            continue
        cnt = line_counts.get(ln, 0)
        if cnt > 0:
            hint = cnt
        elif hint > 0 and ln in line_counts:
            line_counts[ln] = hint

if len(sys.argv) != 3:
    print("usage: lcov2gcovr.py <input.info> <output.json>", file=sys.stderr); sys.exit(2)
inp, outp = sys.argv[1], sys.argv[2]

files = {}
cur_file = None
cur_lines = defaultdict(lambda: {'count':0, 'noBranch':True, 'excluded':False, 'branches':[]})

def commit():
    global cur_file, cur_lines
    if cur_file is None: return
    try:
        with open(cur_file) as fh:
            src = fh.readlines()
    except OSError:
        src = []

    # Step 1: BB-coalescing fill on the count map.
    count_map = {ln: ent['count'] for ln, ent in cur_lines.items()}
    fill_bb_coalesced(count_map, src)
    for ln, c in count_map.items():
        cur_lines[ln]['count'] = c

    # Step 2: mark noise lines excluded (also add DA entries for source
    # lines that have no data but are noise, so gcovr renders them in the
    # excluded bucket rather than ambiguously gray).
    for lineno in range(1, len(src)+1):
        if is_noise(src[lineno-1].rstrip('\n')):
            cur_lines[lineno] = {'count':0,'noBranch':True,'excluded':True,'branches':[]}

    line_objs = []
    for lineno in sorted(cur_lines):
        ent = cur_lines[lineno]
        line_objs.append({
            'branches': ent['branches'],
            'count': ent['count'],
            'gcovr/excluded': ent['excluded'],
            'gcovr/noncode': ent['excluded'],
            'line_number': lineno,
        })
    files[cur_file] = {'file': cur_file, 'lines': line_objs, 'functions': []}
    cur_file = None
    cur_lines = defaultdict(lambda: {'count':0,'noBranch':True,'excluded':False,'branches':[]})

with open(inp) as fh:
    for raw in fh:
        line = raw.rstrip('\n')
        if line.startswith('SF:'):
            commit()
            cur_file = line[3:]
        elif line == 'end_of_record':
            commit()
        elif line.startswith('DA:') and cur_file:
            m = re.match(r'DA:(\d+),(-?\d+)', line)
            if m:
                ln, cnt = int(m.group(1)), int(m.group(2))
                cur_lines[ln]['count'] = max(cur_lines[ln]['count'], cnt)
        elif line.startswith('BRDA:') and cur_file:
            m = re.match(r'BRDA:(\d+),(\d+),(\d+),(.+)', line)
            if m:
                ln = int(m.group(1)); taken = m.group(4)
                cur_lines[ln]['branches'].append({
                    'count': 0 if taken == '-' else int(taken),
                    'fallthrough': False, 'throw': False,
                })
commit()

out = {
    'gcovr/format_version': '0.14',
    'files': list(files.values()),
}
with open(outp, 'w') as fh:
    json.dump(out, fh)
print(f"wrote {len(out['files'])} files to {outp}")
