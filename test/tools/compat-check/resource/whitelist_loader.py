#!/usr/bin/env python3
# filepath: hot_update/resource/whitelist_loader.py
"""
Whitelist loader and filter for INFORMATION_SCHEMA diff checks.

Whitelist file naming
---------------------
  {from_prefix}~{to_prefix}.yaml

A "prefix" is a partial version number. Matching uses prefix comparison:
  "3"       matches any 3.x.x.x
  "3.3"     matches any 3.3.x.x
  "3.3.6"   matches any 3.3.6.x
  "3.3.6.0" matches exactly 3.3.6.0

Version values come from SELECT SERVER_VERSION() (e.g. "3.3.8.0").

If a single upgrade matches multiple whitelist files all matched files are
merged (union of allowed items).

YAML schema
-----------
  added_tables:    [TABLE_A, TABLE_B]
  deleted_tables:  []
  modified_tables:
    - table: INS_COLUMNS
      added_columns:    [new_col]
      deleted_columns:  []
      # For changed_type / changed_position specify the column name (string)
      # or a dict {col: name}. Any change on that column is forgiven.
      changed_type:     [col_name]
      changed_position: [col_name]
"""

import os
import re
from dataclasses import dataclass, field
from typing import Dict, List


# ── version helpers ───────────────────────────────────────────────────────────

def _parse_prefix(s: str):
    """Parse e.g. '3.3' -> (3, 3),  '3' -> (3,),  '3.3.6.0' -> (3, 3, 6, 0)."""
    parts = s.strip().split('.')
    try:
        return tuple(int(p) for p in parts if p)
    except ValueError:
        return ()


def _version_matches_prefix(actual_ver: str, prefix_str: str) -> bool:
    """True if the numeric part of actual_ver starts with the prefix_str tuple."""
    m = re.search(r'(\d+(?:\.\d+)*)', actual_ver)
    actual = tuple(int(x) for x in m.group(1).split('.')) if m else ()
    prefix = _parse_prefix(prefix_str)
    if not prefix:
        return False  # unparseable / empty prefix -> no match
    n = len(prefix)
    return actual[:n] == prefix


def _whitelist_matches(filename: str, from_ver: str, to_ver: str) -> bool:
    """True if the whitelist filename applies to this from/to upgrade pair."""
    base = os.path.splitext(os.path.basename(filename))[0]   # e.g. "3.3~3.4"
    if '~' not in base:
        return False
    from_prefix, to_prefix = base.split('~', 1)
    return (
        _version_matches_prefix(from_ver, from_prefix.strip()) and
        _version_matches_prefix(to_ver,   to_prefix.strip())
    )


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class ModifiedTableWL:
    """Whitelist entries for one modified table."""
    table:                 str
    added_columns:         List[str] = field(default_factory=list)
    deleted_columns:       List[str] = field(default_factory=list)
    changed_type_cols:     List[str] = field(default_factory=list)   # column names
    changed_position_cols: List[str] = field(default_factory=list)   # column names


@dataclass
class Whitelist:
    source_files:    List[str]                  = field(default_factory=list)
    added_tables:    List[str]                  = field(default_factory=list)
    deleted_tables:  List[str]                  = field(default_factory=list)
    modified_tables: Dict[str, ModifiedTableWL] = field(default_factory=dict)


# ── loader ────────────────────────────────────────────────────────────────────

def load_whitelists(whitelist_dir: str, from_ver: str, to_ver: str) -> Whitelist:
    """
    Scan *whitelist_dir* for .yaml/.yml files whose name matches (from_ver, to_ver).
    All matching files are merged into a single Whitelist (union of allowed items).
    """
    wl = Whitelist()
    if not whitelist_dir or not os.path.isdir(whitelist_dir):
        return wl
    for fname in sorted(os.listdir(whitelist_dir)):
        if not fname.endswith(('.yaml', '.yml')):
            continue
        fpath = os.path.join(whitelist_dir, fname)
        if _whitelist_matches(fpath, from_ver, to_ver):
            _merge_file(wl, fpath)
            wl.source_files.append(fname)
    return wl


def _col_name(c) -> str:
    """Accept either a bare string or a {col: name} / {name: name} dict."""
    return c if isinstance(c, str) else (c.get("col") or c.get("name") or "")


def _merge_file(wl: Whitelist, fpath: str):
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required. Run: pip install pyyaml")
    with open(fpath, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    for t in (data.get("added_tables") or []):
        if t and t not in wl.added_tables:
            wl.added_tables.append(t)

    for t in (data.get("deleted_tables") or []):
        if t and t not in wl.deleted_tables:
            wl.deleted_tables.append(t)

    for entry in (data.get("modified_tables") or []):
        tname = (entry.get("table") or "").upper()
        if not tname:
            continue
        if tname not in wl.modified_tables:
            wl.modified_tables[tname] = ModifiedTableWL(table=tname)
        mtwl = wl.modified_tables[tname]
        for c in (entry.get("added_columns") or []):
            if c and c not in mtwl.added_columns:
                mtwl.added_columns.append(c)
        for c in (entry.get("deleted_columns") or []):
            if c and c not in mtwl.deleted_columns:
                mtwl.deleted_columns.append(c)
        for c in (entry.get("changed_type") or []):
            n = _col_name(c)
            if n and n not in mtwl.changed_type_cols:
                mtwl.changed_type_cols.append(n)
        for c in (entry.get("changed_position") or []):
            n = _col_name(c)
            if n and n not in mtwl.changed_position_cols:
                mtwl.changed_position_cols.append(n)


# ── filter ────────────────────────────────────────────────────────────────────

@dataclass
class FilterResult:
    unexpected_added_tables:    List[str]        = field(default_factory=list)
    unexpected_deleted_tables:  List[str]        = field(default_factory=list)
    unexpected_modified_tables: Dict[str, dict]  = field(default_factory=dict)
    expected_added_tables:      List[str]        = field(default_factory=list)
    expected_deleted_tables:    List[str]        = field(default_factory=list)
    expected_modified_tables:   Dict[str, dict]  = field(default_factory=dict)

    def has_unexpected(self) -> bool:
        return bool(
            self.unexpected_added_tables or
            self.unexpected_deleted_tables or
            self.unexpected_modified_tables
        )

    def _fmt_section(self, tag: str, add_t, del_t, mod_t) -> List[str]:
        lines = []
        if add_t:
            lines.append(f"    Added   tables {tag}: {', '.join(sorted(add_t))}")
        if del_t:
            lines.append(f"    Deleted tables {tag}: {', '.join(sorted(del_t))}")
        for tname in sorted(mod_t):
            d = mod_t[tname]
            lines.append(f"    Modified table {tag}: {tname}")
            if d.get("added_columns"):
                lines.append(f"      + added   : {', '.join(d['added_columns'])}")
            if d.get("deleted_columns"):
                lines.append(f"      - deleted : {', '.join(d['deleted_columns'])}")
            for ct in d.get("changed_type", []):
                lines.append(f"      ~ type    : {ct.col}  {ct.before!r} -> {ct.after!r}")
            for cp in d.get("changed_position", []):
                lines.append(f"      ~ pos     : {cp.col}  pos {cp.before} -> {cp.after}")
        return lines

    def format_unexpected(self) -> str:
        return "\n".join(self._fmt_section(
            "[unexpected]",
            self.unexpected_added_tables,
            self.unexpected_deleted_tables,
            self.unexpected_modified_tables,
        ))

    def format_expected(self) -> str:
        return "\n".join(self._fmt_section(
            "[whitelisted]",
            self.expected_added_tables,
            self.expected_deleted_tables,
            self.expected_modified_tables,
        ))


def apply_whitelist(diff, whitelist: Whitelist) -> FilterResult:
    """
    Split diff items into expected (whitelisted) and unexpected.

    Parameters
    ----------
    diff      : SysInfoDiff        (from sysinfo_checker.compare_snapshots)
    whitelist : Whitelist          (from load_whitelists)
    """
    result    = FilterResult()
    wl_add    = {t.upper() for t in whitelist.added_tables}
    wl_del    = {t.upper() for t in whitelist.deleted_tables}

    for tname in diff.added_tables:
        (result.expected_added_tables if tname.upper() in wl_add
         else result.unexpected_added_tables).append(tname)

    for tname in diff.deleted_tables:
        (result.expected_deleted_tables if tname.upper() in wl_del
         else result.unexpected_deleted_tables).append(tname)

    for tname, td in diff.modified_tables.items():
        mtwl = whitelist.modified_tables.get(tname.upper())
        ok_add  = set(mtwl.added_columns)         if mtwl else set()
        ok_del  = set(mtwl.deleted_columns)        if mtwl else set()
        ok_type = set(mtwl.changed_type_cols)      if mtwl else set()
        ok_pos  = set(mtwl.changed_position_cols)  if mtwl else set()

        unexp = dict(added_columns=[], deleted_columns=[], changed_type=[], changed_position=[])
        exp   = dict(added_columns=[], deleted_columns=[], changed_type=[], changed_position=[])

        for c in td.added_columns:
            (exp["added_columns"] if c in ok_add else unexp["added_columns"]).append(c)
        for c in td.deleted_columns:
            (exp["deleted_columns"] if c in ok_del else unexp["deleted_columns"]).append(c)
        for ct in td.changed_type:
            (exp["changed_type"] if ct.col in ok_type else unexp["changed_type"]).append(ct)
        for cp in td.changed_position:
            (exp["changed_position"] if cp.col in ok_pos else unexp["changed_position"]).append(cp)

        if any(v for v in unexp.values()):
            result.unexpected_modified_tables[tname] = unexp
        if any(v for v in exp.values()):
            result.expected_modified_tables[tname] = exp

    return result


# ── whitelist writer ──────────────────────────────────────────────────────────

def gen_whitelist_filepath(
    from_ver: str, to_ver: str, whitelist_dir: str, gen_whitelist_arg
) -> str:
    """
    Resolve the output filepath for a generated whitelist.

    gen_whitelist_arg:
      True  -> auto-generate filename {from_ver}~{to_ver}.yaml in whitelist_dir
      str   -> use as filename (relative = into whitelist_dir, absolute = as-is)
    """
    def _safe_ver(s: str) -> str:
        """Extract only the numeric X.Y.Z.W part so filenames stay clean."""
        m = re.search(r'\d+(?:\.\d+)+', str(s))
        return m.group(0) if m else re.sub(r'[^\w.\-]', '_', str(s))

    if isinstance(gen_whitelist_arg, str):
        path = gen_whitelist_arg
        if not os.path.isabs(path):
            path = os.path.join(whitelist_dir, path)
        return path
    # Auto-generate using sanitized version numbers
    fname = f"{_safe_ver(from_ver)}~{_safe_ver(to_ver)}.yaml"
    return os.path.join(whitelist_dir, fname)


def write_whitelist_yaml(diff, from_ver: str, to_ver: str, filepath: str):
    """Serialize a SysInfoDiff to a YAML whitelist file at filepath."""
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required. Run: pip install pyyaml")
    from datetime import datetime

    data = {}
    if diff.added_tables:
        data["added_tables"] = diff.added_tables[:]
    if diff.deleted_tables:
        data["deleted_tables"] = diff.deleted_tables[:]
    modified = []
    for tname in sorted(diff.modified_tables):
        td = diff.modified_tables[tname]
        entry = {"table": tname}
        if td.added_columns:
            entry["added_columns"] = td.added_columns[:]
        if td.deleted_columns:
            entry["deleted_columns"] = td.deleted_columns[:]
        ct_list = [{"col": ct.col, "before": ct.before, "after": ct.after}
                   for ct in td.changed_type]
        if ct_list:
            entry["changed_type"] = ct_list
        cp_list = [{"col": cp.col, "before": cp.before, "after": cp.after}
                   for cp in td.changed_position]
        if cp_list:
            entry["changed_position"] = cp_list
        modified.append(entry)
    if modified:
        data["modified_tables"] = modified

    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)

    header = (
        f"# Auto-generated INFORMATION_SCHEMA whitelist\n"
        f"# from_version : {from_ver}\n"
        f"# to_version   : {to_ver}\n"
        f"# generated_at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"#\n"
        f"# All changes listed here are treated as expected for this upgrade path.\n"
        f"# Rename/copy with a shorter version prefix to apply more broadly:\n"
        f"#   e.g.  3.3~3.4.yaml  applies to all 3.3.x.x -> 3.4.x.x upgrades\n\n"
    )
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
