#!/usr/bin/env python3
# filepath: hot_update/resource/sysinfo_checker.py
"""
Snapshot and diff INFORMATION_SCHEMA system tables across TDengine upgrades.

Public API
----------
  from_ver = get_server_version(fqdn, cfg_dir)
  before   = snapshot(fqdn, cfg_dir)          # dict[TABLE_NAME -> list[ColumnInfo]]
  ...perform upgrade...
  after    = snapshot(fqdn, cfg_dir)
  diff     = compare_snapshots(before, after) # SysInfoDiff
  print(diff.format_report())
"""

import logging
import sys
from dataclasses import dataclass, field
from typing import Dict, List

_log = logging.getLogger(__name__)

# ── TDengine field-type helpers ───────────────────────────────────────────────

_FIELD_TYPE_NAMES: Dict[int, str] = {
    0:  "",
    1:  "BOOL",
    2:  "TINYINT",
    3:  "SMALLINT",
    4:  "INT",
    5:  "BIGINT",
    6:  "FLOAT",
    7:  "DOUBLE",
    8:  "VARCHAR",
    9:  "TIMESTAMP",
    10: "NCHAR",
    11: "TINYINT UNSIGNED",
    12: "SMALLINT UNSIGNED",
    13: "INT UNSIGNED",
    14: "BIGINT UNSIGNED",
    15: "JSON",
    16: "VARBINARY",
    20: "GEOMETRY",
}
_SIZED_TYPES = {8, 10, 16}  # types where length is shown: VARCHAR(n), NCHAR(n), VARBINARY(n)


def _field_type_str(type_code: int, length: int) -> str:
    name = _FIELD_TYPE_NAMES.get(type_code, f"TYPE_{type_code}")
    if type_code in _SIZED_TYPES and length and length > 0:
        return f"{name}({length})"
    return name

# ── data classes ─────────────────────────────────────────────────────────────

@dataclass
class ColumnInfo:
    """One column descriptor from DESCRIBE information_schema.<table>."""
    name:     str
    type_str: str   # e.g. "INT", "BINARY(64)", "TIMESTAMP"
    pos:      int   # 0-based ordinal position


@dataclass
class ColumnTypeDiff:
    col:    str
    before: str
    after:  str


@dataclass
class ColumnPosDiff:
    col:    str
    before: int
    after:  int


@dataclass
class TableDiff:
    table:             str
    added_columns:    List[str]              = field(default_factory=list)
    deleted_columns:  List[str]              = field(default_factory=list)
    changed_type:     List[ColumnTypeDiff]   = field(default_factory=list)
    changed_position: List[ColumnPosDiff]    = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.added_columns or self.deleted_columns
                    or self.changed_type or self.changed_position)


@dataclass
class SysInfoDiff:
    added_tables:    List[str]            = field(default_factory=list)
    deleted_tables:  List[str]            = field(default_factory=list)
    modified_tables: Dict[str, TableDiff] = field(default_factory=dict)

    def is_empty(self) -> bool:
        return not (self.added_tables or self.deleted_tables or self.modified_tables)

    def format_report(self) -> str:
        lines = []
        if self.added_tables:
            lines.append(f"    Added   tables : {', '.join(sorted(self.added_tables))}")
        if self.deleted_tables:
            lines.append(f"    Deleted tables : {', '.join(sorted(self.deleted_tables))}")
        for tname in sorted(self.modified_tables):
            td = self.modified_tables[tname]
            lines.append(f"    Modified table : {tname}")
            if td.added_columns:
                lines.append(f"      + added   cols : {', '.join(td.added_columns)}")
            if td.deleted_columns:
                lines.append(f"      - deleted cols : {', '.join(td.deleted_columns)}")
            for ct in td.changed_type:
                lines.append(f"      ~ type changed : {ct.col}  {ct.before!r} -> {ct.after!r}")
            for cp in td.changed_position:
                lines.append(f"      ~ pos  changed : {cp.col}  pos {cp.before} -> {cp.after}")
        return "\n".join(lines) if lines else "    (no differences)"


# ── snapshot ──────────────────────────────────────────────────────────────────

def get_server_version(fqdn: str, cfg_dir: str) -> str:
    """Connect to the running server and return its version string (e.g. '3.3.8.0')."""
    import re
    import taos

    def _extract(s: str) -> str:
        """Extract the first X.Y.Z.W or X.Y.Z numeric version from a string."""
        m = re.search(r'\d+\.\d+\.\d+\.\d+', str(s))
        if m:
            return m.group(0)
        m = re.search(r'\d+\.\d+\.\d+', str(s))
        return m.group(0) if m else ""

    conn = taos.connect(host=fqdn, config=cfg_dir)
    try:
        # Prefer the connector's built-in server_info attribute (e.g. 'ver:3.3.8.0')
        ver = getattr(conn, "server_info", None)
        if ver:
            extracted = _extract(ver)
            if extracted:
                return extracted
        # Fallback: parse from SELECT server_version() result
        cursor = conn.cursor()
        cursor.execute("SELECT server_version()")
        row = cursor.fetchone()
        cursor.close()
        if row is not None and row[0] is not None:
            return _extract(row[0]) or str(row[0])
        return ""
    finally:
        conn.close()


def snapshot(fqdn: str, cfg_dir: str) -> Dict[str, List[ColumnInfo]]:
    """
    Return a snapshot of INFORMATION_SCHEMA:
        { TABLE_NAME_UPPER -> [ColumnInfo, ...] }   (ordered by natural schema position)

    Strategy 1 (3.4.x): query ins_columns WHERE db_name='information_schema'.
    Strategy 2 (3.3.x): SHOW information_schema.tables + per-table SELECT LIMIT 0.
    """
    import taos
    conn = taos.connect(host=fqdn, config=cfg_dir)
    try:
        return _do_snapshot(conn)
    finally:
        conn.close()


def _do_snapshot(conn) -> Dict[str, List[ColumnInfo]]:
    cursor = conn.cursor()
    result: Dict[str, List[ColumnInfo]] = {}
    try:
        # ── Strategy 1: ins_columns (TDengine 3.4.x) ──────────────────────
        # In 3.4.x, SHOW information_schema.tables returns 0 rows, but
        # ins_columns carries full schema for the system tables directly.
        cursor.execute(
            "SELECT table_name, col_name, col_type "
            "FROM information_schema.ins_columns "
            "WHERE db_name='information_schema'"
        )
        rows = cursor.fetchall()

        if rows:
            # Build {TABLE_UPPER -> [ColumnInfo...]} from ins_columns result
            pos_counters: Dict[str, int] = {}
            for row in rows:
                if row is None or row[0] is None or row[1] is None:
                    continue
                tbl_upper = str(row[0]).upper()
                col_name  = str(row[1]).strip()
                col_type  = str(row[2]).strip() if row[2] else ""
                if not col_name:
                    continue
                pos = pos_counters.get(tbl_upper, 0)
                pos_counters[tbl_upper] = pos + 1
                result.setdefault(tbl_upper, []).append(
                    ColumnInfo(name=col_name, type_str=col_type, pos=pos)
                )
            return result

        # ── Strategy 2: SHOW tables + per-table SELECT (TDengine 3.3.x) ──
        cursor.execute("SHOW information_schema.tables")
        rows = cursor.fetchall()
        tables = [str(r[0]) for r in rows if r is not None and r[0] is not None]

        for tbl in tables:
            tbl_upper = tbl.upper()
            try:
                # SELECT LIMIT 0 gets field metadata without fetching rows.
                # If _fields is empty (edge-case), retry with LIMIT 1.
                cursor.execute(
                    f"SELECT * FROM information_schema.`{tbl}` LIMIT 0"
                )
                fields = cursor._fields
                if not fields or fields.count == 0:
                    cursor.execute(
                        f"SELECT * FROM information_schema.`{tbl}` LIMIT 1"
                    )
                    fields = cursor._fields
                cols: List[ColumnInfo] = []
                for pos, fld in enumerate(fields or []):
                    fname = (fld.name or "").strip()
                    type_str = _field_type_str(fld.type, fld.bytes)
                    if fname:
                        cols.append(ColumnInfo(name=fname, type_str=type_str, pos=pos))
                result[tbl_upper] = cols
            except Exception as exc:
                print(f"[sysinfo] WARN: skip {tbl}: {exc}", file=sys.stderr)
    finally:
        cursor.close()
    return result


# ── compare ───────────────────────────────────────────────────────────────────

def compare_snapshots(
    before: Dict[str, List[ColumnInfo]],
    after:  Dict[str, List[ColumnInfo]],
) -> SysInfoDiff:
    """Return a SysInfoDiff describing all structural changes between two snapshots."""
    diff = SysInfoDiff()

    before_names = set(before.keys())
    after_names  = set(after.keys())

    diff.added_tables   = sorted(after_names - before_names)
    diff.deleted_tables = sorted(before_names - after_names)

    for tname in sorted(before_names & after_names):
        bc_map = {c.name: c for c in before[tname]}
        ac_map = {c.name: c for c in after[tname]}

        added   = [n for n in ac_map if n not in bc_map]
        deleted = [n for n in bc_map if n not in ac_map]

        chg_type = []
        chg_pos  = []
        for cname in bc_map:
            if cname not in ac_map:
                continue
            bc, ac = bc_map[cname], ac_map[cname]
            if bc.type_str != ac.type_str:
                chg_type.append(ColumnTypeDiff(col=cname, before=bc.type_str, after=ac.type_str))
            if bc.pos != ac.pos:
                chg_pos.append(ColumnPosDiff(col=cname, before=bc.pos, after=ac.pos))

        if added or deleted or chg_type or chg_pos:
            diff.modified_tables[tname] = TableDiff(
                table=tname,
                added_columns=added,
                deleted_columns=deleted,
                changed_type=chg_type,
                changed_position=chg_pos,
            )

    return diff
