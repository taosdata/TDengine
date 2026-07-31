"""
test_fq_02_path_resolution.py

Data-driven framework for path resolution tests (FQ-PATH §2).
All SQL statements are extracted into a shared _CASES array, and results
are validated through file comparison (ans/tmp pattern, same as test_fq_01).

Design:
  - Each case = one testing dimension (e.g., "with default namespace",
    "without default namespace", "after USE external", etc.)
  - Within each dimension, all 3 source types (MySQL, PG, InfluxDB) are exercised.
  - Each source type tests 0–5 segment path combinations across multiple
    statement types (SELECT, DESCRIBE, INSERT, DROP, ALTER, REFRESH, USE, etc.)
  - Framework auto-serializes SQL + results to a result file.
  - On first run the result file becomes the baseline (ans/).
  - Subsequent runs compare against baseline; mismatch = test failure.

Environment requirements:
    - Enterprise edition with federatedQueryEnable = 1.
    - MySQL (FQ_MYSQL_HOST), PostgreSQL (FQ_PG_HOST), InfluxDB (FQ_INFLUX_HOST).
    - Python packages: pymysql, psycopg2, requests.
"""

import os
import re
import shutil
import tempfile
import time
from typing import Any, List, Optional, Sequence, Union

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
)

# ── Connection globals ─────────────────────────────────────────────────────
_M_HOST = ExtSrcEnv.MYSQL_HOST
_M_PORT = ExtSrcEnv.MYSQL_PORT
_M_USER = ExtSrcEnv.MYSQL_USER
_M_PASS = ExtSrcEnv.MYSQL_PASS
_M_DB   = "fq02_path_mdb"
_M_DB2  = "fq02_path_mdb2"

_P_HOST   = ExtSrcEnv.PG_HOST
_P_PORT   = ExtSrcEnv.PG_PORT
_P_USER   = ExtSrcEnv.PG_USER
_P_PASS   = ExtSrcEnv.PG_PASS
_P_DB     = "fq02_path_pdb"
_P_SCHEMA = "public"
_P_SCHEMA2 = "analytics"

_I_HOST  = ExtSrcEnv.INFLUX_HOST
_I_PORT  = ExtSrcEnv.INFLUX_PORT
_I_TOKEN = ExtSrcEnv.INFLUX_TOKEN
_I_DB    = "fq02_path_idb"
_I_DB2   = "fq02_path_idb2"

_FW_TABLE = "path_t"
_BASE_TS  = 1_704_067_200_000  # 2024-01-01T00:00:00Z in ms

_FW_ROWS = [
    ("2024-01-01 00:00:00.000", 1, 1.5, "alpha"),
    ("2024-01-01 00:01:00.000", 2, 2.5, "beta"),
    ("2024-01-01 00:02:00.000", 3, 3.5, "gamma"),
]

_DYNAMIC_RESULT_COLUMNS = {"create_time", "ctime"}

# ── Type shortcuts ─────────────────────────────────────────────────────────
_mysql    = "mysql"
_pg       = "postgresql"
_influxdb = "influxdb"

# ── Step sentinel classes ──────────────────────────────────────────────────


class _CleanupSourceStep:
    """DROP all source names in current case's source_names list."""
    pass


class _CommentStep:
    """Insert a comment/label into the output for readability."""
    def __init__(self, text: str):
        self.text = text


# ── Step helper functions ──────────────────────────────────────────────────

def _cleanup_sources() -> _CleanupSourceStep:
    return _CleanupSourceStep()


def _comment(text: str) -> _CommentStep:
    return _CommentStep(text)


# ── Path generators ────────────────────────────────────────────────────────

def _paths_for_source(src_name: str, ns: str, table: str):
    """Generate path test entries for 0 through 5 segments.

    Returns list of (seg_count, path_str).
    """
    paths = []
    # 0-seg: empty (syntax error)
    paths.append((0, ""))
    # 1-seg: table only
    paths.append((1, table))
    # 2-seg: source.table
    paths.append((2, f"{src_name}.{table}"))
    # 3-seg: source.ns.table
    paths.append((3, f"{src_name}.{ns}.{table}"))
    # 4-seg: too many segments
    paths.append((4, f"{src_name}.{ns}.{table}.extra"))
    # 5-seg: too many segments
    paths.append((5, f"{src_name}.{ns}.{table}.extra.more"))
    return paths


def _use_paths_for_source(src_name: str, ns: str):
    """Generate USE path test entries for 0 through 5 segments.

    Returns list of (seg_count, use_sql).
    """
    paths = []
    # 0-seg: empty USE (syntax error)
    paths.append((0, "use "))
    # 1-seg: USE source_name
    paths.append((1, f"use {src_name}"))
    # 2-seg: USE source.ns
    paths.append((2, f"use {src_name}.{ns}"))
    # 3-seg: USE source.ns.extra (should fail)
    paths.append((3, f"use {src_name}.{ns}.extra"))
    # 4-seg: USE a.b.c.d (should fail)
    paths.append((4, f"use {src_name}.{ns}.extra.more"))
    # 5-seg: USE a.b.c.d.e (should fail)
    paths.append((5, f"use {src_name}.{ns}.extra.more.deep"))
    return paths


# ── SQL Statement Templates ────────────────────────────────────────────────

def _select_stmts(path: str) -> List[str]:
    """SELECT statement for the given path."""
    return [
        f"select * from {path} order by val limit 5",
    ]


def _describe_stmts(path: str) -> List[str]:
    """DESCRIBE statement for the given path."""
    return [f"describe {path}"]


def _insert_stmts(path: str) -> List[str]:
    """INSERT statement (expected to fail on external tables)."""
    return [f"insert into {path} values (now, 999, 9.9, 'test')"]


def _drop_stmts(path: str) -> List[str]:
    """DROP TABLE statement (expected to fail on external tables)."""
    return [f"drop table {path}"]


def _alter_stmts(path: str) -> List[str]:
    """ALTER TABLE statement (expected to fail on external tables)."""
    return [f"alter table {path} comment 'test'"]


def _refresh_stmts(src_name: str) -> List[str]:
    """REFRESH EXTERNAL SOURCE statement."""
    return [f"refresh external source {src_name}"]


# ── Build case steps from paths and templates ──────────────────────────────

def _build_path_test_steps(src_name: str, ns: str, table: str, src_type: str,
                           include_write_ops: bool = True) -> List[str]:
    """Build SQL steps for all segment combinations across statement types.

    Returns a flat list of SQL strings that the framework will execute and record.
    """
    steps: List[str] = []
    paths = _paths_for_source(src_name, ns, table)

    for seg_count, path in paths:
        if not path:
            # 0-seg: empty FROM
            steps.append(f"-- [{seg_count}-seg FROM] expect syntax error")
            steps.append("select * from ")
            continue

        steps.append(f"-- [{seg_count}-seg FROM] path={path}")
        # SELECT (read queries)
        steps.extend(_select_stmts(path))
        # DESCRIBE
        steps.extend(_describe_stmts(path))
        # Write operations (expected to fail for external)
        if include_write_ops:
            steps.extend(_insert_stmts(path))
            steps.extend(_drop_stmts(path))
            steps.extend(_alter_stmts(path))

    return steps


def _build_use_test_steps(src_name: str, ns: str, table: str = "",
                          has_default_ns: bool = True) -> List[str]:
    """Build USE statement steps for all segment combinations (0-5 segments).

    When *table* is provided, a verification SELECT is appended after each
    USE that is expected to succeed (1-seg when has_default_ns, 2-seg always).
    """
    steps: List[str] = []
    use_paths = _use_paths_for_source(src_name, ns)

    for seg_count, use_sql in use_paths:
        steps.append(f"-- [{seg_count}-seg USE] sql={use_sql}")
        steps.append(use_sql)
        # Verify context switch with a 1-seg query after successful USE
        if table:
            if seg_count == 1 and has_default_ns:
                steps.append(f"-- verify: 1-seg query after USE {src_name}")
                steps.append(f"select * from {table} order by val limit 1")
            elif seg_count == 2:
                steps.append(f"-- verify: 1-seg query after USE {src_name}.{ns}")
                steps.append(f"select * from {table} order by val limit 1")

    return steps


# ── Dimension step generators ──────────────────────────────────────────────

def _dim01_steps(src_type: str) -> list:
    """DIM-01: Source WITH default namespace — 0-5 segment paths × all statements."""
    src = "fq02_dim01"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = _build_path_test_steps(src, ns, table, src_type)
    # Also test USE with 0-5 segments
    steps.append("-- [USE segment tests in DIM-01]")
    steps.extend(_build_use_test_steps(src, ns, table))
    return steps


def _dim02_steps(src_type: str) -> list:
    """DIM-02: Source WITHOUT default namespace — 0-5 segment paths × all statements."""
    src = "fq02_dim02"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = _build_path_test_steps(src, ns, table, src_type)
    steps.append("-- [USE segment tests in DIM-02]")
    steps.extend(_build_use_test_steps(src, ns, table, has_default_ns=False))
    # After USE source.ns, 2-seg source.table should succeed
    # (USE sets the namespace context; 2-seg falls back to USE ns when source matches)
    steps.append(f"-- [2-seg after USE source.ns] should succeed — USE provides the ns")
    steps.append(f"use {src}.{ns}")
    steps.append(f"select * from {src}.{table} order by val limit 1")
    return steps


def _dim03_steps(src_type: str) -> list:
    """DIM-03: After USE source — 0-5 segment paths × all statements."""
    src = "fq02_dim03"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = [f"use {src}"]
    steps.extend(_build_path_test_steps(src, ns, table, src_type))
    # 1-seg after USE source should resolve via default ns
    steps.append(f"-- [1-seg after USE source] table={table}")
    steps.extend(_select_stmts(table))
    # USE 0-5 segment tests while already in USE context
    steps.append("-- [USE segment tests in DIM-03]")
    steps.extend(_build_use_test_steps(src, ns, table))
    return steps


def _dim04_steps(src_type: str) -> list:
    """DIM-04: After USE source.ns — 0-5 segment paths × all statements."""
    src = "fq02_dim04"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = [f"use {src}.{ns}"]
    steps.extend(_build_path_test_steps(src, ns, table, src_type))
    # 1-seg after USE source.ns should resolve
    steps.append(f"-- [1-seg after USE source.ns] table={table}")
    steps.extend(_select_stmts(table))
    # USE 0-5 segment tests
    steps.append("-- [USE segment tests in DIM-04]")
    steps.extend(_build_use_test_steps(src, ns, table))
    return steps


def _dim05_steps(src_type: str) -> list:
    """DIM-05: After USE local_db — 0-5 segment paths, external paths still work."""
    src = "fq02_dim05"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = [
        "drop database if exists fq02_local_db",
        "create database fq02_local_db",
        "use fq02_local_db",
    ]
    steps.extend(_build_path_test_steps(src, ns, table, src_type))
    # 1-seg → should look in local db, not find external table
    steps.append(f"-- [1-seg in local db context] table={table}")
    steps.append(f"select * from {table}")
    # USE 0-5 segment tests from local db context
    steps.append("-- [USE segment tests in DIM-05]")
    steps.extend(_build_use_test_steps(src, ns, table))
    steps.append("drop database if exists fq02_local_db")
    return steps


def _dim06_steps(src_type: str) -> list:
    """DIM-06: Three-segment disambiguation — source_name vs local_db.

    When a 3-seg path's first segment matches both a source_name and a local
    database, external source takes priority.
    """
    src = "fq02_dim06"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = [
        # Create local db with same name as source
        f"drop database if exists {src}",
        f"create database {src}",
        f"use {src}",
        f"create table {table} (ts timestamp, val int, score double, name binary(32))",
        f"insert into {table} values ('2024-06-01 00:00:00.000', 999, 99.9, 'local')",
        # 3-seg: source_name.ns.table → external (source takes priority)
        f"-- [3-seg disambiguation] source_name.ns.table → external",
        f"select * from {src}.{ns}.{table} order by val limit 5",
        # 2-seg: src.table → depends on context
        f"-- [2-seg disambiguation] src.table",
        f"select * from {src}.{table} order by val limit 5",
        # USE 0-5 segments with conflict name
        f"-- [USE segment tests in DIM-06 with name conflict]",
    ]
    steps.extend(_build_use_test_steps(src, ns, table))
    steps.append(f"drop database if exists {src}")
    return steps


def _dim07_steps(src_type: str) -> list:
    """DIM-07: Case sensitivity rules.

    MySQL: case-insensitive identifiers.
    PG: fold to lowercase, quoted preserves case.
    InfluxDB: case-sensitive.
    """
    src = "fq02_dim07"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
        steps = [
            f"-- MySQL case-insensitive: uppercase source name",
            f"select * from {src.upper()}.{ns}.{table} limit 1",
            f"select * from {src}.{ns.upper()}.{table} limit 1",
            f"select * from {src}.{ns}.{table.upper()} limit 1",
            f"select * from {src.upper()}.{ns.upper()}.{table.upper()} limit 1",
            f"-- MySQL case-insensitive: mixed case 2-seg",
            f"select * from {src.upper()}.{table} limit 1",
            f"select * from {src}.{table.upper()} limit 1",
            f"-- MySQL case-insensitive: USE with different case",
            f"use {src.upper()}",
            f"select * from {table} limit 1",
            f"use {src.upper()}.{ns.upper()}",
            f"select * from {table} limit 1",
        ]
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
        steps = [
            f"-- PG case fold: unquoted → lowercase",
            f"select * from {src}.{ns}.{table} limit 1",
            f"select * from {src.upper()}.{ns.upper()}.{table.upper()} limit 1",
            f"-- PG quoted preserves case (backtick in TDengine = quote in PG)",
            f"select * from `{src}`.`{ns}`.`{table}` limit 1",
            f"-- PG case fold in 2-seg",
            f"select * from {src.upper()}.{table} limit 1",
            f"-- PG USE with case variants",
            f"use {src.upper()}",
            f"select * from {table} limit 1",
            f"use {src.upper()}.{ns.upper()}",
            f"select * from {table} limit 1",
        ]
    else:
        ns, table = _I_DB, _FW_TABLE
        steps = [
            f"-- InfluxDB case-sensitive: exact match required",
            f"select * from {src}.{ns}.{table} limit 1",
            f"-- InfluxDB: wrong case → error",
            f"select * from {src}.{ns}.{table.upper()} limit 1",
            f"select * from {src}.{ns.upper()}.{table} limit 1",
            f"select * from {src.upper()}.{ns}.{table} limit 1",
            f"-- InfluxDB: USE with wrong case → error",
            f"use {src.upper()}",
            f"use {src}.{ns.upper()}",
        ]
    return steps


def _dim08_steps(src_type: str) -> list:
    """DIM-08: Special identifiers — backtick full permutations, reserved words, special chars."""
    src = "fq02_dim08"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = []

    # ── 2-seg backtick permutations (4 combinations) ──────────────────
    steps.append("-- [2-seg backtick permutations: 4 combinations]")
    # combo 1: no backticks
    steps.append(f"select * from {src}.{table} limit 1")
    # combo 2: backtick first segment only
    steps.append(f"select * from `{src}`.{table} limit 1")
    # combo 3: backtick second segment only
    steps.append(f"select * from {src}.`{table}` limit 1")
    # combo 4: backtick both segments
    steps.append(f"select * from `{src}`.`{table}` limit 1")

    # ── 3-seg backtick permutations (8 combinations) ──────────────────
    steps.append("-- [3-seg backtick permutations: 8 combinations]")
    for s_bt in (False, True):
        for n_bt in (False, True):
            for t_bt in (False, True):
                s_str = f"`{src}`" if s_bt else src
                n_str = f"`{ns}`" if n_bt else ns
                t_str = f"`{table}`" if t_bt else table
                steps.append(f"select * from {s_str}.{n_str}.{t_str} limit 1")

    # ── Backtick in USE statements ────────────────────────────────────
    steps.append("-- [Backtick in USE: 1-seg and 2-seg]")
    steps.append(f"use `{src}`")
    steps.append(f"select * from {table} limit 1")
    steps.append(f"use `{src}`.`{ns}`")
    steps.append(f"select * from {table} limit 1")
    steps.append("-- [Backtick in 3-seg USE → should fail]")
    steps.append(f"use `{src}`.`{ns}`.`{table}`")

    # ── Special identifiers: reserved words as table name ─────────────
    steps.append("-- [Special: SQL reserved word as table name (backtick required)]")
    steps.append(f"select * from {src}.`select` limit 1")
    steps.append(f"select * from {src}.{ns}.`select` limit 1")

    # ── Special identifiers: Chinese characters ───────────────────────
    steps.append("-- [Special: Chinese characters in table name]")
    steps.append(f"select * from {src}.`\u6d4b\u8bd5\u8868` limit 1")
    steps.append(f"select * from {src}.{ns}.`\u6d4b\u8bd5\u8868` limit 1")

    # ── Special identifiers: numeric-prefix identifier ────────────────
    steps.append("-- [Special: numeric prefix identifier]")
    steps.append(f"select * from {src}.`123table` limit 1")
    steps.append(f"select * from {src}.{ns}.`123table` limit 1")

    # ── Special identifiers: identifier containing dot ────────────────
    steps.append("-- [Special: identifier containing dot (must be backticked)]")
    steps.append(f"select * from {src}.`a.b` limit 1")
    steps.append(f"select * from {src}.{ns}.`a.b` limit 1")

    # ── Special identifiers: identifier containing space ──────────────
    steps.append("-- [Special: identifier containing space]")
    steps.append(f"select * from {src}.`my table` limit 1")
    steps.append(f"select * from {src}.{ns}.`my table` limit 1")

    # ── USE with special source name (backtick) ───────────────────────
    steps.append("-- [USE with special identifiers]")
    steps.append(f"use `{src}`")
    steps.append(f"select * from `{table}` limit 1")

    return steps


def _dim09_steps(src_type: str) -> list:
    """DIM-09: ALTER default namespace, verify path resolution changes."""
    src = "fq02_dim09"
    if src_type == _mysql:
        ns, ns2, table = _M_DB, _M_DB2, _FW_TABLE
        alter_sql = f"alter external source {src} set database='{ns2}'"
        restore_sql = f"alter external source {src} set database='{ns}'"
    elif src_type == _pg:
        ns, ns2, table = _P_SCHEMA, _P_SCHEMA2, _FW_TABLE
        alter_sql = f"alter external source {src} set schema='{ns2}'"
        restore_sql = f"alter external source {src} set schema='{ns}'"
    else:
        ns, ns2, table = _I_DB, _I_DB2, _FW_TABLE
        alter_sql = f"alter external source {src} set database='{ns2}'"
        restore_sql = f"alter external source {src} set database='{ns}'"

    steps = [
        f"-- Before ALTER: 2-seg resolves to ns={ns}",
        f"select * from {src}.{table} order by val limit 5",
        f"-- ALTER default namespace to {ns2}",
        alter_sql,
        f"-- After ALTER: 2-seg now resolves to ns={ns2}",
        f"select * from {src}.{table} order by val limit 5",
        f"-- 3-seg explicit still works for original ns",
        f"select * from {src}.{ns}.{table} order by val limit 5",
        f"-- USE after ALTER: USE source → uses new default ns",
        f"use {src}",
        f"select * from {table} limit 3",
        f"-- USE source.ns with old ns still explicit",
        f"use {src}.{ns}",
        f"select * from {table} limit 3",
        f"-- Restore",
        restore_sql,
        f"select * from {src}.{table} order by val limit 5",
    ]
    return steps


def _dim10_steps(src_type: str) -> list:
    """DIM-10: Multi-source JOIN — local+external, cross-source, subquery."""
    src = "fq02_dim10"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    ext_path = f"{src}.{ns}.{table}"
    steps = [
        "create database if not exists fq02_join_db",
        "use fq02_join_db",
        "drop table if exists local_t",
        "create table local_t (ts timestamp, val int, score double, name binary(32))",
        "insert into local_t values ('2024-01-01 00:00:00.000', 1, 1.5, 'alpha')",
        "insert into local_t values ('2024-01-01 00:01:00.000', 2, 2.5, 'beta')",
        f"-- JOIN local + external (3-seg)",
        f"select a.val, b.val from local_t a, {ext_path} b where a.val = b.val limit 5",
        f"-- Subquery with external (3-seg)",
        f"select * from (select val from {ext_path} order by val limit 3)",
        f"-- UNION external + local",
        f"select val from {ext_path} union all select val from local_t order by val limit 5",
        f"-- JOIN with 2-seg path",
        f"select a.val, b.val from local_t a, {src}.{table} b where a.val = b.val limit 5",
    ]

    # ── Cross-source JOIN: create a secondary source of different type ──
    # For each primary type, create a secondary source and JOIN them
    sec_src = "fq02_dim10_sec"
    if src_type == _mysql:
        # Secondary = PG
        sec_create = (
            f"create external source if not exists {sec_src} type='postgresql' "
            f"host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' "
            f"database='{_P_DB}' schema='{_P_SCHEMA}'")
        sec_path = f"{sec_src}.{_P_SCHEMA}.{_FW_TABLE}"
    elif src_type == _pg:
        # Secondary = MySQL
        sec_create = (
            f"create external source if not exists {sec_src} type='mysql' "
            f"host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' "
            f"database='{_M_DB}'")
        sec_path = f"{sec_src}.{_M_DB}.{_FW_TABLE}"
    else:
        # Secondary = MySQL
        sec_create = (
            f"create external source if not exists {sec_src} type='mysql' "
            f"host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' "
            f"database='{_M_DB}'")
        sec_path = f"{sec_src}.{_M_DB}.{_FW_TABLE}"

    steps.extend([
        f"-- [Cross-source JOIN] create secondary source",
        f"drop external source if exists {sec_src}",
        sec_create,
        f"-- Cross-source JOIN: primary({src_type}) + secondary",
        f"select a.val, b.val from {ext_path} a, {sec_path} b where a.val = b.val limit 5",
        f"-- Cross-source in subquery",
        f"select * from (select val from {sec_path} order by val limit 3)",
        f"-- Cross-source UNION",
        f"select val from {ext_path} union all select val from {sec_path} order by val limit 5",
        f"-- [EXT-010] WHERE IN subquery with external path",
        f"select val from local_t where val in (select val from {ext_path})",
        f"-- [EXT-010] WHERE EXISTS subquery with external path",
        f"select val from local_t a where exists (select 1 from {ext_path} b where b.val = a.val)",
        f"-- [EXT-010] Nested subquery with external path",
        f"select * from (select * from (select val from {ext_path} order by val limit 3))",
        f"-- Cleanup secondary source",
        f"drop external source if exists {sec_src}",
    ])

    steps.append("drop table if exists local_t")
    return steps


def _dim11_steps(src_type: str) -> list:
    """DIM-11: USE context switching — external ↔ local ↔ cross-ext alternation."""
    src = "fq02_dim11"
    sec_src = "fq02_dim11b"
    if src_type == _mysql:
        ns, ns2, table = _M_DB, _M_DB2, _FW_TABLE
    elif src_type == _pg:
        ns, ns2, table = _P_SCHEMA, _P_SCHEMA2, _FW_TABLE
    else:
        ns, ns2, table = _I_DB, _I_DB2, _FW_TABLE

    # Build secondary source CREATE SQL for cross-ext switching test
    if src_type == _mysql:
        sec_create = (
            f"create external source if not exists {sec_src} type='mysql' "
            f"host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}' "
            f"database='{ns2}'")
    elif src_type == _pg:
        sec_create = (
            f"create external source if not exists {sec_src} type='postgresql' "
            f"host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' "
            f"database='{_P_DB}' schema='{ns2}'")
    else:
        sec_create = (
            f"create external source if not exists {sec_src} type='influxdb' "
            f"host='{_I_HOST}' port={_I_PORT} user='admin' "
            f"database='{ns2}' options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')")

    steps = [
        "create database if not exists fq02_switch_db",
        "drop table if exists fq02_switch_db.local_t",
        "create table fq02_switch_db.local_t (ts timestamp, val int)",
        "insert into fq02_switch_db.local_t values ('2024-06-01 00:00:00.000', 42)",
        f"-- Switch to external source (1-seg USE)",
        f"use {src}",
        f"select * from {table} order by val limit 3",
        f"-- Switch to local db",
        "use fq02_switch_db",
        "select * from local_t limit 1",
        f"-- [Rule 4] 1-seg ext table after USE local_db should fail (ext context cleared)",
        f"select * from {table}",
        f"-- 2-seg still reaches external from local context",
        f"select * from {src}.{table} order by val limit 3",
        f"-- Switch to external with explicit ns (2-seg USE)",
        f"use {src}.{ns}",
        f"select * from {table} order by val limit 3",
        f"-- Switch to local again",
        "use fq02_switch_db",
        "select * from local_t limit 1",
        f"-- [Rule 4] 1-seg ext table after second USE local_db should fail",
        f"select * from {table}",
        f"-- Rapid alternation: external → local → external",
        f"use {src}",
        f"select count(*) from {table}",
        "use fq02_switch_db",
        "select count(*) from local_t",
        f"-- [Rule 4] ext context cleared during rapid alternation",
        f"select count(*) from {table}",
        f"use {src}.{ns}",
        f"select count(*) from {table}",
        # ── Cross-ext-source switching ──
        f"-- [Cross-ext switch] Create secondary source",
        f"drop external source if exists {sec_src}",
        sec_create,
        f"-- [Cross-ext switch] USE source_A then query",
        f"use {src}",
        f"select * from {table} order by val limit 1",
        f"-- [Cross-ext switch] USE source_B → context switches to source_B",
        f"use {sec_src}",
        f"select * from {table} order by val limit 1",
        f"-- [Cross-ext switch] USE source_A again → back to source_A context",
        f"use {src}",
        f"select * from {table} order by val limit 1",
        f"-- Cleanup secondary source",
        f"drop external source if exists {sec_src}",
        "use fq02_switch_db",
    ]
    return steps


def _dim12_steps(src_type: str) -> list:
    """DIM-12: Name conflict — source name = local db name, CREATE rejection."""
    src = "fq02_dim12"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = [
        f"-- Attempt to create local db with same name as external source → should fail",
        f"create database {src}",
        f"-- Verify source still works after conflict attempt",
        f"select * from {src}.{ns}.{table} order by val limit 1",
        f"-- USE the source (not the db)",
        f"use {src}",
        f"select * from {table} limit 1",
        f"-- Cleanup conflicting db if created",
        f"drop database if exists {src}",
        f"-- [EXT-007] System DB name conflict: information_schema",
        (f"create external source information_schema type='{src_type}' "
         f"host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'"),
        f"-- [EXT-007] Cleanup: drop information_schema source if created",
        f"drop external source if exists information_schema",
        f"-- [EXT-007] System DB name conflict: performance_schema",
        (f"create external source performance_schema type='{src_type}' "
         f"host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'"),
        f"-- [EXT-007] Cleanup: drop performance_schema source if created",
        f"drop external source if exists performance_schema",
    ]
    return steps


def _dim13_steps(src_type: str) -> list:
    """DIM-13: Non-SELECT statements on external paths (INSERT/DELETE/DROP/ALTER/CREATE/DESCRIBE/REFRESH).

    Tests all statement types with both 2-seg and 3-seg paths.
    Unsupported write/DDL operations: INSERT, DELETE, DROP TABLE, DROP STABLE,
    ALTER TABLE, CREATE TABLE, CREATE STABLE, CREATE INDEX.
    Also tests query against non-existent source.
    """
    src = "fq02_dim13"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    path_3seg = f"{src}.{ns}.{table}"
    path_2seg = f"{src}.{table}"
    steps = [
        f"-- INSERT into external (3-seg) → should fail",
        f"insert into {path_3seg} values (now, 999, 9.9, 'test')",
        f"-- INSERT into external (2-seg) → should fail",
        f"insert into {path_2seg} values (now, 999, 9.9, 'test')",
        f"-- DELETE from external (3-seg) → should fail",
        f"delete from {path_3seg} where ts < now",
        f"-- DELETE from external (2-seg) → should fail",
        f"delete from {path_2seg} where ts < now",
        f"-- DROP external table (3-seg) → should fail",
        f"drop table {path_3seg}",
        f"-- DROP external table (2-seg) → should fail",
        f"drop table {path_2seg}",
        f"-- DROP STABLE on external (3-seg) → should fail",
        f"drop stable {path_3seg}",
        f"-- DROP STABLE on external (2-seg) → should fail",
        f"drop stable {path_2seg}",
        f"-- ALTER external table (3-seg) → should fail",
        f"alter table {path_3seg} comment 'test'",
        f"-- ALTER external table (2-seg) → should fail",
        f"alter table {path_2seg} comment 'test'",
        f"-- CREATE TABLE on external path (3-seg) → should fail",
        f"create table {path_3seg} (ts timestamp, v int)",
        f"-- CREATE TABLE on external path (2-seg) → should fail",
        f"create table {path_2seg} (ts timestamp, v int)",
        f"-- CREATE STABLE on external path (3-seg) → should fail",
        f"create stable {path_3seg} (ts timestamp, v int) tags (t1 int)",
        f"-- CREATE STABLE on external path (2-seg) → should fail",
        f"create stable {path_2seg} (ts timestamp, v int) tags (t1 int)",
        f"-- CREATE INDEX on external table (3-seg) → should fail",
        f"create index idx1 on {path_3seg} (v)",
        f"-- CREATE INDEX on external table (2-seg) → should fail",
        f"create index idx1 on {path_2seg} (v)",
        f"-- DESCRIBE external table (3-seg) → should succeed",
        f"describe {path_3seg}",
        f"-- DESCRIBE external table (2-seg) → should succeed",
        f"describe {path_2seg}",
        f"-- REFRESH source → should succeed",
        f"refresh external source {src}",
        f"-- SHOW tables in external ns",
        f"show {src}.{ns}.tables",
        f"-- [Non-existent source] SELECT → should return EXT_SOURCE_NOT_FOUND",
        f"select * from nonexist_source_xyz.some_ns.{table} limit 1",
        f"select * from nonexist_source_xyz.{table} limit 1",
        f"-- [Non-existent source] DESCRIBE → should fail",
        f"describe nonexist_source_xyz.some_ns.{table}",
        f"-- [Non-existent source] USE → should fail",
        f"use nonexist_source_xyz",
        f"use nonexist_source_xyz.some_ns",
        f"-- [EXT-011] USE source then SHOW/DESCRIBE",
        f"use {src}",
        f"show tables",
        f"describe {table}",
        f"-- [EXT-011] USE source.ns then SHOW/DESCRIBE",
        f"use {src}.{ns}",
        f"show tables",
        f"describe {table}",
    ]
    return steps


def _dim14_steps(src_type: str) -> list:
    """DIM-14: USE statement 0-5 segments — comprehensive test of all USE forms."""
    src = "fq02_dim14"
    if src_type == _mysql:
        ns, ns2 = _M_DB, _M_DB2
        table = _FW_TABLE
    elif src_type == _pg:
        ns, ns2 = _P_SCHEMA, _P_SCHEMA2
        table = _FW_TABLE
    else:
        ns, ns2 = _I_DB, _I_DB2
        table = _FW_TABLE

    steps = []
    # Test USE with 0 through 5 segments
    use_paths = _use_paths_for_source(src, ns)
    for seg_count, use_sql in use_paths:
        steps.append(f"-- [{seg_count}-seg USE] sql={use_sql}")
        steps.append(use_sql)
        # After each successful USE, verify context with a query
        if seg_count in (1, 2):
            steps.append(f"-- verify context after {seg_count}-seg USE")
            steps.append(f"select * from {table} order by val limit 1")

    # Additional USE edge cases
    steps.extend([
        f"-- USE non-existent source → should fail",
        f"use nonexist_source_xyz",
        f"-- USE non-existent source.ns → should fail",
        f"use nonexist_source_xyz.some_ns",
        f"-- USE source.nonexistent_ns → should fail",
        f"use {src}.nonexistent_ns_xyz",
        f"-- [Failed USE preserves context] previous ext context should be retained",
        f"select * from {table} order by val limit 1",
        f"-- USE local db (verify local USE still works)",
        "drop database if exists fq02_use_db",
        "create database fq02_use_db",
        "use fq02_use_db",
        "select database()",
        f"-- [Rule 4] 1-seg ext table after USE local_db should fail",
        f"select * from {table}",
        "drop database if exists fq02_use_db",
        f"-- USE back to external after local",
        f"use {src}",
        f"select * from {table} order by val limit 1",
        f"-- USE source.ns then 1-seg query",
        f"use {src}.{ns}",
        f"select * from {table} order by val limit 1",
        f"-- [USE override/reset] USE source.ns2 overrides to alt namespace",
        f"use {src}.{ns2}",
        f"select * from {table} order by val limit 1",
        f"-- [USE override/reset] USE source (1-seg) resets to default ns",
        f"use {src}",
        f"select * from {table} order by val limit 1",
        f"-- [EXT-013] USE with leading dot → should fail",
        f"use .{src}",
        f"-- [EXT-013] USE with trailing dot → should fail",
        f"use {src}.",
        f"-- [EXT-013] USE with double dot → should fail",
        f"use {src}..{ns}",
    ])
    return steps


def _dim15_steps(src_type: str) -> list:
    """DIM-15: PG-specific schema handling (only full test for PG, brief for others)."""
    src = "fq02_dim15"
    if src_type != _pg:
        return [f"-- DIM-15 skipped for {src_type} (PG-only test)"]

    table = _FW_TABLE
    steps = [
        f"-- PG without explicit schema → defaults to public",
        f"select * from {src}.{table} order by val limit 3",
        f"-- PG 3-seg with explicit schema=public",
        f"select * from {src}.{_P_SCHEMA}.{table} order by val limit 3",
        f"-- PG 3-seg with schema=analytics",
        f"select * from {src}.{_P_SCHEMA2}.{table} order by val limit 3",
        f"-- ALTER to change schema",
        f"alter external source {src} set schema='{_P_SCHEMA2}'",
        f"-- After ALTER: 2-seg now resolves to analytics",
        f"select * from {src}.{table} order by val limit 3",
        f"-- USE after ALTER schema: should use new schema",
        f"use {src}",
        f"select * from {table} limit 3",
        f"-- USE with explicit ns overrides",
        f"use {src}.{_P_SCHEMA}",
        f"select * from {table} limit 3",
        f"-- ALTER to clear schema (set empty)",
        f"alter external source {src} set schema=''",
        f"-- After clear: 2-seg falls back to public",
        f"select * from {src}.{table} order by val limit 3",
        f"-- USE after clear schema",
        f"use {src}",
        f"select * from {table} limit 3",
        f"-- Restore schema",
        f"alter external source {src} set schema='{_P_SCHEMA}'",
        # ── Scenario 3: USE source.non-default-schema + 1-seg query ──
        f"-- USE source.analytics (non-default schema) then 1-seg query",
        f"use {src}.{_P_SCHEMA2}",
        f"select * from {table} order by val limit 1",
        # ── Scenario 2: USE source.database_name (PG db, not schema) ──
        f"-- USE source.database_name (PG db, not schema) → should error",
        f"use {src}.{_P_DB}",
        # ── Scenario 4: USE source.nonexistent_schema ──
        f"-- USE source.nonexistent_schema → should error",
        f"use {src}.nonexistent_schema_xyz",
        # ── Scenario 1: PG source without schema → USE should error ──
        f"-- PG source without schema: create + USE should error",
        f"drop external source if exists fq02_dim15b",
        (f"create external source if not exists fq02_dim15b type='postgresql' "
         f"host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' "
         f"database='{_P_DB}'"),
        f"use fq02_dim15b",
        f"-- Verify 2-seg USE with schema works on no-schema source",
        f"use fq02_dim15b.{_P_SCHEMA}",
        f"select * from {table} order by val limit 1",
        f"drop external source if exists fq02_dim15b",
    ]
    return steps


def _dim16_steps(src_type: str) -> list:
    """DIM-16: View path resolution (EXT-004).

    MySQL/PG views are queryable via FROM path. InfluxDB has no views.
    """
    src = "fq02_dim16"
    if src_type == _influxdb:
        return [f"-- DIM-16 skipped for InfluxDB (no views)"]

    if src_type == _mysql:
        ns = _M_DB
    else:
        ns = _P_SCHEMA

    steps = [
        f"-- [EXT-004] View with ts PK: 2-seg path",
        f"select * from {src}.v_{_FW_TABLE} order by val limit 5",
        f"-- [EXT-004] View with ts PK: 3-seg path",
        f"select * from {src}.{ns}.v_{_FW_TABLE} order by val limit 5",
        f"-- [EXT-004] View without ts PK (no_ts): 2-seg",
        f"select * from {src}.v_no_ts limit 5",
        f"-- [EXT-004] View without ts PK (no_ts): 3-seg",
        f"select * from {src}.{ns}.v_no_ts limit 5",
        f"-- [EXT-004] DESCRIBE view: 2-seg",
        f"describe {src}.v_{_FW_TABLE}",
        f"-- [EXT-004] DESCRIBE view: 3-seg",
        f"describe {src}.{ns}.v_{_FW_TABLE}",
        f"-- [EXT-004] USE source then 1-seg view query",
        f"use {src}",
        f"select * from v_{_FW_TABLE} order by val limit 3",
        f"-- [EXT-004] USE source.ns then 1-seg view query",
        f"use {src}.{ns}",
        f"select * from v_{_FW_TABLE} order by val limit 3",
    ]
    return steps


def _dim17_steps(src_type: str) -> list:
    """DIM-17: DROP source during active USE context (EXT-016).

    After USE source, DROP the source, then verify queries fail.
    """
    src = "fq02_dim17"
    if src_type == _mysql:
        ns, table = _M_DB, _FW_TABLE
    elif src_type == _pg:
        ns, table = _P_SCHEMA, _FW_TABLE
    else:
        ns, table = _I_DB, _FW_TABLE

    steps = [
        f"-- [EXT-016] USE source then query (should succeed)",
        f"use {src}",
        f"select * from {table} order by val limit 1",
        f"-- [EXT-016] DROP the source while USE context is active",
        f"drop external source {src}",
        f"-- [EXT-016] 1-seg query after DROP → should fail",
        f"select * from {table} order by val limit 1",
        f"-- [EXT-016] 2-seg query after DROP → should fail",
        f"select * from {src}.{table} order by val limit 1",
        f"-- [EXT-016] USE dropped source → should fail",
        f"use {src}",
    ]
    return steps


def _dim18_steps(src_type: str) -> list:
    """DIM-18: ALTER source during active USE context (EXT-015).

    After USE source, ALTER the source, then verify behavior changes.
    """
    src = "fq02_dim18"
    if src_type == _mysql:
        ns, ns2, table = _M_DB, _M_DB2, _FW_TABLE
        alter_sql = f"alter external source {src} set database='{ns2}'"
    elif src_type == _pg:
        ns, ns2, table = _P_SCHEMA, _P_SCHEMA2, _FW_TABLE
        alter_sql = f"alter external source {src} set schema='{ns2}'"
    else:
        ns, ns2, table = _I_DB, _I_DB2, _FW_TABLE
        alter_sql = f"alter external source {src} set database='{ns2}'"

    steps = [
        f"-- [EXT-015] USE source then query (should succeed from ns={ns})",
        f"use {src}",
        f"select * from {table} order by val limit 1",
        f"-- [EXT-015] ALTER source while USE context is active",
        alter_sql,
        f"-- [EXT-015] 1-seg query after ALTER → should use new ns={ns2}",
        f"select * from {table} order by val limit 1",
        f"-- [EXT-015] Explicit 3-seg with old ns still works",
        f"select * from {src}.{ns}.{table} order by val limit 1",
    ]
    return steps


# ── Build _CASES array ─────────────────────────────────────────────────────

def _make_create_sql(src_name: str, src_type: str, database: Optional[str],
                     schema: Optional[str] = None) -> str:
    """Generate CREATE EXTERNAL SOURCE SQL for the given type."""
    if src_type == _mysql:
        sql = (f"create external source if not exists {src_name} type='mysql' "
               f"host='{_M_HOST}' port={_M_PORT} user='{_M_USER}' password='{_M_PASS}'")
        if database:
            sql += f" database='{database}'"
        return sql
    elif src_type == _pg:
        sql = (f"create external source if not exists {src_name} type='postgresql' "
               f"host='{_P_HOST}' port={_P_PORT} user='{_P_USER}' password='{_P_PASS}' "
               f"database='{_P_DB}'")
        if schema:
            sql += f" schema='{schema}'"
        return sql
    else:  # influxdb
        sql = (f"create external source if not exists {src_name} type='influxdb' "
               f"host='{_I_HOST}' port={_I_PORT} user='admin'")
        if database:
            sql += f" database='{database}'"
        sql += f" options('api_token'='{_I_TOKEN}', 'protocol'='flight_sql')"
        return sql


def _drop_src_sql(name: str) -> str:
    return f"drop external source if exists {name}"


# Each dimension creates its sources, runs all path tests, then cleans up.

_CASES = [

    # ── DIM-01: With default namespace ─────────────────────────────────
    ["DIM-01", [_mysql, _pg, _influxdb], ["fq02_dim01"],
     "With default namespace: 0-5 segment paths × all statement types (incl. USE)",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim01"),
         lambda t: _make_create_sql("fq02_dim01", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim01_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-02: Without default namespace ──────────────────────────────
    ["DIM-02", [_mysql, _pg, _influxdb], ["fq02_dim02"],
     "Without default namespace: 0-5 segment paths × all statement types (incl. USE)",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim02"),
         lambda t: _make_create_sql("fq02_dim02", t, database=None,
                                    schema=None) if t == _mysql else
                   _make_create_sql("fq02_dim02", t,
                                    database=_P_DB if t == _pg else None,
                                    schema=None),
         lambda t: _dim02_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-03: After USE source ───────────────────────────────────────
    ["DIM-03", [_mysql, _pg, _influxdb], ["fq02_dim03"],
     "After USE source_name: 0-5 segment paths × all statement types (incl. USE)",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim03"),
         lambda t: _make_create_sql("fq02_dim03", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim03_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-04: After USE source.ns ────────────────────────────────────
    ["DIM-04", [_mysql, _pg, _influxdb], ["fq02_dim04"],
     "After USE source.ns: 0-5 segment paths × all statement types (incl. USE)",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim04"),
         lambda t: _make_create_sql("fq02_dim04", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim04_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-05: After USE local_db ─────────────────────────────────────
    ["DIM-05", [_mysql, _pg, _influxdb], ["fq02_dim05"],
     "After USE local_db: external paths via qualified paths + USE 0-5 seg",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim05"),
         lambda t: _make_create_sql("fq02_dim05", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim05_steps(t),
         "drop database if exists fq02_local_db",
         _cleanup_sources(),
     ],
    ],

    # ── DIM-06: Three-segment disambiguation ───────────────────────────
    ["DIM-06", [_mysql, _pg, _influxdb], ["fq02_dim06"],
     "Three-segment disambiguation: source_name vs local_db conflict + USE 0-5 seg",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim06"),
         lambda t: _make_create_sql("fq02_dim06", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim06_steps(t),
         lambda t: f"drop database if exists fq02_dim06",
         _cleanup_sources(),
     ],
    ],

    # ── DIM-07: Case sensitivity ───────────────────────────────────────
    ["DIM-07", [_mysql, _pg, _influxdb], ["fq02_dim07"],
     "Case sensitivity: MySQL insensitive, PG fold, InfluxDB sensitive (incl. USE)",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim07"),
         lambda t: _make_create_sql("fq02_dim07", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim07_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-08: Special identifiers & backticks ────────────────────────
    ["DIM-08", [_mysql, _pg, _influxdb], ["fq02_dim08"],
     "Special identifiers: backtick combinations in FROM and USE",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim08"),
         lambda t: _make_create_sql("fq02_dim08", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim08_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-09: ALTER namespace impact ─────────────────────────────────
    ["DIM-09", [_mysql, _pg, _influxdb], ["fq02_dim09"],
     "ALTER default namespace: path resolution + USE changes after ALTER",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim09"),
         lambda t: _make_create_sql("fq02_dim09", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim09_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-10: Multi-source JOIN ──────────────────────────────────────
    ["DIM-10", [_mysql, _pg, _influxdb], ["fq02_dim10", "fq02_dim10_sec"],
     "Multi-source JOIN: local+external, cross-source, subquery, UNION",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim10"),
         lambda t: _make_create_sql("fq02_dim10", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim10_steps(t),
         "drop table if exists fq02_join_db.local_t",
         _cleanup_sources(),
     ],
    ],

    # ── DIM-11: USE context switching ──────────────────────────────────
    ["DIM-11", [_mysql, _pg, _influxdb], ["fq02_dim11", "fq02_dim11b"],
     "USE context switching: external ↔ local ↔ cross-ext alternation",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim11"),
         lambda t: _make_create_sql("fq02_dim11", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim11_steps(t),
         "drop table if exists fq02_switch_db.local_t",
         _cleanup_sources(),
     ],
    ],

    # ── DIM-12: Name conflict ──────────────────────────────────────────
    ["DIM-12", [_mysql, _pg, _influxdb], ["fq02_dim12"],
     "Name conflict: source_name = local db name rejection + USE behavior",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim12"),
         lambda t: _make_create_sql("fq02_dim12", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim12_steps(t),
         lambda t: f"drop database if exists fq02_dim12",
         _cleanup_sources(),
     ],
    ],

    # ── DIM-13: Non-SELECT statements on external paths ────────────────
    ["DIM-13", [_mysql, _pg, _influxdb], ["fq02_dim13"],
     "Non-SELECT statements: INSERT/DELETE/DROP/ALTER/CREATE/CREATE INDEX/DESCRIBE/REFRESH on external paths",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim13"),
         lambda t: _make_create_sql("fq02_dim13", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim13_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-14: USE 0-5 segments comprehensive ─────────────────────────
    ["DIM-14", [_mysql, _pg, _influxdb], ["fq02_dim14"],
     "USE statement 0-5 segments: all forms, valid + invalid + edge cases",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim14"),
         lambda t: _make_create_sql("fq02_dim14", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim14_steps(t),
         "drop database if exists fq02_use_db",
         _cleanup_sources(),
     ],
    ],

    # ── DIM-15: PG-specific schema handling ────────────────────────────
    ["DIM-15", [_mysql, _pg, _influxdb], ["fq02_dim15", "fq02_dim15b"],
     "PG-specific: schema handling (default/explicit/ALTER/clear) + USE",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim15"),
         lambda t: _make_create_sql("fq02_dim15", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim15_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-16: View path resolution (EXT-004) ────────────────────────
    ["DIM-16", [_mysql, _pg, _influxdb], ["fq02_dim16"],
     "View path resolution: MySQL/PG views via FROM path (EXT-004)",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim16"),
         lambda t: _make_create_sql("fq02_dim16", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim16_steps(t),
         _cleanup_sources(),
     ],
    ],

    # ── DIM-17: DROP during active USE (EXT-016) ──────────────────────
    ["DIM-17", [_mysql, _pg, _influxdb], ["fq02_dim17"],
     "DROP source during active USE context (EXT-016)",
     [
         lambda t: _drop_src_sql("fq02_dim17"),
         lambda t: _make_create_sql("fq02_dim17", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim17_steps(t),
         # Source already dropped in steps; cleanup just in case
         _cleanup_sources(),
     ],
    ],

    # ── DIM-18: ALTER during active USE (EXT-015) ─────────────────────
    ["DIM-18", [_mysql, _pg, _influxdb], ["fq02_dim18"],
     "ALTER source during active USE context (EXT-015)",
     [
         _cleanup_sources(),
         lambda t: _drop_src_sql("fq02_dim18"),
         lambda t: _make_create_sql("fq02_dim18", t,
                                    database=_M_DB if t == _mysql else (_I_DB if t == _influxdb else _P_DB),
                                    schema=_P_SCHEMA if t == _pg else None),
         lambda t: _dim18_steps(t),
         _cleanup_sources(),
     ],
    ],
]


# ── Test class ─────────────────────────────────────────────────────────────

class TestFq02PathResolution(FederatedQueryVersionedMixin):
    _fw_data_prepared = False

    # Override parent updatecfgDict to also reset the client-side timezone to
    # Asia/Shanghai (CST).  Same reason as TestFq01ExternalSource: prevents
    # C-library UTC leakage from prior test classes in the full suite.
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "timezone": "Asia/Shanghai",
        "clientCfg": {
            "federatedQueryEnable": 1,
            "timezone": "Asia/Shanghai",
        },
    }

    # ──────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def setup_method(self, method):
        if TestFq02PathResolution._fw_data_prepared:
            return
        self._fw_prepare_shared_data()
        TestFq02PathResolution._fw_data_prepared = True

    def teardown_class(self):
        tdLog.debug(f"teardown {__file__}")
        TestFq02PathResolution._fw_data_prepared = False

    # ──────────────────────────────────────────────────────────────────────
    # Shared test data setup
    # ──────────────────────────────────────────────────────────────────────

    def _fw_prepare_shared_data(self):
        """Prepare test tables in all external databases."""
        mysql_cfg  = self._mysql_cfg()
        pg_cfg     = self._pg_cfg()
        influx_cfg = self._influx_cfg()

        mysql_values = ", ".join(
            f"('{ts}', {val}, {score}, '{name}')"
            for ts, val, score, name in _FW_ROWS
        )

        # MySQL: create tables and views in both databases
        for db in [_M_DB, _M_DB2]:
            ExtSrcEnv.mysql_create_db_cfg(mysql_cfg, db)
            ExtSrcEnv.mysql_exec_cfg(mysql_cfg, db, [
                f"DROP TABLE IF EXISTS `{_FW_TABLE}`",
                (
                    f"CREATE TABLE `{_FW_TABLE}` ("
                    "ts DATETIME(3) PRIMARY KEY, "
                    "val INT, score DOUBLE, name VARCHAR(32))"
                ),
                f"INSERT INTO `{_FW_TABLE}` VALUES {mysql_values}",
                # Views for EXT-004 (view path resolution)
                f"DROP VIEW IF EXISTS `v_{_FW_TABLE}`",
                f"CREATE VIEW `v_{_FW_TABLE}` AS SELECT * FROM `{_FW_TABLE}` WHERE val > 0",
                f"DROP VIEW IF EXISTS `v_no_ts`",
                f"CREATE VIEW `v_no_ts` AS SELECT val, name FROM `{_FW_TABLE}`",
            ])

        # PostgreSQL: create tables and views in both schemas
        ExtSrcEnv.pg_create_db_cfg(pg_cfg, _P_DB)
        ExtSrcEnv.pg_exec_cfg(pg_cfg, _P_DB, [
            f"CREATE SCHEMA IF NOT EXISTS {_P_SCHEMA}",
            f"CREATE SCHEMA IF NOT EXISTS {_P_SCHEMA2}",
            # Drop views before table to avoid DependentObjectsStillExist
            f"DROP VIEW IF EXISTS {_P_SCHEMA}.v_no_ts",
            f"DROP VIEW IF EXISTS {_P_SCHEMA}.v_{_FW_TABLE}",
            f"DROP TABLE IF EXISTS {_P_SCHEMA}.{_FW_TABLE}",
            (
                f"CREATE TABLE {_P_SCHEMA}.{_FW_TABLE} ("
                "ts TIMESTAMP PRIMARY KEY, "
                "val INT, score DOUBLE PRECISION, name VARCHAR(32))"
            ),
            f"INSERT INTO {_P_SCHEMA}.{_FW_TABLE} VALUES {mysql_values}",
            # Views for EXT-004 (view path resolution)
            f"CREATE VIEW {_P_SCHEMA}.v_{_FW_TABLE} AS SELECT * FROM {_P_SCHEMA}.{_FW_TABLE} WHERE val > 0",
            f"CREATE VIEW {_P_SCHEMA}.v_no_ts AS SELECT val, name FROM {_P_SCHEMA}.{_FW_TABLE}",
            # Drop view before table for schema2 as well
            f"DROP VIEW IF EXISTS {_P_SCHEMA2}.v_{_FW_TABLE}",
            f"DROP TABLE IF EXISTS {_P_SCHEMA2}.{_FW_TABLE}",
            (
                f"CREATE TABLE {_P_SCHEMA2}.{_FW_TABLE} ("
                "ts TIMESTAMP PRIMARY KEY, "
                "val INT, score DOUBLE PRECISION, name VARCHAR(32))"
            ),
            f"INSERT INTO {_P_SCHEMA2}.{_FW_TABLE} VALUES {mysql_values}",
            f"CREATE VIEW {_P_SCHEMA2}.v_{_FW_TABLE} AS SELECT * FROM {_P_SCHEMA2}.{_FW_TABLE} WHERE val > 0",
        ])

        # InfluxDB: create tables in both databases
        for db in [_I_DB, _I_DB2]:
            ExtSrcEnv.influx_create_db_cfg(influx_cfg, db)
            influx_lines = [
                (
                    f"{_FW_TABLE} val={val}i,score={score},name=\"{name}\" "
                    f"{_BASE_TS + idx * 60000}000000"
                )
                for idx, (_, val, score, name) in enumerate(_FW_ROWS)
            ]
            ExtSrcEnv.influx_write_cfg(influx_cfg, db, influx_lines)

    # ──────────────────────────────────────────────────────────────────────
    # Framework helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _fw_fmt_cell(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, float):
            return f"{value:.12g}"
        return str(value)

    def _fw_fmt_rows(self, rows: Sequence) -> List[str]:
        return ["|".join(self._fw_fmt_cell(v) for v in row) for row in rows]

    @staticmethod
    def _fw_drop_dynamic_columns(description, rows: Sequence):
        """Drop dynamic result columns (e.g. create_time) before serializing."""
        if not description:
            return description, rows
        keep_indices = [
            i for i, col in enumerate(description)
            if str(col[0]).lower() not in _DYNAMIC_RESULT_COLUMNS
        ]
        if len(keep_indices) == len(description):
            return description, rows
        filtered_desc = [description[i] for i in keep_indices]
        filtered_rows = [tuple(row[i] for i in keep_indices) for row in rows]
        return filtered_desc, filtered_rows

    def _fw_fmt_result(self, description, rows: Sequence) -> List[str]:
        """Format query result with column header line followed by data rows."""
        description, rows = self._fw_drop_dynamic_columns(description, rows)
        lines = []
        if description:
            lines.append("|".join(col[0] for col in description))
        lines.extend(self._fw_fmt_rows(rows))
        return lines

    @staticmethod
    def _fw_normalize_result_lines(result) -> List[str]:
        """Normalize result payload to non-empty lines for stable serialization."""
        if result is None:
            return ["<empty result>"]
        if isinstance(result, str):
            return [result] if result else ["<empty result>"]
        lines = [str(line) for line in result if str(line)]
        return lines if lines else ["<empty result set>"]

    def _fw_append_step_block(
        self,
        blocks: List[str],
        label: str,
        step_tag: str,
        kind: str,
        sql: str,
        result,
    ):
        """Append one serialized block (SQL + RESULT)."""
        lines = [
            f"### {label} {step_tag} {kind}",
            "SQL: " + sql,
            "RESULT:",
        ]
        lines.extend(self._fw_normalize_result_lines(result))
        lines.append("---")
        blocks.append("\n".join(lines))

    @staticmethod
    def _fw_append_case_boundary(blocks: List[str], label: str, desc: str, is_start: bool):
        marker = "CASE START" if is_start else "CASE END"
        blocks.append(
            "\n".join([
                "=" * 96,
                f"{marker}: {label}",
                f"DESC: {desc}",
                "=" * 96,
            ])
        )

    @staticmethod
    def _fw_query_once(sql: str, exit: bool = False):
        """Execute SQL exactly once via tdSql.query (no internal retry)."""
        return tdSql.query(sql, exit=exit, queryTimes=1)

    def _fw_runtime_sql(self, sql: str, src_type: str) -> str:
        """Patch import-time Influx token placeholders with runtime token."""
        if src_type != _influxdb:
            return sql
        runtime_token = self._influx_cfg().token
        if not runtime_token or runtime_token == _I_TOKEN:
            return sql
        pattern = re.compile(
            r"(api_token'\s*=\s*')" + re.escape(_I_TOKEN) + r"(')",
            re.IGNORECASE,
        )
        return pattern.sub(r"\1" + runtime_token + r"\2", sql)

    def _fw_exec_step(
        self,
        step,
        src_type: str,
        source_names: List[str],
        blocks: List[str],
        label: str,
        step_tag: str,
    ):
        """Recursively execute one step; append serialised result to blocks."""

        # ── callable step (lambda t: ...) ─────────────────────────────────
        if callable(step):
            result = step(src_type)
            if result is not None:
                self._fw_exec_step(result, src_type, source_names, blocks, label, step_tag)
            return

        # ── list of steps (from dimension functions) ──────────────────────
        if isinstance(step, list):
            for sub_no, sub_step in enumerate(step, start=1):
                sub_tag = f"{step_tag}.{sub_no:03d}"
                self._fw_exec_step(sub_step, src_type, source_names, blocks, label, sub_tag)
            return

        # ── string step (SQL) ─────────────────────────────────────────────
        if isinstance(step, str):
            sql = self._fw_runtime_sql(step.strip(), src_type)
            # Comment lines → record but don't execute
            if sql.startswith("--"):
                blocks.append(f"### {label} {step_tag} COMMENT\n{sql}\n---")
                return

            is_query = bool(
                re.match(
                    r"(select|show|describe|explain)\b",
                    sql,
                    re.IGNORECASE,
                )
            )
            if is_query:
                try:
                    tdSql.cursor.execute(sql)
                    rows = self._fw_fmt_result(tdSql.cursor.description, tdSql.cursor.fetchall() or [])
                except Exception as _query_err:
                    _msg = str(_query_err).splitlines()[0] if str(_query_err) else "unknown error"
                    rows = [f"ERROR: {_msg[:200]}"]
                self._fw_append_step_block(blocks, label, step_tag, "QUERY", sql, rows)
            else:
                try:
                    tdSql.cursor.execute(sql)
                    exec_result = "OK"
                except Exception as _exec_err:
                    _msg = str(_exec_err).splitlines()[0] if str(_exec_err) else "unknown error"
                    exec_result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(blocks, label, step_tag, "EXEC", sql, exec_result)
            return

        # ── CleanupSourceStep ─────────────────────────────────────────────
        if isinstance(step, _CleanupSourceStep):
            for name in source_names:
                sql = f"drop external source if exists {name}"
                try:
                    tdSql.cursor.execute(sql)
                    result = "OK"
                except Exception as _drop_err:
                    _msg = str(_drop_err).splitlines()[0] if str(_drop_err) else "unknown error"
                    result = f"ERROR: {_msg[:200]}"
                self._fw_append_step_block(blocks, label, step_tag, "CLEANUP", sql, result)
            return

        # ── CommentStep ───────────────────────────────────────────────────
        if isinstance(step, _CommentStep):
            blocks.append(f"### {label} {step_tag} COMMENT\n-- {step.text}\n---")
            return

        raise ValueError(f"Unknown step type: {type(step).__name__}")

    # ──────────────────────────────────────────────────────────────────────
    # Result file management
    # ──────────────────────────────────────────────────────────────────────

    def _fw_baseline_file(self) -> str:
        label = self._version_label().replace(".", "_").replace("/", "_")
        return os.path.join(
            os.path.dirname(__file__),
            "ans",
            f"test_fq_02_path_resolution_framework_{label}.txt",
        )

    def _fw_compare_baseline(self, blocks: List[str]):
        actual   = "\n".join(blocks) + "\n"
        # Normalise dynamic influxdb3 admin tokens (apiv3_...) to the stable
        # placeholder "test-token" so that the baseline comparison is stable
        # across test runs even though influxdb3 generates a new random token
        # on each hard reset.  The baseline was captured with "test-token".
        import re as _re
        actual = _re.sub(r"apiv3_[A-Za-z0-9_\-]+", "test-token", actual)
        # TDengine includes the offending SQL text in syntax-error messages.
        # When the real apiv3 token is long the SQL string is longer than with
        # the short "test-token" placeholder, causing TDengine to truncate the
        # error message before the closing ')"' chars.  After the token
        # normalisation above both actual and baseline should agree on the
        # token spelling, so strip everything AFTER "test-token" on syntax-
        # error lines in BOTH texts so truncation differences don't matter.
        _SYNTAX_ERR_NORM = _re.compile(
            r'(ERROR: \[0x2600\]: syntax error near "[^\n]*test-token)[^\n]*',
            _re.MULTILINE,
        )
        def _norm_synerr(text):
            return _SYNTAX_ERR_NORM.sub(r"\1", text)
        actual = _norm_synerr(actual)
        baseline = self._fw_baseline_file()
        tmp_file = os.path.join(
            tempfile.gettempdir(),
            f"{os.path.basename(baseline)}.{os.getpid()}.tmp",
        )

        os.makedirs(os.path.dirname(baseline), exist_ok=True)
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(actual)

        if not os.path.isfile(baseline):
            shutil.copy(tmp_file, baseline)
            os.remove(tmp_file)
            tdLog.info(f"Framework baseline file created: {baseline}")
            return

        with open(baseline, "r", encoding="utf-8") as f:
            expected = f.read()
        expected = _norm_synerr(expected)

        if expected == actual:
            os.remove(tmp_file)
            tdLog.info("Framework baseline comparison: OK")
            return

        exp_lines = expected.splitlines()
        act_lines = actual.splitlines()
        diff_line, exp_val, act_val = -1, "<EOF>", "<EOF>"
        for idx in range(max(len(exp_lines), len(act_lines))):
            lhs = exp_lines[idx] if idx < len(exp_lines) else "<EOF>"
            rhs = act_lines[idx] if idx < len(act_lines) else "<EOF>"
            if lhs != rhs:
                diff_line = idx + 1
                exp_val, act_val = lhs, rhs
                break

        raise AssertionError(
            "Framework baseline mismatch\n"
            f"  baseline: {baseline}\n"
            f"  actual  : {tmp_file}\n"
            f"  first diff at line {diff_line}:\n"
            f"    baseline: {exp_val!r}\n"
            f"    actual  : {act_val!r}"
        )

    # ──────────────────────────────────────────────────────────────────────
    # Main test entry point
    # ──────────────────────────────────────────────────────────────────────

    def test_fq_path_resolution_framework(self):
        """Data-driven path resolution test: all dimensions × all source types."""
        # FQ_CASES="DIM-01,DIM-05" → only run specified dimensions
        _only = os.environ.get("FQ_CASES", "").strip()
        _only_set = set(_only.split(",")) if _only else None
        blocks: List[str] = []
        timings: List[str] = []

        for case in _CASES:
            case_id, types, source_names, desc, steps = case
            if _only_set and case_id not in _only_set:
                continue
            for src_type in types:
                label = f"{case_id}[{src_type[:3].upper()}]"
                tdLog.info(f"Running {label}: {desc}")
                # Reconnect to clear USE context — new connection has no db selected
                tdSql.connect()
                self._fw_append_case_boundary(blocks, label, desc, is_start=True)
                _t0 = time.monotonic()
                for step_no, step in enumerate(steps, start=1):
                    step_tag = f"STEP-{step_no:03d}"
                    self._fw_exec_step(step, src_type, source_names, blocks, label, step_tag)
                elapsed = time.monotonic() - _t0
                self._fw_append_case_boundary(blocks, label, desc, is_start=False)
                timing_line = f"{label}: {elapsed:.2f}s"
                print(f"[TIMING] {timing_line}", flush=True)
                timings.append(timing_line)

        print("[TIMING SUMMARY]", flush=True)
        for t in timings:
            print(f"  {t}", flush=True)

        if _only_set:
            tdLog.info(f"FQ_CASES={_only} — skipping baseline comparison")
        else:
            self._fw_compare_baseline(blocks)
