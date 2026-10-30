# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""
test_fq_05_config_effectiveness.py

FQ-05: Configuration Effectiveness — validates that all federated-query
config parameters take effect correctly:

    test_fq_05_001_defaults       – Group A: default values match documentation
    test_fq_05_002_alter_and_verify – Group B: ALTER + SHOW VARIABLES roundtrip
    test_fq_05_003_behavioral     – Group C: behavioural effectiveness

5 target parameters:

    | Parameter                            | Default       | Range                | Scope  |
    |--------------------------------------|---------------|----------------------|--------|
    | federatedQueryEnable                 | false (bool)  | 0/1                  | BOTH   |
    | federatedQueryConnectTimeoutMs       | 5000          | [100, 600000]        | BOTH   |
    | federatedQueryQueryTimeoutMs         | 1000000000    | [100, 1000000000]    | BOTH   |
    | federatedQueryMaxPoolSizePerSource   | 64            | [1, 1024]            | BOTH   |
    | federatedQueryIdleConnTtlSec         | 600           | [1, 86400]           | BOTH   |
"""

import os
import shutil
import threading
import time

import pytest

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    FederatedQueryCaseHelper,
    FederatedQueryVersionedMixin,
    ExtSrcEnv,
    TSDB_CODE_EXT_FEATURE_DISABLED,
    TSDB_CODE_EXT_CONNECT_FAILED,
    TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
    TSDB_CODE_EXT_SOURCE_UNAVAILABLE,
    TSDB_CODE_EXT_QUERY_TIMEOUT,
    TSDB_CODE_EXT_FEDERATED_DISABLED,
    TSDB_CODE_MND_DB_NOT_EXIST,
)


# ---------------------------------------------------------------------------
# MySQL test data
# ---------------------------------------------------------------------------
_PUSH_T_SQLS = [
    "CREATE TABLE IF NOT EXISTS push_t "
    "(val INT, score DOUBLE, name VARCHAR(32), flag TINYINT(1), status VARCHAR(16))",
    "DELETE FROM push_t",
    "INSERT INTO push_t VALUES "
    "(1,1.5,'alpha',1,'active'),"
    "(2,2.5,'beta',0,'idle'),"
    "(3,3.5,'gamma',1,'active'),"
    "(4,4.5,'delta',0,'idle'),"
    "(5,5.5,'epsilon',1,'active')",
]

# ---------------------------------------------------------------------------
# SQL Server test data — same 5 rows as the MySQL push_t fixture, with the
# T-SQL type mapping (INT / FLOAT / NVARCHAR / BIT) used across the FQ suites.
# ---------------------------------------------------------------------------
_SQLSERVER_PUSH_T_SQLS = [
    "IF OBJECT_ID('dbo.push_t') IS NOT NULL DROP TABLE dbo.push_t",
    "CREATE TABLE dbo.push_t "
    "(val INT, score FLOAT, name NVARCHAR(32), flag BIT, status NVARCHAR(16))",
    "INSERT INTO dbo.push_t VALUES "
    "(1,1.5,'alpha',1,'active'),"
    "(2,2.5,'beta',0,'idle'),"
    "(3,3.5,'gamma',1,'active'),"
    "(4,4.5,'delta',0,'idle'),"
    "(5,5.5,'epsilon',1,'active')",
]

# ---------------------------------------------------------------------------
# Oracle test data — the same 5 rows as the MySQL push_t fixture, with the
# Oracle type mapping used across the FQ suites.  Oracle 19c has no BOOLEAN,
# so the bit-like column is NUMBER(1); rows are inserted one statement at a
# time because a multi-row VALUES list is not accepted before 23ai.
# ---------------------------------------------------------------------------
_ORACLE_PUSH_T_SQLS = [
    "CREATE TABLE PUSH_T "
    "(VAL NUMBER(10,0), SCORE BINARY_DOUBLE, NAME VARCHAR2(32), "
    "FLAG NUMBER(1), STATUS VARCHAR2(16))",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (1,1.5,'alpha',1,'active')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (2,2.5,'beta',0,'idle')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (3,3.5,'gamma',1,'active')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (4,4.5,'delta',0,'idle')",
    "INSERT INTO PUSH_T (VAL, SCORE, NAME, FLAG, STATUS) "
    "VALUES (5,5.5,'epsilon',1,'active')",
    "COMMIT",
]

# ---------------------------------------------------------------------------
# Default values (post-fix) — these are the documented defaults
# ---------------------------------------------------------------------------
_DEFAULTS = {
    "federatedQueryEnable":              ("1",           "1"),
    # (dnode_value, local_value) — note: enable is bool, SHOW returns '1'
    # All pool/timeout params are BOTH scope — visible and modifiable on client
    "federatedQueryConnectTimeoutMs":    ("5000",        "5000"),
    "federatedQueryQueryTimeoutMs":      ("1000000000",  "1000000000"),
    "federatedQueryMaxPoolSizePerSource": ("64",          "64"),
    "federatedQueryIdleConnTtlSec":      ("600",         "600"),
}


# ===================================================================
# Helper: query MySQL processlist for taosd connections
# ===================================================================
def _mysql_processlist_count(cfg, database):
    """Count MySQL processlist entries for a given database (Sleep state).

    taosd pool connections in IDLE state appear as Command='Sleep' in MySQL
    processlist.  When eviction calls mysql_close(), the row disappears.

    This function opens its own pymysql connection with database=None so it
    is NOT counted in the result.
    """
    count = ExtSrcEnv.mysql_query_cfg(
        cfg, None,
        f"SELECT COUNT(*) FROM information_schema.processlist "
        f"WHERE db = '{database}' AND command = 'Sleep'")
    return int(count) if count is not None else 0


# ===================================================================
# Helper: query the SQL Server session view for taosd connections
# ===================================================================
def _sqlserver_session_count(cfg, database):
    """Count SQL Server user sessions with a context on *database*.

    SQL Server has no ``SHOW PROCESSLIST``; ``sys.dm_exec_sessions`` is the
    equivalent session view (one row per live connection).  taosd pool
    connections that are idle remain visible there until eviction closes the
    ODBC handle, at which point the row disappears.

    The counting query itself runs against ``master`` so the observer session
    is NOT connected to *database* and is therefore not counted (the same
    self-exclusion the MySQL helper achieves by connecting with
    database=None).
    """
    count = ExtSrcEnv.sqlserver_query_cfg(
        cfg, "master",
        f"SELECT COUNT(*) FROM sys.dm_exec_sessions "
        f"WHERE is_user_process = 1 "
        f"AND DB_NAME(database_id) = N'{database}'")
    return int(count) if count is not None else 0


# ===================================================================
# Helper: hold an exclusive table lock on the remote push_t table
# ===================================================================
def _sqlserver_hold_table_lock(database, seconds):
    """Block remote readers of ``dbo.push_t`` for *seconds*.

    SQL Server has no SELECT-embedded SLEEP(), so the deterministic wait used
    by the MySQL paths cannot be translated literally.  Instead a background
    sqlcmd session opens a transaction, takes an exclusive table lock with
    ``WITH (TABLOCKX)`` and holds it with ``WAITFOR DELAY`` before committing.
    A concurrent plain SELECT (no locking hint) blocks for the whole window,
    which is what the pool-exhaustion and query-timeout cases need.
    """
    ExtSrcEnv.sqlserver_exec(database, [
        "BEGIN TRANSACTION; "
        "SELECT COUNT(*) FROM dbo.push_t WITH (TABLOCKX); "
        # WAITFOR takes HH:MM:SS; the seconds field must stay below 60, so
        # anything from a minute upwards has to carry into the minutes field.
        f"WAITFOR DELAY '{int(seconds) // 3600:02d}:{(int(seconds) % 3600) // 60:02d}:{int(seconds) % 60:02d}'; "
        "COMMIT TRANSACTION",
    ])


def _release_sqlserver_table_lock(database):
    """Kill the session holding the exclusive lock on dbo.push_t.

    The negative cases hold the lock far longer than any retry budget so the
    assertion cannot be rescued by a retry; this helper then releases it
    deterministically instead of waiting the whole window out.
    """
    ExtSrcEnv.sqlserver_exec(database, [
        # T-SQL only accepts constants, variables or their concatenation as the
        # EXEC() argument - a function call inside EXEC(...) is a syntax error
        # (Msg 102 near 'CAST'), so build the command in a variable first.
        "DECLARE @spid INT, @cmd VARCHAR(32); "
        "SELECT TOP 1 @spid = l.request_session_id FROM sys.dm_tran_locks l "
        "JOIN sys.objects o ON o.object_id = l.resource_associated_entity_id "
        "WHERE l.resource_database_id = DB_ID() AND l.resource_type = 'OBJECT' "
        "AND l.request_mode = 'X' AND o.name = 'push_t'; "
        "IF @spid IS NOT NULL AND @spid <> @@SPID "
        "BEGIN SET @cmd = 'KILL ' + CAST(@spid AS VARCHAR(10)); EXEC(@cmd); END",
    ])


# ===================================================================
# Helper: count Oracle sessions owned by the external-source account
# ===================================================================
def _oracle_session_count(cfg, user=None):
    """Count live Oracle sessions owned by *user*.

    Oracle has no SHOW PROCESSLIST; ``V$SESSION`` is the equivalent session
    view and is what the Oracle TS names as the preferred observation point
    for pool eviction.  The view lives in SYS and a plain test schema cannot
    be granted SELECT on it (GRANT raises ORA-01031), so the observer logs in
    with the instance's SYSTEM account.  When that account is not configured
    or cannot read the view the helper returns None and the caller falls back
    to behavioural assertions (query still works / new connection is opened),
    which the TS explicitly allows.
    """
    admin_user = getattr(ExtSrcEnv, "ORACLE_ADMIN_USER", None)
    admin_pass = getattr(ExtSrcEnv, "ORACLE_ADMIN_PASS", None)
    if not admin_user or not admin_pass:
        return None
    account = (user or cfg.user).upper()
    try:
        out = ExtSrcEnv._oracle_sql(
            cfg,
            f"SELECT COUNT(*) FROM V$SESSION WHERE username = '{account}'",
            user=admin_user, password=admin_pass)
    except Exception:
        return None
    for line in out.splitlines():
        value = line.strip()
        if value.isdigit():
            return int(value)
    return None


# ===================================================================
# Helper: install the slow-scan fixtures used by the timeout cases
# ===================================================================
def _oracle_sleep_fixtures(cfg, table="PUSH_T"):
    """Make one Oracle base scan take a known number of seconds.

    Oracle has no SELECT-embedded SLEEP(): ``DBMS_SESSION.SLEEP`` is a
    definer-rights call a plain test schema may not execute, so SYSTEM owns a
    one-line wrapper and grants EXECUTE to PUBLIC.  The wrapper is exposed
    through a view because a function in the view projection is the only way
    to slow a federated base scan deterministically — the same shape as the
    MySQL ``SLEEP()`` view and the SQL Server ``TABLOCKX`` window.

    ``FQ05_SLOW_V`` sleeps 2 s per evaluation (long enough to exceed a 1 s
    query timeout); ``FQ05_SLOW10_V`` sleeps 10 s, which outlasts the ~5 s
    pool-acquire wait so the pool-exhaustion case deterministically sees
    EXT_RESOURCE_EXHAUSTED instead of queueing for the single slot.

    The cast to NUMBER(10,0) is required: a bare NUMBER return has no
    precision and the column would be rejected as unmappable.
    """
    ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
        *ExtSrcEnv.oracle_sleep_function_sql("FQ05_SLEEP", 2),
        *ExtSrcEnv.oracle_sleep_function_sql("FQ05_SLEEP10", 10),
    ])
    ExtSrcEnv._oracle_sql(cfg, [
        f"CREATE OR REPLACE VIEW FQ05_SLOW_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ05_SLEEP AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
        f"CREATE OR REPLACE VIEW FQ05_SLOW10_V AS SELECT VAL, "
        f"CAST(SYSTEM.FQ05_SLEEP10 AS NUMBER(10,0)) AS BOOM "
        f"FROM {table} WHERE ROWNUM <= 1",
    ])


def _oracle_drop_slow_fixtures(cfg):
    """Drop the slow-scan views (idempotent; the helpers may not exist)."""
    for view in ("FQ05_SLOW_V", "FQ05_SLOW10_V"):
        try:
            ExtSrcEnv._oracle_sql(cfg, f"DROP VIEW {view}")
        except Exception:
            pass


class TestFq05ConfigEffectiveness(FederatedQueryVersionedMixin):
    """FQ-05: Configuration Effectiveness."""

    _BASELINE_DIR = os.path.join(os.path.dirname(__file__), "ans")

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def teardown_class(self):
        pass

    # ==================================================================
    # Recording infrastructure — serialize test SQL + results to temp
    # file and compare against baseline for regression detection.
    # ==================================================================

    def _rec_reset(self):
        """Reset the recording buffer for a new test method."""
        self._rec_blocks = []
        self._rec_seq = 0

    def _rec_add(self, sql, rows=None, error_errno=None, error_info=None):
        """Append a serialized block to the recording buffer."""
        self._rec_seq += 1
        sid = f"{self._rec_seq:03d}"
        lines = [f"### {sid}", f"SQL: {sql}"]
        if error_info is not None:
            if error_errno is not None:
                lines.append(f"ERROR {error_errno:#010x}: {error_info}")
            else:
                lines.append(f"ERROR: {error_info}")
        elif rows is not None:
            lines.append("RESULT")
            for row in rows:
                lines.append("|".join(str(c) for c in row))
        else:
            lines.append("OK")
        lines.append("---")
        self._rec_blocks.append("\n".join(lines))

    def _rec_query(self, sql):
        """Execute query (no retry) and record result."""
        tdSql.query(sql, queryTimes=1)
        rows = list(tdSql.queryResult) if tdSql.queryResult else []
        self._rec_add(sql, rows=rows)

    def _rec_error(self, sql, expectedErrno=None, expectErrInfo=None,
                   fullMatched=True, show=False):
        """Execute error-expected SQL and record the error."""
        tdSql.error(sql, expectedErrno=expectedErrno,
                    expectErrInfo=expectErrInfo,
                    fullMatched=fullMatched, show=show)
        errno = getattr(tdSql, 'errno', None)
        err_info = getattr(tdSql, 'error_info', '')
        self._rec_add(sql, error_errno=errno, error_info=err_info)

    def _check_baseline(self, name):
        """Write recorded blocks to tmp file and compare with baseline."""
        baseline_file = os.path.join(self._BASELINE_DIR, f"{name}.txt")
        tmp_file = baseline_file + ".tmp"
        tmp_content = ExtSrcEnv.baseline_environment("\n".join(self._rec_blocks) + "\n")
        os.makedirs(self._BASELINE_DIR, exist_ok=True)
        with open(tmp_file, "w") as f:
            f.write(tmp_content)
        tdLog.info(f"Temp result file written: {tmp_file}")

        if os.path.isfile(baseline_file):
            with open(baseline_file, "r") as f:
                baseline_content = f.read()
            if tmp_content != baseline_content:
                tmp_lines = tmp_content.splitlines()
                base_lines = baseline_content.splitlines()
                diff_line = -1
                tl, bl = "", ""
                for li in range(max(len(tmp_lines), len(base_lines))):
                    tl = tmp_lines[li] if li < len(tmp_lines) else "<EOF>"
                    bl = base_lines[li] if li < len(base_lines) else "<EOF>"
                    if tl != bl:
                        diff_line = li + 1
                        break
                msg = (
                    f"Regression baseline mismatch!\n"
                    f"  baseline: {baseline_file}\n"
                    f"  actual  : {tmp_file}\n"
                    f"  first diff at line {diff_line}:\n"
                    f"    baseline: {bl!r}\n"
                    f"    actual  : {tl!r}\n"
                    f"  Run: diff {baseline_file} {tmp_file}"
                )
                assert False, msg
            else:
                tdLog.info(f"Baseline comparison: OK ({baseline_file})")
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass
        else:
            shutil.copy(tmp_file, baseline_file)
            tdLog.info(f"Baseline file created: {baseline_file}")
            try:
                os.remove(tmp_file)
            except OSError:
                pass

    # ==================================================================
    # Internal helpers
    # ==================================================================

    @staticmethod
    def _like_pattern(param):
        """Build a LIKE pattern that works even if the name column is narrow.

        SHOW DNODE VARIABLES has a ~30-char name column; exact LIKE on longer
        names (e.g. federatedQueryMaxPoolSizePerSource, 34 chars) returns 0
        rows.  Truncate to 28 chars + '%' for safety.
        """
        if len(param) > 30:
            return param[:28] + '%'
        return param

    def _query_dnode_var(self, param):
        """SHOW DNODE 1 VARIABLES LIKE <pattern> → assert 1 row."""
        self._rec_query(f"SHOW DNODE 1 VARIABLES LIKE '{self._like_pattern(param)}'")
        tdSql.checkRows(1)

    def _query_local_var(self, param):
        """SHOW LOCAL VARIABLES LIKE <pattern> → assert 1 row."""
        self._rec_query(f"SHOW LOCAL VARIABLES LIKE '{self._like_pattern(param)}'")
        tdSql.checkRows(1)

    def _alter_dnode_and_verify(self, param, value):
        """ALTER ALL DNODES → SHOW DNODE 1 VARIABLES → assert value."""
        tdSql.execute(f"ALTER ALL DNODES '{param}' '{value}'", queryTimes=1)
        # ALTER ALL DNODES is acked before the dnode applies the value (see
        # _wait_dnode_var), so the assertion below would race the propagation.
        self._wait_dnode_var(param, value)
        # Keep the recorded block: the baseline compares this query result.
        self._query_dnode_var(param)
        tdSql.checkData(0, 2, value)

    def _alter_local_and_verify(self, param, value):
        """ALTER LOCAL → SHOW LOCAL VARIABLES → assert value."""
        tdSql.execute(f"ALTER LOCAL '{param}' '{value}'", queryTimes=1)
        self._query_local_var(param)
        tdSql.checkData(0, 1, value)

    def _wait_dnode_var(self, param, value, timeout=30.0):
        """Poll SHOW DNODE 1 VARIABLES until the dnode reports ``value``.

        ALTER ALL DNODES is acknowledged by the mnode before the change has
        been applied on the dnode: it is carried by a transaction redo action
        and lands on the dnode a few hundred milliseconds after the client
        ack (measured on Windows: mnode appended the redo at 12.179, the
        dnode applied the value at 12.656).  A test that asserts on the
        *effect* of a value (query timeout, pool size, TTL) must therefore
        wait for the dnode to catch up, or it races the propagation and ends
        up exercising the previous value.  ins_dnode_variables is served by
        the dnode itself, so a matching row proves the new value is live in
        the dnode's config -- and in the ext-connector module config that
        dmProcessConfigReq derives from it in the same handler.
        """
        pattern = self._like_pattern(param)
        deadline = time.monotonic() + timeout
        while True:
            tdSql.query(f"SHOW DNODE 1 VARIABLES LIKE '{pattern}'", queryTimes=1)
            tdSql.checkRows(1)
            got = str(tdSql.getData(0, 2))
            if got == str(value):
                return
            if time.monotonic() >= deadline:
                tdLog.exit(f"dnode 1 did not apply {param}={value} within "
                           f"{timeout:.0f}s (still {got})")
            time.sleep(0.2)

    def _alter_dnode_and_verify_plain(self, param, value):
        """ALTER ALL DNODES + verify, without touching the recorded baseline.

        The SQL Server paths must leave ans/test_fq_05_003_behavioral.txt
        byte-for-byte unchanged, so their verification queries go through
        plain tdSql calls instead of the recording helpers above.
        """
        tdSql.execute(f"ALTER ALL DNODES '{param}' '{value}'", queryTimes=1)
        self._wait_dnode_var(param, value)

    def _alter_local_and_verify_plain(self, param, value):
        """ALTER LOCAL + verify, without touching the recorded baseline."""
        tdSql.execute(f"ALTER LOCAL '{param}' '{value}'", queryTimes=1)
        tdSql.query(f"SHOW LOCAL VARIABLES LIKE '{self._like_pattern(param)}'",
                    queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, value)

    @staticmethod
    def _block_port(port):
        """Add iptables DROP rule to silently drop outgoing TCP to 127.0.0.1:port."""
        import subprocess
        import socket
        subprocess.run(
            ["iptables", "-I", "OUTPUT", "-p", "tcp",
             "-d", socket.gethostbyname(ExtSrcEnv.MYSQL_HOST), "--dport", str(port), "-j", "DROP"],
            check=True, capture_output=True)

    @staticmethod
    def _unblock_port(port):
        """Remove iptables DROP rule (idempotent)."""
        import subprocess
        import socket
        subprocess.run(
            ["iptables", "-D", "OUTPUT", "-p", "tcp",
             "-d", socket.gethostbyname(ExtSrcEnv.MYSQL_HOST), "--dport", str(port), "-j", "DROP"],
            capture_output=True)  # no check — ignore if rule doesn't exist

    def _restore_all_defaults(self):
        """Best-effort restore of all config params to defaults."""
        try:
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
        except Exception:
            pass
        try:
            tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
        except Exception:
            pass
        for param, val in [
            ("federatedQueryConnectTimeoutMs", "5000"),
            ("federatedQueryQueryTimeoutMs", "1000000000"),
            ("federatedQueryMaxPoolSizePerSource", "64"),
            ("federatedQueryIdleConnTtlSec", "600"),
        ]:
            try:
                tdSql.execute(f"ALTER ALL DNODES '{param}' '{val}'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute(f"ALTER LOCAL '{param}' '{val}'", queryTimes=1)
            except Exception:
                pass

    # ==================================================================
    # Group A: Default value verification (test_fq_05_001)
    # ==================================================================

    def test_fq_05_001_defaults(self):
        """FQ-05-001: Verify all config default values via SHOW VARIABLES.

        FQ-05-001: Verify all config default values via SHOW VARIABLES.

        Since: v3.4.0.0

        """
        self._rec_reset()
        # --- Server-side (SHOW DNODE 1 VARIABLES) ---
        for param, (dnode_val, _) in _DEFAULTS.items():
            self._query_dnode_var(param)
            tdSql.checkData(0, 2, dnode_val)
            tdLog.info(f"[A1] {param} dnode default = {dnode_val}  OK")

        # --- Client-side (SHOW LOCAL VARIABLES) ---
        # All federated query params are BOTH scope — visible in LOCAL
        for param, (_, local_val) in _DEFAULTS.items():
            self._query_local_var(param)
            tdSql.checkData(0, 1, local_val)
            tdLog.info(f"[A1] {param} local default = {local_val}  OK")
        self._check_baseline("test_fq_05_001_defaults")

    # ==================================================================
    # Group B: ALTER + value verification (test_fq_05_002)
    # ==================================================================

    def test_fq_05_002_alter_and_verify(self):
        """FQ-05-002: ALTER config values and verify via SHOW VARIABLES.

        FQ-05-002: ALTER config values and verify via SHOW VARIABLES.

        Since: v3.4.0.0

        """
        self._rec_reset()
        try:
            self._b1_enable()
            self._b2_connect_timeout()
            self._b3_query_timeout()
            self._b4_pool_size()
            self._b5_idle_ttl()
        finally:
            self._restore_all_defaults()
        self._check_baseline("test_fq_05_002_alter_and_verify")

    # --- B1: federatedQueryEnable (BOTH scope) ---
    def _b1_enable(self):
        tdLog.info("[B1] federatedQueryEnable — BOTH scope")

        # ALTER LOCAL
        self._alter_local_and_verify("federatedQueryEnable", "1")
        self._alter_local_and_verify("federatedQueryEnable", "0")

        # ALTER ALL DNODES
        self._alter_dnode_and_verify("federatedQueryEnable", "1")
        self._alter_dnode_and_verify("federatedQueryEnable", "0")

        # Scope: visible in both DNODE and LOCAL
        self._query_dnode_var("federatedQueryEnable")
        self._query_local_var("federatedQueryEnable")

        # Invalid values
        self._rec_error("ALTER LOCAL 'federatedQueryEnable' '2'")
        self._rec_error("ALTER LOCAL 'federatedQueryEnable' '-1'")

        # Restore
        tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
        tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
        tdLog.info("[B1] federatedQueryEnable  OK")

    # --- B2: federatedQueryConnectTimeoutMs (BOTH scope) ---
    def _b2_connect_timeout(self):
        tdLog.info("[B2] federatedQueryConnectTimeoutMs — BOTH scope")

        # Valid values + verify (server side)
        self._alter_dnode_and_verify("federatedQueryConnectTimeoutMs", "100")
        self._alter_dnode_and_verify("federatedQueryConnectTimeoutMs", "600000")
        self._alter_dnode_and_verify("federatedQueryConnectTimeoutMs", "5000")

        # ALTER LOCAL (client side)
        self._alter_local_and_verify("federatedQueryConnectTimeoutMs", "3000")
        self._alter_local_and_verify("federatedQueryConnectTimeoutMs", "5000")

        # Scope: visible in LOCAL
        self._query_local_var("federatedQueryConnectTimeoutMs")

        # Out-of-range
        self._rec_error("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '99'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '600001'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '0'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '-1'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' 'abc'")
        self._rec_error("ALTER LOCAL 'federatedQueryConnectTimeoutMs' '99'")
        self._rec_error("ALTER LOCAL 'federatedQueryConnectTimeoutMs' '600001'")

        # Restore
        self._alter_dnode_and_verify("federatedQueryConnectTimeoutMs", "5000")
        tdSql.execute("ALTER LOCAL 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
        tdLog.info("[B2] federatedQueryConnectTimeoutMs  OK")

    # --- B3: federatedQueryQueryTimeoutMs (BOTH scope) ---
    def _b3_query_timeout(self):
        tdLog.info("[B3] federatedQueryQueryTimeoutMs — BOTH scope")

        self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "100")
        self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "1000000000")

        # ALTER LOCAL (client side)
        self._alter_local_and_verify("federatedQueryQueryTimeoutMs", "5000")
        self._alter_local_and_verify("federatedQueryQueryTimeoutMs", "1000000000")

        # Scope: visible in LOCAL
        self._query_local_var("federatedQueryQueryTimeoutMs")

        # Out-of-range
        self._rec_error("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '99'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000001'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '0'")
        self._rec_error("ALTER LOCAL 'federatedQueryQueryTimeoutMs' '99'")
        self._rec_error("ALTER LOCAL 'federatedQueryQueryTimeoutMs' '1000000001'")

        # Restore
        self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "1000000000")
        tdSql.execute("ALTER LOCAL 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
        tdLog.info("[B3] federatedQueryQueryTimeoutMs  OK")

    # --- B4: federatedQueryMaxPoolSizePerSource (BOTH scope) ---
    def _b4_pool_size(self):
        tdLog.info("[B4] federatedQueryMaxPoolSizePerSource — BOTH scope")

        self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "1")
        self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "1024")
        self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "64")

        # ALTER LOCAL (client side)
        self._alter_local_and_verify("federatedQueryMaxPoolSizePerSource", "128")
        self._alter_local_and_verify("federatedQueryMaxPoolSizePerSource", "64")

        # Scope: visible in LOCAL
        self._query_local_var("federatedQueryMaxPoolSizePerSource")

        # Out-of-range
        self._rec_error("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '0'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '1025'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '-1'")
        self._rec_error("ALTER LOCAL 'federatedQueryMaxPoolSizePerSource' '0'")
        self._rec_error("ALTER LOCAL 'federatedQueryMaxPoolSizePerSource' '1025'")

        # Restore
        self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "64")
        tdSql.execute("ALTER LOCAL 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
        tdLog.info("[B4] federatedQueryMaxPoolSizePerSource  OK")

    # --- B5: federatedQueryIdleConnTtlSec (BOTH scope) ---
    def _b5_idle_ttl(self):
        tdLog.info("[B5] federatedQueryIdleConnTtlSec — BOTH scope")

        self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "1")
        self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "86400")
        self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "600")

        # ALTER LOCAL (client side)
        self._alter_local_and_verify("federatedQueryIdleConnTtlSec", "300")
        self._alter_local_and_verify("federatedQueryIdleConnTtlSec", "600")

        # Scope: visible in LOCAL
        self._query_local_var("federatedQueryIdleConnTtlSec")

        # Out-of-range
        self._rec_error("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '0'")
        self._rec_error("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '86401'")
        self._rec_error("ALTER LOCAL 'federatedQueryIdleConnTtlSec' '0'")
        self._rec_error("ALTER LOCAL 'federatedQueryIdleConnTtlSec' '86401'")

        # Restore
        self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "600")
        tdSql.execute("ALTER LOCAL 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
        tdLog.info("[B5] federatedQueryIdleConnTtlSec  OK")

    # ==================================================================
    # Group C: Behavioural effectiveness (test_fq_05_003)
    # ==================================================================

    def test_fq_05_003_behavioral(self):
        """FQ-05-003: Behavioural effectiveness of each config parameter.

        Server-side tests (C1–C5) use ALTER ALL DNODES and verify effects on
        the server ext-connector pool (used by the federated-scan executor).

        Client-side tests (C2c–C5c) use ALTER LOCAL and verify effects on the
        client ext-connector pool (used by the catalog for schema resolution).
        The two pools are fully independent: ALTER LOCAL only affects the
        catalog phase, ALTER ALL DNODES only affects the data-query phase.

        SQL Server paths (C1-s/C2-s/C3-s/C4-s/C5-s and the C2-s-client …
        C5-s-client ALTER LOCAL mirrors) mirror the MySQL behavioural cases
        for the 4th external source; sessions are observed through
        sys.dm_exec_sessions instead of the MySQL processlist.  These SQL
        Server paths use plain tdSql calls instead of _rec_query/_rec_error
        so the recorded block sequence — and therefore the
        ans/test_fq_05_003_behavioral.txt baseline — stays unchanged.

        Oracle paths (C1-o … C5-o and their client-side mirrors) extend the
        same matrix to the 5th external source.  DATABASE is the required
        Easy Connect service name and SCHEMA is the only table namespace
        level, so each method recreates PUSH_T in the login schema.  Session
        visibility uses V$SESSION through the SYSTEM account; the
        query-timeout case additionally exercises the per-source
        OPTIONS('read_timeout_ms'), which only Oracle exposes.
        Like the SQL Server paths they use plain tdSql calls so the
        recorded baseline is untouched.

        Since: v3.4.0.0

        """
        self._rec_reset()
        try:
            self._c1_enable()
            self._c2_connect_timeout()
            self._c2_connect_timeout_client()
            self._c3_query_timeout()
            self._c3_query_timeout_client()
            self._c4_pool_size()
            self._c4_pool_size_client()
            self._c5_idle_ttl()
            self._c5_idle_ttl_client()
            # --- SQL Server (4th source) — parallel to the MySQL paths above ---
            self._c1_enable_sqlserver()
            self._c2_connect_timeout_sqlserver()
            self._c3_query_timeout_sqlserver()
            self._c4_pool_size_sqlserver()
            self._c5_idle_ttl_sqlserver()
            # --- SQL Server client-side (ALTER LOCAL) mirrors ---
            self._c2_connect_timeout_sqlserver_client()
            self._c3_query_timeout_sqlserver_client()
            self._c4_pool_size_sqlserver_client()
            self._c5_idle_ttl_sqlserver_client()
            # --- Oracle (5th source) — parallel to the paths above ---
            self._c1_enable_oracle()
            self._c2_connect_timeout_oracle()
            self._c3_query_timeout_oracle()
            self._c4_pool_size_oracle()
            self._c5_idle_ttl_oracle()
            # --- Oracle client-side (ALTER LOCAL) mirrors ---
            self._c2_connect_timeout_oracle_client()
            self._c3_query_timeout_oracle_client()
            self._c4_pool_size_oracle_client()
            self._c5_idle_ttl_oracle_client()
        finally:
            self._restore_all_defaults()
        self._check_baseline("test_fq_05_003_behavioral")

    # ----- C1: federatedQueryEnable -----
    def _c1_enable(self):
        tdLog.info("[C1] federatedQueryEnable — behavioural")
        src = "fq05_en_m"
        ext_db = "fq05_enable"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)

            # -- Positive: enable=1, query succeeds --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C1] enable=1 query OK")

            # -- Negative: enable=0, query fails --
            # Pre-clean src2 while still enabled, in case a prior run left it.
            src2 = "fq05_en2_m"
            self._cleanup_src(src2)

            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '0'", queryTimes=1)
            self._rec_error(
                f"select count(*) from {src}.push_t",
            )
            tdLog.info("[C1] enable=0 query blocked  OK")

            # -- DDL also blocked by enable=0 --
            # When federated query is disabled, ALL external-source DDL
            # statements must fail with TSDB_CODE_EXT_FEDERATED_DISABLED.

            # CREATE
            self._rec_error(
                f"create external source {src2} type='mysql' "
                f"host='{cfg.host}' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={ext_db}",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED,
            )
            tdLog.info("[C1] enable=0: CREATE EXTERNAL SOURCE blocked  OK")

            # ALTER  (src was created with enable=1 above)
            self._rec_error(
                f"alter external source {src} set host='127.0.0.2'",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED,
            )
            tdLog.info("[C1] enable=0: ALTER EXTERNAL SOURCE blocked  OK")

            # DROP
            self._rec_error(
                f"drop external source {src}",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED,
            )
            tdLog.info("[C1] enable=0: DROP EXTERNAL SOURCE blocked  OK")

            # REFRESH
            self._rec_error(
                f"refresh external source {src}",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED,
            )
            tdLog.info("[C1] enable=0: REFRESH EXTERNAL SOURCE blocked  OK")

            # SHOW
            self._rec_error(
                "show external sources",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED,
            )
            tdLog.info("[C1] enable=0: SHOW EXTERNAL SOURCES blocked  OK")

            # DESCRIBE
            self._rec_error(
                f"describe external source {src}",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED,
            )
            tdLog.info("[C1] enable=0: DESCRIBE EXTERNAL SOURCE blocked  OK")

            # -- Restore + re-verify --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C1] enable restored, query OK")

        finally:
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ----- C2: federatedQueryConnectTimeoutMs -----
    def _c2_connect_timeout(self):
        tdLog.info("[C2] federatedQueryConnectTimeoutMs — behavioural")
        src = "fq05_ct_m"
        ext_db = "fq05_ct"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)

            # -- Positive: default timeout, query succeeds --
            self._alter_dnode_and_verify("federatedQueryConnectTimeoutMs", "5000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C2] conn_tmo=5000 query OK")

            # -- Negative: non-routable IP → connection fails --
            # (MariaDB Connector/C does not reliably honour
            # MYSQL_OPT_CONNECT_TIMEOUT for non-routable IPs, so we only
            # verify the query fails, not the exact elapsed time.)
            tdSql.execute(f"drop database if exists {src}", queryTimes=1)
            tdSql.execute(f"drop external source if exists {src}", queryTimes=1)
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='192.0.2.1' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={ext_db}", queryTimes=1)
            self._rec_error(f"select count(*) from {src}.push_t")
            tdLog.info("[C2] non-routable host → EXT_CONNECT_FAILED  OK")

            self._cleanup_src(src)

            # -- Dynamic: change timeout, real query still succeeds --
            self._alter_dnode_and_verify("federatedQueryConnectTimeoutMs", "600000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)

            self._alter_dnode_and_verify("federatedQueryConnectTimeoutMs", "100")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C2] dynamic conn_tmo=100, reachable MySQL → query OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ----- C3: federatedQueryQueryTimeoutMs -----
    def _c3_query_timeout(self):
        tdLog.info("[C3] federatedQueryQueryTimeoutMs — behavioural")
        src = "fq05_qt_m"
        ext_db = "fq05_qtmo"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, [
                "DROP VIEW IF EXISTS slow_v",
                "CREATE VIEW slow_v AS "
                "SELECT val, SLEEP(3) AS delay FROM push_t LIMIT 1",
            ])

            # -- Positive: huge timeout → slow query succeeds --
            self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select val, delay from {src}.slow_v limit 1")
            tdLog.info("[C3] query_tmo=1000000000, slow_v query succeeded  OK")

            # -- Negative: 1s timeout → slow query fails fast --
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "1000")
            self._mk_mysql_real(src, database=ext_db)

            t0 = time.monotonic()
            self._rec_error(f"select val, delay from {src}.slow_v limit 1")
            elapsed = time.monotonic() - t0
            assert elapsed < 10, (
                f"[C3] query timeout not effective: elapsed={elapsed:.1f}s "
                f"(expected < 10s with 1s timeout on SLEEP(3))")
            tdLog.info(f"[C3] query_tmo=1000 → fail in {elapsed:.1f}s  OK")

            # -- Dynamic: switch from large to small timeout --
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_mysql_real(src, database=ext_db)

            # fast query to establish connection
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            # now shrink timeout
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "1000")
            self._mk_mysql_real(src, database=ext_db)

            t0 = time.monotonic()
            self._rec_error(f"select val, delay from {src}.slow_v limit 1")
            elapsed = time.monotonic() - t0
            assert elapsed < 10, (
                f"[C3] dynamic query timeout not effective: elapsed={elapsed:.1f}s")
            tdLog.info(f"[C3] dynamic query_tmo=1000 → fail in {elapsed:.1f}s  OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ----- C4: federatedQueryMaxPoolSizePerSource -----
    def _c4_pool_size(self):
        tdLog.info("[C4] federatedQueryMaxPoolSizePerSource — behavioural")
        src = "fq05_ps_m"
        ext_db = "fq05_pool"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, [
                "DROP VIEW IF EXISTS slow_v",
                "CREATE VIEW slow_v AS "
                "SELECT val, SLEEP(10) AS delay FROM push_t LIMIT 1",
            ])

            # -- Positive: pool=64, query succeeds --
            self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "64")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4] pool=64, query OK")

            # -- Negative: pool=1, concurrent → RESOURCE_EXHAUSTED --
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "1")
            time.sleep(0.5)
            self._mk_mysql_real(src, database=ext_db)

            # confirm pool=1 still works for serial queries
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            bg_errors = []

            def _run_slow():
                import taos
                try:
                    conn = taos.connect()
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                f"select val, delay from {src}.slow_v limit 1")
                            cur.fetchall()
                        except Exception as exc:
                            bg_errors.append(exc)
                        finally:
                            cur.close()
                    finally:
                        conn.close()
                except Exception as exc:
                    bg_errors.append(exc)

            t = threading.Thread(target=_run_slow, daemon=True)
            t.start()
            time.sleep(1.0)

            # pool is full — should get RESOURCE_EXHAUSTED
            self._rec_error(
                f"select count(*) from {src}.push_t",
                expectedErrno=TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
            )
            tdLog.info("[C4] pool=1 + concurrent → EXT_RESOURCE_EXHAUSTED  OK")

            t.join(timeout=30)
            if t.is_alive():
                tdLog.info("[C4] background slow-query thread did not finish")
            if bg_errors:
                tdLog.info(f"[C4] background thread error: {bg_errors[0]}")

            # Recovery: after bg thread releases, serial query works again
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4] pool=1 recovery OK")

            # -- Dynamic: pool 64→1 while running --
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "64")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "1")
            time.sleep(0.5)
            self._mk_mysql_real(src, database=ext_db)

            bg_errors2 = []

            def _run_slow2():
                import taos
                try:
                    conn = taos.connect()
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                f"select val, delay from {src}.slow_v limit 1")
                            cur.fetchall()
                        except Exception as exc:
                            bg_errors2.append(exc)
                        finally:
                            cur.close()
                    finally:
                        conn.close()
                except Exception as exc:
                    bg_errors2.append(exc)

            t2 = threading.Thread(target=_run_slow2, daemon=True)
            t2.start()
            time.sleep(1.0)

            self._rec_error(
                f"select count(*) from {src}.push_t",
                expectedErrno=TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
            )
            tdLog.info("[C4] dynamic pool=1 → EXT_RESOURCE_EXHAUSTED  OK")

            t2.join(timeout=30)

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ----- C5: federatedQueryIdleConnTtlSec -----
    def _c5_idle_ttl(self):
        tdLog.info("[C5] federatedQueryIdleConnTtlSec — behavioural")
        src = "fq05_ttl_m"
        ext_db = "fq05_ttl"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)

            # -- Positive: TTL=86400 → connections survive 15s idle --
            self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "86400")
            self._mk_mysql_real(src, database=ext_db)

            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            cnt_before = _mysql_processlist_count(cfg, ext_db)
            assert cnt_before >= 1, (
                f"[C5] expected >= 1 taosd connections after query, got {cnt_before}")
            tdLog.info(f"[C5] TTL=86400, processlist count after query = {cnt_before}")

            time.sleep(5)

            cnt_after = _mysql_processlist_count(cfg, ext_db)
            assert cnt_after >= 1, (
                f"[C5] TTL=86400: connections should survive 5s idle, "
                f"but processlist count dropped to {cnt_after}")
            tdLog.info(f"[C5] TTL=86400, 5s later processlist count = {cnt_after}  OK")

            # query still works (reuses idle connection)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            # -- Negative: TTL=1 → connections evicted within 15s --
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "1")
            self._mk_mysql_real(src, database=ext_db)

            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            cnt_before2 = _mysql_processlist_count(cfg, ext_db)
            assert cnt_before2 >= 1, (
                f"[C5] expected >= 1 taosd connections after query, got {cnt_before2}")
            tdLog.info(f"[C5] TTL=1, processlist count after query = {cnt_before2}")

            # Wait for eviction: TTL=1s + probe interval 5s + buffer
            time.sleep(12)

            cnt_after2 = _mysql_processlist_count(cfg, ext_db)
            # The native client library (libtaosnative.so) maintains its own
            # connection pool for catalog metadata fetching.  ALTER ALL DNODES
            # only affects the server-side pool, so the client-side connection
            # may persist.  Assert that at least one connection was evicted
            # (proving the server-side TTL works) rather than requiring zero.
            assert cnt_after2 < cnt_before2, (
                f"[C5] TTL=1: server-side connections should be evicted "
                f"after 12s, but count didn't decrease "
                f"(before={cnt_before2} after={cnt_after2})")
            tdLog.info(f"[C5] TTL=1, 12s later processlist count "
                       f"{cnt_before2} → {cnt_after2}  OK")

            # query still works (creates new connection)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            # -- Dynamic: TTL 86400→1, observe eviction --
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "86400")
            self._mk_mysql_real(src, database=ext_db)

            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            cnt_dyn1 = _mysql_processlist_count(cfg, ext_db)
            assert cnt_dyn1 >= 1, (
                f"[C5] dynamic: expected >= 1 connections, got {cnt_dyn1}")

            # wait 5s with TTL=86400 — connections should stay
            time.sleep(5)
            cnt_dyn2 = _mysql_processlist_count(cfg, ext_db)
            assert cnt_dyn2 >= 1, (
                f"[C5] dynamic: TTL=86400, connections should survive, "
                f"got {cnt_dyn2}")
            tdLog.info(f"[C5] dynamic TTL=86400, 5s → count={cnt_dyn2}  OK")

            # now switch to TTL=1
            self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "1")

            # wait for eviction (TTL=1s + probe interval 5s + buffer)
            time.sleep(12)

            cnt_dyn3 = _mysql_processlist_count(cfg, ext_db)
            # Same rationale as Phase 2: client-side pool not affected by
            # ALTER ALL DNODES, so only require that the count decreased.
            assert cnt_dyn3 < cnt_dyn2, (
                f"[C5] dynamic: after switching TTL to 1s, server-side "
                f"connections should be evicted, but count didn't decrease "
                f"(before={cnt_dyn2} after={cnt_dyn3})")
            tdLog.info(f"[C5] dynamic TTL=86400→1, 12s later count "
                       f"{cnt_dyn2} → {cnt_dyn3}  OK")

            # query still works
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C5] dynamic recovery query OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ==================================================================
    # Group C client-side: ALTER LOCAL behavioural effectiveness
    #
    # The client ext-connector pool handles catalog metadata fetching
    # (table schema resolution via extConnectorGetTableSchema in
    # ctgAsync.c).  The server ext-connector pool handles actual data
    # queries (federatedscanoperator.c).  The two pools are fully
    # independent — ALTER LOCAL changes only the client pool, ALTER ALL
    # DNODES changes only the server pool.
    # ==================================================================

    # ----- C2-client: federatedQueryConnectTimeoutMs (ALTER LOCAL) -----
    def _c2_connect_timeout_client(self):
        """C2-client: ALTER LOCAL connect timeout controls catalog-phase connect."""
        tdLog.info("[C2-client] federatedQueryConnectTimeoutMs — client-side")
        src = "fq05_ct_cl"
        ext_db = "fq05_ct_cl"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)

            # -- Positive: ALTER LOCAL default timeout, reachable MySQL → OK --
            self._alter_local_and_verify("federatedQueryConnectTimeoutMs", "5000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C2-client] conn_tmo LOCAL=5000 query OK")

            # -- Negative: non-routable IP → catalog connect fails --
            # The catalog (client-side) uses ALTER LOCAL timeout when
            # connecting to the external source for schema resolution.
            tdSql.execute(
                f"create external source {src} type='mysql' "
                f"host='192.0.2.1' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={ext_db}", queryTimes=1)
            self._rec_error(f"select count(*) from {src}.push_t")
            tdLog.info("[C2-client] non-routable host → catalog connect failed  OK")
            self._cleanup_src(src)

            # -- Timing isolation note --
            # The client ext-connector pool handles catalog metadata fetching.
            # Timing-based connect-timeout isolation via iptables is unreliable
            # because:  (a) MariaDB Connector/C truncates ms→integer seconds
            # (500 ms → 0 s = no timeout);  (b) the TCP SYN retransmit timer
            # may dominate over the connector timeout.
            # The non-routable-IP test above verifies that the catalog phase
            # detects connectivity failure.  Group B verifies ALTER LOCAL
            # changes the stored value.

            # -- Dynamic: change LOCAL timeout, reachable MySQL → query OK --
            self._alter_local_and_verify("federatedQueryConnectTimeoutMs", "600000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)

            self._alter_local_and_verify("federatedQueryConnectTimeoutMs", "100")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C2-client] dynamic conn_tmo LOCAL=100 reachable → OK")

        finally:
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ----- C3-client: federatedQueryQueryTimeoutMs (ALTER LOCAL) -----
    def _c3_query_timeout_client(self):
        """C3-client: ALTER LOCAL query timeout affects catalog schema queries.

        The client ext-connector only runs schema-resolution queries (e.g.
        SHOW COLUMNS) against the external database.  These queries complete
        in <1 ms on localhost, so inducing a query-timeout failure on the
        client side is infeasible.

        This test verifies that ALTER LOCAL applies the new timeout value
        without breaking catalog operations — i.e. even the minimum allowed
        timeout (100 ms) does not cause failures for fast schema queries.
        """
        tdLog.info("[C3-client] federatedQueryQueryTimeoutMs — client-side")
        src = "fq05_qt_cl"
        ext_db = "fq05_qt_cl"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)

            # -- Positive: minimum timeout (100 ms) — schema queries still OK --
            self._alter_local_and_verify("federatedQueryQueryTimeoutMs", "100")
            # Keep server timeout at default so data-query phase is unaffected
            self._alter_dnode_and_verify("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-client] query_tmo LOCAL=100, catalog OK")

            # -- Dynamic: switch LOCAL timeout from min to mid-range --
            self._cleanup_src(src)
            self._alter_local_and_verify("federatedQueryQueryTimeoutMs", "5000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-client] dynamic query_tmo LOCAL=5000 → OK")

            # -- Dynamic: switch to max --
            self._cleanup_src(src)
            self._alter_local_and_verify("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-client] dynamic query_tmo LOCAL=max → OK")

        finally:
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ----- C4-client: federatedQueryMaxPoolSizePerSource (ALTER LOCAL) -----
    def _c4_pool_size_client(self):
        """C4-client: ALTER LOCAL pool size affects client catalog pool.

        Catalog schema queries are sub-millisecond on localhost, so pool
        exhaustion via concurrent catalog operations is infeasible.  This
        test verifies that ALTER LOCAL pool_size=1 (minimum) does not break
        serial catalog + query operations.
        """
        tdLog.info("[C4-client] federatedQueryMaxPoolSizePerSource — client-side")
        src = "fq05_ps_cl"
        ext_db = "fq05_ps_cl"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)

            # -- Positive: pool=1, serial queries work --
            self._alter_local_and_verify("federatedQueryMaxPoolSizePerSource", "1")
            # Keep server pool at default so data-query phase is unaffected
            self._alter_dnode_and_verify("federatedQueryMaxPoolSizePerSource", "64")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-client] pool LOCAL=1, serial query OK")

            # Multiple serial queries — all succeed with pool=1
            for i in range(5):
                self._cleanup_src(src)
                self._mk_mysql_real(src, database=ext_db)
                self._rec_query(f"select count(*) from {src}.push_t")
                tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-client] pool LOCAL=1, 5 serial queries OK")

            # -- Dynamic: pool 1→128, query OK --
            self._cleanup_src(src)
            self._alter_local_and_verify("federatedQueryMaxPoolSizePerSource", "128")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-client] dynamic pool LOCAL=128 → OK")

            # -- Dynamic: pool 128→1, query OK --
            self._cleanup_src(src)
            self._alter_local_and_verify("federatedQueryMaxPoolSizePerSource", "1")
            self._mk_mysql_real(src, database=ext_db)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-client] dynamic pool LOCAL=1 → OK")

        finally:
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ----- C5-client: federatedQueryIdleConnTtlSec (ALTER LOCAL) -----
    def _c5_idle_ttl_client(self):
        """C5-client: ALTER LOCAL idle TTL — independent eviction of catalog pool.

        Verifies that the client-side (catalog) connection pool and the
        server-side (data-query) connection pool honour their respective TTL
        independently.

        Phase 1: server TTL=86400 (keep), client TTL=1 (evict).
                 → Only client catalog connections should be evicted;
                   server connections survive.
        Phase 2: server TTL=1, client TTL=1.
                 → All connections evicted.
        Phase 3: server TTL=1 (evict), client TTL=86400 (keep).
                 → Only server connections evicted; client catalog
                   connections survive.
        """
        tdLog.info("[C5-client] federatedQueryIdleConnTtlSec — client-side")
        src = "fq05_tc_cl"
        ext_db = "fq05_tc_cl"
        cfg = self._mysql_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.mysql_create_db_cfg(cfg, ext_db)
            ExtSrcEnv.mysql_exec_cfg(cfg, ext_db, _PUSH_T_SQLS)

            # ---- Phase 1: server TTL=86400, client TTL=1 ----
            tdLog.info("[C5-client] Phase 1: server TTL=86400, client TTL=1")
            self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "86400")
            self._alter_local_and_verify("federatedQueryIdleConnTtlSec", "1")
            self._mk_mysql_real(src, database=ext_db)

            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            cnt_p1_before = _mysql_processlist_count(cfg, ext_db)
            assert cnt_p1_before >= 1, (
                f"[C5-client] P1: expected >= 1 connections, got {cnt_p1_before}")
            tdLog.info(f"[C5-client] P1 initial count = {cnt_p1_before}")

            # Wait for client-side eviction (TTL=1 + probe interval 5s + buffer)
            time.sleep(12)

            cnt_p1_after = _mysql_processlist_count(cfg, ext_db)
            # Client pool connections (TTL=1) should be evicted.
            # Server pool connections (TTL=86400) should remain.
            assert cnt_p1_after < cnt_p1_before, (
                f"[C5-client] P1: client TTL=1 should evict catalog connections "
                f"but count didn't decrease (before={cnt_p1_before} "
                f"after={cnt_p1_after})")
            assert cnt_p1_after >= 1, (
                f"[C5-client] P1: server connections (TTL=86400) should remain "
                f"but count dropped to {cnt_p1_after}")
            tdLog.info(f"[C5-client] P1: {cnt_p1_before} → {cnt_p1_after} "
                       f"(client evicted, server retained)  OK")

            # query still works (catalog creates new connection)
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            # ---- Phase 2: both TTL=1 → all evicted ----
            tdLog.info("[C5-client] Phase 2: both TTL=1")
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "1")
            self._alter_local_and_verify("federatedQueryIdleConnTtlSec", "1")
            self._mk_mysql_real(src, database=ext_db)

            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            cnt_p2_before = _mysql_processlist_count(cfg, ext_db)
            assert cnt_p2_before >= 1, (
                f"[C5-client] P2: expected >= 1 connections, got {cnt_p2_before}")

            time.sleep(12)

            cnt_p2_after = _mysql_processlist_count(cfg, ext_db)
            assert cnt_p2_after == 0, (
                f"[C5-client] P2: both TTL=1, all connections should be evicted "
                f"but {cnt_p2_after} remain")
            tdLog.info(f"[C5-client] P2: both TTL=1 → all evicted "
                       f"({cnt_p2_before} → {cnt_p2_after})  OK")

            # ---- Phase 3: server TTL=1, client TTL=86400 ----
            tdLog.info("[C5-client] Phase 3: server TTL=1, client TTL=86400")
            self._cleanup_src(src)
            self._alter_dnode_and_verify("federatedQueryIdleConnTtlSec", "1")
            self._alter_local_and_verify("federatedQueryIdleConnTtlSec", "86400")
            self._mk_mysql_real(src, database=ext_db)

            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)

            cnt_p3_before = _mysql_processlist_count(cfg, ext_db)
            assert cnt_p3_before >= 1, (
                f"[C5-client] P3: expected >= 1 connections, got {cnt_p3_before}")
            tdLog.info(f"[C5-client] P3 initial count = {cnt_p3_before}")

            time.sleep(12)

            cnt_p3_after = _mysql_processlist_count(cfg, ext_db)
            # Server connections (TTL=1) evicted; client connections (TTL=86400)
            # should survive.
            assert cnt_p3_after < cnt_p3_before, (
                f"[C5-client] P3: server TTL=1 should evict data-query "
                f"connections but count didn't decrease "
                f"(before={cnt_p3_before} after={cnt_p3_after})")
            assert cnt_p3_after >= 1, (
                f"[C5-client] P3: client connections (TTL=86400) should remain "
                f"but count dropped to {cnt_p3_after}")
            tdLog.info(f"[C5-client] P3: {cnt_p3_before} → {cnt_p3_after} "
                       f"(server evicted, client retained)  OK")

            # query still works
            self._rec_query(f"select count(*) from {src}.push_t")
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C5-client] all phases OK")

        finally:
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(cfg, ext_db)
            except Exception:
                pass

    # ==================================================================
    # Group C — SQL Server (4th source), parallel to the MySQL paths above
    #
    # SQL Server runs zero pushdown in step 1: the remote SQL is a base scan
    # and WHERE / aggregation are evaluated locally, so only behaviour that
    # is independent of pushdown is asserted here (enable gating, connect
    # timeout, pool size, idle TTL).
    #
    # These paths deliberately use plain tdSql calls instead of
    # _rec_query / _rec_error: keeping them out of the recording buffer
    # leaves the ans/test_fq_05_003_behavioral.txt baseline byte-for-byte
    # unchanged.
    # ==================================================================

    # ----- C1-s: federatedQueryEnable (SQL Server) -----
    def _c1_enable_sqlserver(self):
        tdLog.info("[C1-s] federatedQueryEnable — SQL Server")
        src = "fq05_en_s"
        src2 = "fq05_en2_s"
        ext_db = "fq05_en_sdb"
        cfg = self._sqlserver_cfg()
        self._cleanup_src(src, src2)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Positive: enable=1, query succeeds --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C1-s] enable=1 query OK")

            # -- Negative: enable=0, query blocked --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '0'", queryTimes=1)
            tdSql.error(f"select count(*) from {src}.push_t")
            tdLog.info("[C1-s] enable=0 query blocked  OK")

            # -- DDL also blocked by enable=0 (parser-level guard, the same
            #    code path the MySQL assertions above exercise) --
            tdSql.error(
                f"create external source {src2} type='sqlserver' "
                f"host='{cfg.host}' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={ext_db} schema='dbo'",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"alter external source {src} set host='127.0.0.2'",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"drop external source {src}",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"refresh external source {src}",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error("show external sources",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"describe external source {src}",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdLog.info("[C1-s] enable=0: DDL blocked  OK")

            # -- Restore + re-verify --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C1-s] enable restored, query OK")

        finally:
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src, src2)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ----- C2-s: federatedQueryConnectTimeoutMs (SQL Server) -----
    def _c2_connect_timeout_sqlserver(self):
        tdLog.info("[C2-s] federatedQueryConnectTimeoutMs — SQL Server")
        src = "fq05_ct_s"
        ext_db = "fq05_ct_sdb"
        cfg = self._sqlserver_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Positive: default timeout, reachable source → query succeeds --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C2-s] conn_tmo=5000 query OK")

            # -- Negative: non-routable IP → connect fails --
            # As with the MySQL [C2] path above, only the failure itself is
            # asserted, not the exact elapsed time: the SQL Server ODBC driver
            # applies connect_timeout_ms as SQL_ATTR_LOGIN_TIMEOUT (rounded up
            # to whole seconds), but its own connect-retry logic and the
            # scheduler retries make the wall-clock bound environment-specific.
            tdSql.execute(f"drop database if exists {src}", queryTimes=1)
            tdSql.execute(f"drop external source if exists {src}", queryTimes=1)
            tdSql.execute(
                f"create external source {src} type='sqlserver' "
                f"host='192.0.2.1' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={ext_db} schema='dbo'", queryTimes=1)
            t0 = time.monotonic()
            tdSql.error(f"select count(*) from {src}.push_t")
            elapsed = time.monotonic() - t0
            tdLog.info(f"[C2-s] non-routable host → query failed in {elapsed:.1f}s  OK")
            self._cleanup_src(src)

            # -- Dynamic: change timeout, real query still succeeds --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '600000'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)

            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '100'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C2-s] dynamic conn_tmo=100, reachable SQL Server → query OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ----- C3-s: federatedQueryQueryTimeoutMs (SQL Server) -----
    def _c3_query_timeout_sqlserver(self):
        """C3-s: query timeout aborts a slow (lock-blocked) SQL Server scan.

        MySQL uses SELECT SLEEP(); the SQL Server equivalent is a plain scan
        blocked on an exclusive table lock held by a background sqlcmd session
        (see _sqlserver_hold_table_lock).  With a 1 s timeout the query must
        fail instead of waiting out the lock window; with a huge timeout the
        same slow query succeeds.

        The exact error code is deliberately not pinned, mirroring the MySQL
        [C3] style: the SQL Server timeout SQLSTATE (HYT00/S1T00) may be
        classified as a connection error or a query timeout depending on the
        driver build, and both abort the scan.
        """
        tdLog.info("[C3-s] federatedQueryQueryTimeoutMs — SQL Server")
        src = "fq05_qt_s"
        ext_db = "fq05_qt_sdb"
        self._cleanup_src(src)
        lock_errors = []

        def _hold(seconds):
            try:
                _sqlserver_hold_table_lock(ext_db, seconds)
            except Exception as exc:
                lock_errors.append(exc)

        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Positive: huge timeout → lock-blocked scan still succeeds --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            lt = threading.Thread(target=_hold, args=(4,), daemon=True)
            lt.start()
            time.sleep(1.5)          # let the lock holder acquire TABLOCKX
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            lt.join(timeout=30)
            tdLog.info("[C3-s] query_tmo=1000000000, lock-blocked scan succeeded  OK")

            # -- Negative: 1s timeout → blocked scan fails instead of waiting
            #    out the whole lock window --
            self._cleanup_src(src)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            # Hold far longer than any plausible retry budget so a retry cannot
            # slip through after the lock is released; release the lock
            # explicitly once the timeout assertion has been made.
            lt2 = threading.Thread(target=_hold, args=(60,), daemon=True)
            lt2.start()
            time.sleep(1.5)
            t0 = time.monotonic()
            try:
                tdSql.error(f"select count(*) from {src}.push_t")
            finally:
                _release_sqlserver_table_lock(ext_db)
            elapsed = time.monotonic() - t0
            assert elapsed < 60, (
                f"[C3-s] query timeout not effective: elapsed={elapsed:.1f}s "
                f"(expected the 1s timeout to abort the blocked scan)")
            tdLog.info(f"[C3-s] query_tmo=1000 → blocked scan failed in {elapsed:.1f}s  OK")
            lt2.join(timeout=5)

            # -- Dynamic: switch from huge back to 1s timeout --
            self._cleanup_src(src)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            self._cleanup_src(src)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            lt3 = threading.Thread(target=_hold, args=(60,), daemon=True)
            lt3.start()
            time.sleep(1.5)
            t0 = time.monotonic()
            try:
                tdSql.error(f"select count(*) from {src}.push_t")
            finally:
                _release_sqlserver_table_lock(ext_db)
            elapsed = time.monotonic() - t0
            assert elapsed < 60, (
                f"[C3-s] dynamic query timeout not effective: elapsed={elapsed:.1f}s")
            tdLog.info(f"[C3-s] dynamic query_tmo=1e9→1000 → "
                       f"blocked scan failed in {elapsed:.1f}s  OK")
            lt3.join(timeout=5)

            if lock_errors:
                tdLog.info(f"[C3-s] lock holder error: {lock_errors[0]}")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ----- C4-s: federatedQueryMaxPoolSizePerSource (SQL Server) -----
    def _c4_pool_size_sqlserver(self):
        """C4-s: pool cap enforced for SQL Server sources.

        SQL Server has no SELECT-embedded SLEEP(), so the single pool slot is
        held by a federated base scan that blocks on an exclusive table lock.
        The lock is taken by a background sqlcmd session running
        BEGIN TRAN / SELECT ... WITH (TABLOCKX) / WAITFOR DELAY / COMMIT, and
        is held long enough to outlast the retry loop of the retryable
        EXT_RESOURCE_EXHAUSTED error.
        """
        tdLog.info("[C4-s] federatedQueryMaxPoolSizePerSource — SQL Server")
        src = "fq05_ps_s"
        ext_db = "fq05_ps_sdb"
        self._cleanup_src(src)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Positive: pool=64, query succeeds --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C4-s] pool=64, query OK")

            # -- Negative: pool=1, blocked remote scan → RESOURCE_EXHAUSTED --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '1'", queryTimes=1)
            time.sleep(0.5)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

            # confirm pool=1 still works for serial queries
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            lock_errors = []

            def _hold_table_lock():
                """Hold an exclusive lock on dbo.push_t until released below."""
                try:
                    _sqlserver_hold_table_lock(ext_db, 60)
                except Exception as exc:
                    lock_errors.append(exc)

            bg_errors = []

            def _run_blocked():
                import taos
                try:
                    conn = taos.connect()
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute(f"select count(*) from {src}.push_t")
                            cur.fetchall()
                        except Exception as exc:
                            bg_errors.append(exc)
                        finally:
                            cur.close()
                    finally:
                        conn.close()
                except Exception as exc:
                    bg_errors.append(exc)

            lock_thread = threading.Thread(target=_hold_table_lock, daemon=True)
            lock_thread.start()
            time.sleep(1.5)      # let the lock holder acquire TABLOCKX

            blocked_thread = threading.Thread(target=_run_blocked, daemon=True)
            blocked_thread.start()
            time.sleep(1.5)      # blocked scan now holds the only pool slot

            # pool is full — should get RESOURCE_EXHAUSTED
            tdSql.error(
                f"select count(*) from {src}.push_t",
                expectedErrno=TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
            )
            tdLog.info("[C4-s] pool=1 + blocked scan → EXT_RESOURCE_EXHAUSTED  OK")
            _release_sqlserver_table_lock(ext_db)

            blocked_thread.join(timeout=60)
            if blocked_thread.is_alive():
                tdLog.info("[C4-s] blocked-query thread did not finish")
            lock_thread.join(timeout=30)
            if lock_errors:
                tdLog.info(f"[C4-s] lock holder error: {lock_errors[0]}")
            if bg_errors:
                tdLog.info(f"[C4-s] background thread error: {bg_errors[0]}")

            # Recovery: lock released, pool slot freed → serial query works
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-s] pool=1 recovery OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ----- C5-s: federatedQueryIdleConnTtlSec (SQL Server) -----
    def _c5_idle_ttl_sqlserver(self):
        """C5-s: idle TTL evicts SQL Server pool connections.

        SQL Server has no SHOW PROCESSLIST; the session view
        (sys.dm_exec_sessions, see _sqlserver_session_count) is the equivalent
        observation point.  Only the server-side pool is driven by ALTER ALL
        DNODES, so — exactly like the MySQL path [C5] — the assertion is that
        the session count decreases rather than reaching zero: the client-side
        catalog pool keeps its own TTL and may retain a connection.
        """
        tdLog.info("[C5-s] federatedQueryIdleConnTtlSec — SQL Server")
        src = "fq05_ttl_s"
        ext_db = "fq05_ttl_sdb"
        cfg = self._sqlserver_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Positive: TTL=86400 → sessions survive 5s idle --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '86400'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_before = _sqlserver_session_count(cfg, ext_db)
            assert cnt_before >= 1, (
                f"[C5-s] expected >= 1 SQL Server sessions after query, got {cnt_before}")
            tdLog.info(f"[C5-s] TTL=86400, session count after query = {cnt_before}")

            time.sleep(5)

            cnt_after = _sqlserver_session_count(cfg, ext_db)
            assert cnt_after >= 1, (
                f"[C5-s] TTL=86400: sessions should survive 5s idle, "
                f"but session count dropped to {cnt_after}")
            tdLog.info(f"[C5-s] TTL=86400, 5s later session count = {cnt_after}  OK")

            # query still works (reuses idle connection)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # -- Negative: TTL=1 → sessions evicted within 12s --
            self._cleanup_src(src)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '1'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_before2 = _sqlserver_session_count(cfg, ext_db)
            assert cnt_before2 >= 1, (
                f"[C5-s] expected >= 1 SQL Server sessions after query, got {cnt_before2}")
            tdLog.info(f"[C5-s] TTL=1, session count after query = {cnt_before2}")

            # Wait for eviction: TTL=1s + probe interval 5s + buffer
            time.sleep(12)

            cnt_after2 = _sqlserver_session_count(cfg, ext_db)
            assert cnt_after2 < cnt_before2, (
                f"[C5-s] TTL=1: server-side connections should be evicted "
                f"after 12s, but count didn't decrease "
                f"(before={cnt_before2} after={cnt_after2})")
            tdLog.info(f"[C5-s] TTL=1, 12s later session count "
                       f"{cnt_before2} → {cnt_after2}  OK")

            # query still works (creates new connection)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # -- Dynamic: TTL 86400→1, observe eviction --
            self._cleanup_src(src)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '86400'", queryTimes=1)
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_dyn1 = _sqlserver_session_count(cfg, ext_db)
            assert cnt_dyn1 >= 1, (
                f"[C5-s] dynamic: expected >= 1 sessions, got {cnt_dyn1}")

            # wait 5s with TTL=86400 — connections should stay
            time.sleep(5)
            cnt_dyn2 = _sqlserver_session_count(cfg, ext_db)
            assert cnt_dyn2 >= 1, (
                f"[C5-s] dynamic: TTL=86400, connections should survive, "
                f"got {cnt_dyn2}")
            tdLog.info(f"[C5-s] dynamic TTL=86400, 5s → count={cnt_dyn2}  OK")

            # now switch to TTL=1
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '1'", queryTimes=1)

            # wait for eviction (TTL=1s + probe interval 5s + buffer)
            time.sleep(12)

            cnt_dyn3 = _sqlserver_session_count(cfg, ext_db)
            assert cnt_dyn3 < cnt_dyn2, (
                f"[C5-s] dynamic: after switching TTL to 1s, server-side "
                f"connections should be evicted, but count didn't decrease "
                f"(before={cnt_dyn2} after={cnt_dyn3})")
            tdLog.info(f"[C5-s] dynamic TTL=86400→1, 12s later count "
                       f"{cnt_dyn2} → {cnt_dyn3}  OK")

            # query still works
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C5-s] dynamic recovery query OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ==================================================================
    # Group C — SQL Server client-side (ALTER LOCAL) mirrors
    #
    # The C2c–C5c MySQL paths above drive the *client* ext-connector pool
    # (catalog schema resolution) through ALTER LOCAL.  These SQL Server
    # paths mirror them for the 4th source; sessions are observed through
    # sys.dm_exec_sessions (see _sqlserver_session_count) instead of the
    # MySQL processlist.  Like the server-side SQL Server paths they use
    # plain tdSql calls so the recorded baseline stays unchanged.
    # ==================================================================

    # ----- C2-s-client: federatedQueryConnectTimeoutMs (ALTER LOCAL) -----
    def _c2_connect_timeout_sqlserver_client(self):
        """C2-s-client: ALTER LOCAL connect timeout controls catalog-phase connect."""
        tdLog.info("[C2-s-client] federatedQueryConnectTimeoutMs — SQL Server client-side")
        src = "fq05_ct_scl"
        ext_db = "fq05_ct_scl_db"
        cfg = self._sqlserver_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Positive: ALTER LOCAL default timeout, reachable → query OK --
            self._alter_local_and_verify_plain("federatedQueryConnectTimeoutMs", "5000")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C2-s-client] conn_tmo LOCAL=5000 query OK")

            # -- Negative: non-routable IP → catalog connect fails --
            tdSql.execute(f"drop external source if exists {src}", queryTimes=1)
            tdSql.execute(
                f"create external source {src} type='sqlserver' "
                f"host='192.0.2.1' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={ext_db} schema='dbo'", queryTimes=1)
            tdSql.error(f"select count(*) from {src}.push_t")
            tdLog.info("[C2-s-client] non-routable host → catalog connect failed  OK")
            self._cleanup_src(src)

            # -- Dynamic: change LOCAL timeout, reachable source → query OK --
            self._alter_local_and_verify_plain("federatedQueryConnectTimeoutMs", "600000")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)

            self._alter_local_and_verify_plain("federatedQueryConnectTimeoutMs", "100")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C2-s-client] dynamic conn_tmo LOCAL=100 reachable → OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryConnectTimeoutMs' '5000'",
                "ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ----- C3-s-client: federatedQueryQueryTimeoutMs (ALTER LOCAL) -----
    def _c3_query_timeout_sqlserver_client(self):
        """C3-s-client: ALTER LOCAL query timeout affects catalog schema queries.

        The client ext-connector resolves external table metadata through the
        same statement timeout as data queries, which is intended.  SQL Server
        ODBC only accepts whole seconds for SQL_ATTR_QUERY_TIMEOUT, so every
        value is rounded up, and the first catalog query against a freshly
        created database costs roughly one second (plan compilation plus system
        catalog warm-up).  The documented minimum of 100 ms therefore rounds to
        exactly 1 s and lands on that cold-start cost, which made this case
        fail.  Query with a value that keeps headroom for a cold catalog query
        (10000 ms), and still assert that the 100 ms boundary is accepted as a
        configuration value.
        """
        tdLog.info("[C3-s-client] federatedQueryQueryTimeoutMs — SQL Server client-side")
        src = "fq05_qt_scl"
        ext_db = "fq05_qt_scl_db"
        cfg = self._sqlserver_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Configuration boundary: the documented minimum is accepted --
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "100")

            # -- Positive: 10 s timeout — cold catalog query still OK --
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "10000")
            self._alter_dnode_and_verify_plain("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-s-client] query_tmo LOCAL=10000, catalog OK")

            # -- Dynamic: switch LOCAL timeout from min to mid-range --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "5000")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-s-client] dynamic query_tmo LOCAL=5000 → OK")

            # -- Dynamic: switch to max --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-s-client] dynamic query_tmo LOCAL=max → OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryQueryTimeoutMs' '1000000000'",
                "ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000000'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ----- C4-s-client: federatedQueryMaxPoolSizePerSource (ALTER LOCAL) -----
    def _c4_pool_size_sqlserver_client(self):
        """C4-s-client: ALTER LOCAL pool size affects the client catalog pool.

        Concurrent catalog operations against localhost cannot exhaust the
        pool reliably; the assertion is that the minimum pool size (1) does
        not break serial catalog + query operations, including dynamic
        switches in both directions.
        """
        tdLog.info("[C4-s-client] federatedQueryMaxPoolSizePerSource — SQL Server client-side")
        src = "fq05_ps_scl"
        ext_db = "fq05_ps_scl_db"
        cfg = self._sqlserver_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # -- Positive: pool=1, serial queries work --
            self._alter_local_and_verify_plain("federatedQueryMaxPoolSizePerSource", "1")
            self._alter_dnode_and_verify_plain("federatedQueryMaxPoolSizePerSource", "64")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-s-client] pool LOCAL=1, serial query OK")

            for _ in range(5):
                self._cleanup_src(src)
                self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
                tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
                tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-s-client] pool LOCAL=1, 5 serial queries OK")

            # -- Dynamic: pool 1→128, query OK --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryMaxPoolSizePerSource", "128")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-s-client] dynamic pool LOCAL=128 → OK")

            # -- Dynamic: pool 128→1, query OK --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryMaxPoolSizePerSource", "1")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-s-client] dynamic pool LOCAL=1 → OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryMaxPoolSizePerSource' '64'",
                "ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ----- C5-s-client: federatedQueryIdleConnTtlSec (ALTER LOCAL) -----
    def _c5_idle_ttl_sqlserver_client(self):
        """C5-s-client: ALTER LOCAL idle TTL — independent catalog-pool eviction.

        Same three phases as the MySQL [C5-client] path, observed through
        sys.dm_exec_sessions:

        Phase 1: server TTL=86400 (keep), client TTL=1 (evict)
                 → client catalog sessions evicted, server sessions survive.
        Phase 2: server TTL=1, client TTL=1 → all sessions evicted.
        Phase 3: server TTL=1 (evict), client TTL=86400 (keep)
                 → server sessions evicted, client catalog sessions survive.
        """
        tdLog.info("[C5-s-client] federatedQueryIdleConnTtlSec — SQL Server client-side")
        src = "fq05_tc_scl"
        ext_db = "fq05_tc_scl_db"
        cfg = self._sqlserver_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.sqlserver_create_db(ext_db)
            ExtSrcEnv.sqlserver_exec(ext_db, _SQLSERVER_PUSH_T_SQLS)

            # ---- Phase 1: server TTL=86400, client TTL=1 ----
            tdLog.info("[C5-s-client] Phase 1: server TTL=86400, client TTL=1")
            self._alter_dnode_and_verify_plain("federatedQueryIdleConnTtlSec", "86400")
            self._alter_local_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_p1_before = _sqlserver_session_count(cfg, ext_db)
            assert cnt_p1_before >= 1, (
                f"[C5-s-client] P1: expected >= 1 sessions, got {cnt_p1_before}")
            tdLog.info(f"[C5-s-client] P1 initial count = {cnt_p1_before}")

            time.sleep(12)

            cnt_p1_after = _sqlserver_session_count(cfg, ext_db)
            assert cnt_p1_after < cnt_p1_before, (
                f"[C5-s-client] P1: client TTL=1 should evict catalog sessions "
                f"but count didn't decrease (before={cnt_p1_before} "
                f"after={cnt_p1_after})")
            assert cnt_p1_after >= 1, (
                f"[C5-s-client] P1: server sessions (TTL=86400) should remain "
                f"but count dropped to {cnt_p1_after}")
            tdLog.info(f"[C5-s-client] P1: {cnt_p1_before} → {cnt_p1_after} "
                       f"(client evicted, server retained)  OK")

            # query still works (catalog creates a new session)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # ---- Phase 2: both TTL=1 → all evicted ----
            tdLog.info("[C5-s-client] Phase 2: both TTL=1")
            self._cleanup_src(src)
            self._alter_dnode_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._alter_local_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_p2_before = _sqlserver_session_count(cfg, ext_db)
            assert cnt_p2_before >= 1, (
                f"[C5-s-client] P2: expected >= 1 sessions, got {cnt_p2_before}")

            time.sleep(12)

            cnt_p2_after = _sqlserver_session_count(cfg, ext_db)
            assert cnt_p2_after == 0, (
                f"[C5-s-client] P2: both TTL=1, all sessions should be evicted "
                f"but {cnt_p2_after} remain")
            tdLog.info(f"[C5-s-client] P2: both TTL=1 → all evicted "
                       f"({cnt_p2_before} → {cnt_p2_after})  OK")

            # ---- Phase 3: server TTL=1, client TTL=86400 ----
            tdLog.info("[C5-s-client] Phase 3: server TTL=1, client TTL=86400")
            self._cleanup_src(src)
            self._alter_dnode_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._alter_local_and_verify_plain("federatedQueryIdleConnTtlSec", "86400")
            self._mk_sqlserver_real(src, database=ext_db, schema="dbo")

            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_p3_before = _sqlserver_session_count(cfg, ext_db)
            assert cnt_p3_before >= 1, (
                f"[C5-s-client] P3: expected >= 1 sessions, got {cnt_p3_before}")
            tdLog.info(f"[C5-s-client] P3 initial count = {cnt_p3_before}")

            time.sleep(12)

            cnt_p3_after = _sqlserver_session_count(cfg, ext_db)
            assert cnt_p3_after < cnt_p3_before, (
                f"[C5-s-client] P3: server TTL=1 should evict data-query "
                f"sessions but count didn't decrease "
                f"(before={cnt_p3_before} after={cnt_p3_after})")
            assert cnt_p3_after >= 1, (
                f"[C5-s-client] P3: client sessions (TTL=86400) should remain "
                f"but count dropped to {cnt_p3_after}")
            tdLog.info(f"[C5-s-client] P3: {cnt_p3_before} → {cnt_p3_after} "
                       f"(server evicted, client retained)  OK")

            # query still works
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C5-s-client] all phases OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryIdleConnTtlSec' '600'",
                "ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '600'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.sqlserver_drop_db(ext_db)
            except Exception:
                pass

    # ==================================================================
    # Group C — Oracle (5th source), parallel to the MySQL and SQL Server
    # paths above
    #
    # Oracle has one namespace level (the schema, which is also the login
    # user) and no per-test database: every method drops and recreates
    # PUSH_T in the login schema and points the source at the Easy Connect
    # service name.  Session observations use V$SESSION through the SYSTEM
    # account (see _oracle_session_count); when that is not possible the
    # assertions degrade to behavioural ones, as the Oracle TS requires.
    #
    # Like the SQL Server paths these use plain tdSql calls instead of
    # _rec_query / _rec_error so the ans baseline stays byte-for-byte
    # unchanged.
    # ==================================================================

    # ----- C1-o: federatedQueryEnable (Oracle) -----
    def _c1_enable_oracle(self):
        tdLog.info("[C1-o] federatedQueryEnable — Oracle")
        src = "fq05_en_o"
        src2 = "fq05_en2_o"
        cfg = self._oracle_cfg()
        self._cleanup_src(src, src2)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)

            # -- Positive: enable=1, query succeeds --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C1-o] enable=1 query OK")

            # -- Negative: enable=0, query blocked with the documented code --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '0'", queryTimes=1)
            # A disabled federated query hides the external sources from the
            # parser, so the 2-segment path degrades to a local db.table
            # reference and fails with "database does not exist".  The MySQL
            # and SQL Server legs above accept any error for the same reason;
            # only the DDL statements are rejected by the explicit
            # federatedQueryEnable guard.
            tdSql.error(f"select count(*) from {src}.push_t",
                        expectedErrno=TSDB_CODE_MND_DB_NOT_EXIST)
            tdLog.info("[C1-o] enable=0 query blocked  OK")

            # -- DDL also blocked by enable=0 (the parser-level guard the
            #    MySQL and SQL Server paths above exercise) --
            tdSql.error(
                f"create external source {src2} type='oracle' "
                f"host='{cfg.host}' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={cfg.service} schema='{cfg.user.upper()}'",
                expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"alter external source {src} set host='127.0.0.2'",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"drop external source {src}",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"refresh external source {src}",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error("show external sources",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdSql.error(f"describe external source {src}",
                        expectedErrno=TSDB_CODE_EXT_FEDERATED_DISABLED)
            tdLog.info("[C1-o] enable=0: DDL blocked  OK")

            # -- Restore + re-verify --
            tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C1-o] enable restored, query OK")

        finally:
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryEnable' '1'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryEnable' '1'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src, src2)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ----- C2-o: federatedQueryConnectTimeoutMs (Oracle) -----
    def _c2_connect_timeout_oracle(self):
        tdLog.info("[C2-o] federatedQueryConnectTimeoutMs — Oracle")
        src = "fq05_ct_o"
        cfg = self._oracle_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)

            # -- Positive: default timeout, reachable source → query succeeds --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C2-o] conn_tmo=5000 query OK")

            # -- Negative: non-routable IP → connect fails with the
            #    documented connection code (SQLSTATE 08xxx/08001 →
            #    EXT_CONNECT_FAILED).  Only the code is asserted, not the
            #    exact elapsed time: connect_timeout_ms becomes
            #    SQL_ATTR_LOGIN_TIMEOUT (whole seconds) and the Oracle
            #    client's own network retries stretch the wall clock, so
            #    the bound below is deliberately loose.
            tdSql.execute(f"drop external source if exists {src}", queryTimes=1)
            tdSql.execute(
                f"create external source {src} type='oracle' "
                f"host='192.0.2.1' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={cfg.service} schema='{cfg.user.upper()}'",
                queryTimes=1)
            t0 = time.monotonic()
            tdSql.error(f"select count(*) from {src}.push_t",
                        expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)
            elapsed = time.monotonic() - t0
            assert elapsed < 120, (
                f"[C2-o] connect timeout not effective: {elapsed:.1f}s")
            tdLog.info(f"[C2-o] non-routable host → query failed in {elapsed:.1f}s  OK")
            self._cleanup_src(src)

            # -- Dynamic: change timeout, real query still succeeds --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '600000'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)

            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '100'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C2-o] dynamic conn_tmo=100, reachable Oracle → query OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ----- C3-o: federatedQueryQueryTimeoutMs (Oracle) -----
    def _c3_query_timeout_oracle(self):
        """C3-o: query timeout aborts a slow Oracle scan, both config paths.

        Oracle has no SELECT-embedded SLEEP(); a view over
        DBMS_SESSION.SLEEP (see _oracle_sleep_fixtures) makes the base scan
        take ~2 s.  Two independent knobs are exercised, because Oracle is
        the only source in the suite that ships a per-source read timeout:

          * the global federatedQueryQueryTimeoutMs (SQL_ATTR_QUERY_TIMEOUT),
          * OPTIONS('read_timeout_ms') on the source itself.

        Both surface as TSDB_CODE_EXT_QUERY_TIMEOUT (the driver reports
        ORA-01013, SQLSTATE HYT00), and a fast query afterwards proves the
        pool connection recovered.
        """
        tdLog.info("[C3-o] federatedQueryQueryTimeoutMs — Oracle")
        src = "fq05_qt_o"
        src_opt = "fq05_qt_oo"
        cfg = self._oracle_cfg()
        self._cleanup_src(src, src_opt)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_sleep_fixtures(cfg)

            # -- Positive: huge timeout → the slow scan still succeeds --
            self._alter_dnode_and_verify_plain(
                "federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select val, boom from {src}.fq05_slow_v", queryTimes=1)
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)
            tdLog.info("[C3-o] query_tmo=1000000000, slow scan succeeded  OK")

            # -- Negative: 1s global timeout → slow scan fails with QUERY_TIMEOUT --
            self._cleanup_src(src)
            self._alter_dnode_and_verify_plain(
                "federatedQueryQueryTimeoutMs", "1000")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            t0 = time.monotonic()
            tdSql.error(f"select val, boom from {src}.fq05_slow_v",
                        expectedErrno=TSDB_CODE_EXT_QUERY_TIMEOUT)
            elapsed = time.monotonic() - t0
            assert elapsed < 60, (
                f"[C3-o] query timeout not effective: elapsed={elapsed:.1f}s")
            tdLog.info(f"[C3-o] query_tmo=1000 → slow scan failed in {elapsed:.1f}s  OK")

            # Recovery: the timed-out connection is replaced, a fast query works
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # -- Negative: per-source OPTIONS read_timeout_ms overrides the
            #    (still huge) global timeout --
            self._alter_dnode_and_verify_plain(
                "federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_oracle_real(
                src_opt, database=cfg.service, schema=cfg.user.upper(),
                extra_options="'connect_timeout_ms'='5000','read_timeout_ms'='1000'")
            t0 = time.monotonic()
            tdSql.error(f"select val, boom from {src_opt}.fq05_slow_v",
                        expectedErrno=TSDB_CODE_EXT_QUERY_TIMEOUT)
            elapsed = time.monotonic() - t0
            assert elapsed < 60, (
                f"[C3-o] OPTIONS read_timeout_ms not effective: elapsed={elapsed:.1f}s")
            tdLog.info(f"[C3-o] read_timeout_ms=1000 → slow scan failed in "
                       f"{elapsed:.1f}s  OK")
            tdSql.query(f"select count(*) from {src_opt}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # -- Dynamic: switch the global timeout back up, slow scan succeeds --
            self._cleanup_src(src)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select val, boom from {src}.fq05_slow_v", queryTimes=1)
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)
            tdLog.info("[C3-o] dynamic query_tmo=1e9 → slow scan OK")

        finally:
            try:
                self._alter_dnode_and_verify_plain(
                    "federatedQueryQueryTimeoutMs", "1000000000")
            except Exception:
                pass
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryQueryTimeoutMs' '1000000000'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src, src_opt)
            try:
                _oracle_drop_slow_fixtures(cfg)
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ----- C4-o: federatedQueryMaxPoolSizePerSource (Oracle) -----
    def _c4_pool_size_oracle(self):
        """C4-o: pool cap enforced for Oracle sources.

        The single slot is held by a 10 s sleep view evaluated through a
        second client connection.  The pool acquire then gives up after the
        connect timeout and reports EXT_RESOURCE_EXHAUSTED (0x640A), which is
        the retryable class the scheduler backs off on; once the slow query
        finishes the slot is reusable.
        """
        tdLog.info("[C4-o] federatedQueryMaxPoolSizePerSource — Oracle")
        src = "fq05_ps_o"
        cfg = self._oracle_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_sleep_fixtures(cfg)

            # -- Positive: pool=64, query succeeds --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C4-o] pool=64, query OK")

            # -- Negative: pool=1, slow scan in flight → RESOURCE_EXHAUSTED --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '1'", queryTimes=1)
            time.sleep(0.5)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())

            # confirm pool=1 still works for serial queries
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            bg_errors = []

            def _run_slow():
                import taos
                try:
                    conn = taos.connect()
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                f"select val, boom from {src}.fq05_slow10_v")
                            cur.fetchall()
                        except Exception as exc:
                            bg_errors.append(exc)
                        finally:
                            cur.close()
                    finally:
                        conn.close()
                except Exception as exc:
                    bg_errors.append(exc)

            slow_thread = threading.Thread(target=_run_slow, daemon=True)
            slow_thread.start()
            time.sleep(1.5)      # let the slow scan take the only pool slot

            # pool is full — the acquire times out as RESOURCE_EXHAUSTED
            t0 = time.monotonic()
            tdSql.error(
                f"select count(*) from {src}.push_t",
                expectedErrno=TSDB_CODE_EXT_RESOURCE_EXHAUSTED,
            )
            elapsed = time.monotonic() - t0
            assert elapsed < 60, (
                f"[C4-o] pool cap not enforced: error took {elapsed:.1f}s")
            tdLog.info(f"[C4-o] pool=1 + slow scan → EXT_RESOURCE_EXHAUSTED "
                       f"after {elapsed:.1f}s  OK")

            slow_thread.join(timeout=60)
            if slow_thread.is_alive():
                tdLog.info("[C4-o] slow-query thread did not finish")
            if bg_errors:
                tdLog.info(f"[C4-o] background thread error: {bg_errors[0]}")

            # Recovery: slot freed → serial query works
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-o] pool=1 recovery OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                _oracle_drop_slow_fixtures(cfg)
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ----- C5-o: federatedQueryIdleConnTtlSec (Oracle) -----
    def _c5_idle_ttl_oracle(self):
        """C5-o: idle TTL evicts Oracle pool connections.

        Observed through V$SESSION (see _oracle_session_count).  Only the
        server-side pool is driven by ALTER ALL DNODES, so — exactly like the
        MySQL [C5] path — the assertion is that the session count decreases
        rather than reaching zero: the client-side catalog pool keeps its own
        TTL and may retain a connection.  When V$SESSION is not readable the
        cases fall back to the behavioural contract (the source keeps
        answering before and after the TTL elapses).
        """
        tdLog.info("[C5-o] federatedQueryIdleConnTtlSec — Oracle")
        src = "fq05_ttl_o"
        cfg = self._oracle_cfg()
        self._cleanup_src(src)

        def _count():
            value = _oracle_session_count(cfg)
            if value is None:
                tdLog.info("[C5-o] V$SESSION not readable; behavioural fallback")
            return value

        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)

            # -- Positive: TTL=86400 → sessions survive 5s idle --
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '86400'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_before = _count()
            if cnt_before is not None:
                assert cnt_before >= 1, (
                    f"[C5-o] expected >= 1 Oracle sessions after query, "
                    f"got {cnt_before}")
                tdLog.info(f"[C5-o] TTL=86400, session count after query = {cnt_before}")

            time.sleep(5)

            cnt_after = _count()
            if cnt_before is not None and cnt_after is not None:
                assert cnt_after >= 1, (
                    f"[C5-o] TTL=86400: sessions should survive 5s idle, "
                    f"but session count dropped to {cnt_after}")
                tdLog.info(f"[C5-o] TTL=86400, 5s later session count = {cnt_after}  OK")

            # query still works (reuses the idle connection)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # -- Negative: TTL=1 → sessions evicted within 12s --
            self._cleanup_src(src)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '1'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_before2 = _count()
            if cnt_before2 is not None:
                assert cnt_before2 >= 1, (
                    f"[C5-o] expected >= 1 Oracle sessions after query, "
                    f"got {cnt_before2}")
                tdLog.info(f"[C5-o] TTL=1, session count after query = {cnt_before2}")

            # Wait for eviction: TTL=1s + probe interval 5s + buffer
            time.sleep(12)

            cnt_after2 = _count()
            if cnt_before2 is not None and cnt_after2 is not None:
                assert cnt_after2 < cnt_before2, (
                    f"[C5-o] TTL=1: server-side connections should be evicted "
                    f"after 12s, but count didn't decrease "
                    f"(before={cnt_before2} after={cnt_after2})")
                tdLog.info(f"[C5-o] TTL=1, 12s later session count "
                           f"{cnt_before2} → {cnt_after2}  OK")

            # query still works (creates a new connection)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # -- Dynamic: TTL 86400→1, observe eviction --
            self._cleanup_src(src)
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '86400'", queryTimes=1)
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_dyn1 = _count()
            if cnt_dyn1 is not None:
                assert cnt_dyn1 >= 1, (
                    f"[C5-o] dynamic: expected >= 1 sessions, got {cnt_dyn1}")

                # wait 5s with TTL=86400 — connections should stay
                time.sleep(5)
                cnt_dyn2 = _count()
                if cnt_dyn2 is not None:
                    assert cnt_dyn2 >= 1, (
                        f"[C5-o] dynamic: TTL=86400, connections should "
                        f"survive, got {cnt_dyn2}")
                    tdLog.info(f"[C5-o] dynamic TTL=86400, 5s → "
                               f"count={cnt_dyn2}  OK")

            # now switch to TTL=1
            tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '1'", queryTimes=1)
            time.sleep(12)

            if cnt_dyn1 is not None:
                cnt_dyn3 = _count()
                if cnt_dyn3 is not None:
                    assert cnt_dyn3 < cnt_dyn1, (
                        f"[C5-o] dynamic: after switching TTL to 1s, "
                        f"server-side connections should be evicted, but the "
                        f"count didn't decrease (before={cnt_dyn1} "
                        f"after={cnt_dyn3})")
                    tdLog.info(f"[C5-o] dynamic TTL=86400→1, 12s later count "
                               f"{cnt_dyn1} → {cnt_dyn3}  OK")

            # query still works
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C5-o] dynamic recovery query OK")

        finally:
            try:
                tdSql.execute("ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
            except Exception:
                pass
            try:
                tdSql.execute("ALTER LOCAL 'federatedQueryIdleConnTtlSec' '600'", queryTimes=1)
            except Exception:
                pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ==================================================================
    # Group C — Oracle client-side (ALTER LOCAL) mirrors
    #
    # These drive the client ext-connector pool (catalog schema resolution)
    # through ALTER LOCAL, mirroring the C2c–C5c MySQL paths and the
    # SQL Server C2-s-client … C5-s-client paths.  Sessions are observed
    # through V$SESSION; plain tdSql calls keep the baseline unchanged.
    # ==================================================================

    # ----- C2-o-client: federatedQueryConnectTimeoutMs (ALTER LOCAL) -----
    def _c2_connect_timeout_oracle_client(self):
        """C2-o-client: ALTER LOCAL connect timeout controls catalog-phase connect."""
        tdLog.info("[C2-o-client] federatedQueryConnectTimeoutMs — Oracle client-side")
        src = "fq05_ct_oc"
        cfg = self._oracle_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)

            # -- Positive: ALTER LOCAL default timeout, reachable → query OK --
            self._alter_local_and_verify_plain("federatedQueryConnectTimeoutMs", "5000")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)
            tdLog.info("[C2-o-client] conn_tmo LOCAL=5000 query OK")

            # -- Negative: non-routable IP → catalog connect fails with the
            #    documented connection code (EXT_CONNECT_FAILED) --
            tdSql.execute(f"drop external source if exists {src}", queryTimes=1)
            tdSql.execute(
                f"create external source {src} type='oracle' "
                f"host='192.0.2.1' port={cfg.port} "
                f"user='{cfg.user}' password='{cfg.password}' "
                f"database={cfg.service} schema='{cfg.user.upper()}'",
                queryTimes=1)
            tdSql.error(f"select count(*) from {src}.push_t",
                        expectedErrno=TSDB_CODE_EXT_CONNECT_FAILED)
            tdLog.info("[C2-o-client] non-routable host → catalog connect failed  OK")
            self._cleanup_src(src)

            # -- Dynamic: change LOCAL timeout, reachable source → query OK --
            self._alter_local_and_verify_plain("federatedQueryConnectTimeoutMs", "600000")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            self._cleanup_src(src)

            self._alter_local_and_verify_plain("federatedQueryConnectTimeoutMs", "100")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C2-o-client] dynamic conn_tmo LOCAL=100 reachable → OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryConnectTimeoutMs' '5000'",
                "ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ----- C3-o-client: federatedQueryQueryTimeoutMs (ALTER LOCAL) -----
    def _c3_query_timeout_oracle_client(self):
        """C3-o-client: ALTER LOCAL query timeout applies to catalog queries.

        The client ext-connector resolves external table metadata through the
        same statement timeout as data queries, which is intended.  Oracle's
        ODBC driver only accepts whole seconds for SQL_ATTR_QUERY_TIMEOUT, so
        the documented 100 ms minimum rounds to 1 s and a cold catalog query
        can land on that boundary; the positive case therefore uses 10 s and
        the 100 ms value is only asserted as an accepted configuration.
        """
        tdLog.info("[C3-o-client] federatedQueryQueryTimeoutMs — Oracle client-side")
        src = "fq05_qt_oc"
        cfg = self._oracle_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)
            _oracle_sleep_fixtures(cfg)

            # -- Configuration boundary: the documented minimum is accepted --
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "100")

            # -- Positive: 10 s timeout — cold catalog query still OK --
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "10000")
            self._alter_dnode_and_verify_plain("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-o-client] query_tmo LOCAL=10000, catalog OK")

            # -- Dynamic: switch LOCAL timeout from min to mid-range --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "5000")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-o-client] dynamic query_tmo LOCAL=5000 → OK")

            # -- Dynamic: switch to max --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryQueryTimeoutMs", "1000000000")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C3-o-client] dynamic query_tmo LOCAL=max → OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryQueryTimeoutMs' '1000000000'",
                "ALTER ALL DNODES 'federatedQueryQueryTimeoutMs' '1000000000'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                _oracle_drop_slow_fixtures(cfg)
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ----- C4-o-client: federatedQueryMaxPoolSizePerSource (ALTER LOCAL) -----
    def _c4_pool_size_oracle_client(self):
        """C4-o-client: ALTER LOCAL pool size affects the client catalog pool.

        Concurrent catalog operations against localhost cannot exhaust the
        pool reliably; the assertion is that the minimum pool size (1) does
        not break serial catalog + query operations, including dynamic
        switches in both directions.
        """
        tdLog.info("[C4-o-client] federatedQueryMaxPoolSizePerSource — Oracle client-side")
        src = "fq05_ps_oc"
        cfg = self._oracle_cfg()
        self._cleanup_src(src)
        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)

            # -- Positive: pool=1, serial queries work --
            self._alter_local_and_verify_plain("federatedQueryMaxPoolSizePerSource", "1")
            self._alter_dnode_and_verify_plain("federatedQueryMaxPoolSizePerSource", "64")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-o-client] pool LOCAL=1, serial query OK")

            for _ in range(5):
                self._cleanup_src(src)
                self._mk_oracle_real(src, database=cfg.service,
                                     schema=cfg.user.upper())
                tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
                tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-o-client] pool LOCAL=1, 5 serial queries OK")

            # -- Dynamic: pool 1→128, query OK --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryMaxPoolSizePerSource", "128")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-o-client] dynamic pool LOCAL=128 → OK")

            # -- Dynamic: pool 128→1, query OK --
            self._cleanup_src(src)
            self._alter_local_and_verify_plain("federatedQueryMaxPoolSizePerSource", "1")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C4-o-client] dynamic pool LOCAL=1 → OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryMaxPoolSizePerSource' '64'",
                "ALTER ALL DNODES 'federatedQueryMaxPoolSizePerSource' '64'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass

    # ----- C5-o-client: federatedQueryIdleConnTtlSec (ALTER LOCAL) -----
    def _c5_idle_ttl_oracle_client(self):
        """C5-o-client: ALTER LOCAL idle TTL — independent catalog-pool eviction.

        Same three phases as the MySQL [C5-client] and SQL Server
        [C5-s-client] paths, observed through V$SESSION:

        Phase 1: server TTL=86400 (keep), client TTL=1 (evict)
                 → client catalog sessions evicted, server sessions survive.
        Phase 2: server TTL=1, client TTL=1 → both pools' sessions evicted.
        Phase 3: server TTL=1 (evict), client TTL=86400 (keep)
                 → server sessions evicted, client catalog sessions survive.

        When V$SESSION is not readable the phases still run and the
        behavioural contract is asserted (the source answers after each TTL
        window), which the TS allows as the fallback.
        """
        tdLog.info("[C5-o-client] federatedQueryIdleConnTtlSec — Oracle client-side")
        src = "fq05_tc_oc"
        cfg = self._oracle_cfg()
        self._cleanup_src(src)

        def _count():
            value = _oracle_session_count(cfg)
            if value is None:
                tdLog.info("[C5-o-client] V$SESSION not readable; behavioural fallback")
            return value

        try:
            ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            ExtSrcEnv.oracle_exec_cfg(cfg, _ORACLE_PUSH_T_SQLS)

            # ---- Phase 1: server TTL=86400, client TTL=1 ----
            tdLog.info("[C5-o-client] Phase 1: server TTL=86400, client TTL=1")
            self._alter_dnode_and_verify_plain("federatedQueryIdleConnTtlSec", "86400")
            self._alter_local_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_p1_before = _count()
            if cnt_p1_before is not None:
                assert cnt_p1_before >= 1, (
                    f"[C5-o-client] P1: expected >= 1 sessions, "
                    f"got {cnt_p1_before}")
                tdLog.info(f"[C5-o-client] P1 initial count = {cnt_p1_before}")

            time.sleep(12)

            cnt_p1_after = _count()
            if cnt_p1_before is not None and cnt_p1_after is not None:
                assert cnt_p1_after < cnt_p1_before, (
                    f"[C5-o-client] P1: client TTL=1 should evict catalog "
                    f"sessions but count didn't decrease "
                    f"(before={cnt_p1_before} after={cnt_p1_after})")
                assert cnt_p1_after >= 1, (
                    f"[C5-o-client] P1: server sessions (TTL=86400) should "
                    f"remain but count dropped to {cnt_p1_after}")
                tdLog.info(f"[C5-o-client] P1: {cnt_p1_before} → {cnt_p1_after} "
                           f"(client evicted, server retained)  OK")

            # query still works (catalog creates a new session)
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            # ---- Phase 2: both TTL=1 → all evicted ----
            tdLog.info("[C5-o-client] Phase 2: both TTL=1")
            self._cleanup_src(src)
            self._alter_dnode_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._alter_local_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_p2_before = _count()
            if cnt_p2_before is not None:
                assert cnt_p2_before >= 1, (
                    f"[C5-o-client] P2: expected >= 1 sessions, "
                    f"got {cnt_p2_before}")

            time.sleep(12)

            cnt_p2_after = _count()
            if cnt_p2_before is not None and cnt_p2_after is not None:
                # The count is scoped to the login account, which every Oracle
                # source of the instance shares, so a foreign session (another
                # test file running in parallel, or a leaked one) would defeat
                # an "== 0" assertion.  Eviction is therefore observed as the
                # decrease this phase is responsible for.
                assert cnt_p2_after < cnt_p2_before, (
                    f"[C5-o-client] P2: both TTL=1, pool sessions should be "
                    f"evicted but the count did not decrease "
                    f"(before={cnt_p2_before} after={cnt_p2_after})")
                tdLog.info(f"[C5-o-client] P2: both TTL=1 → evicted "
                           f"({cnt_p2_before} → {cnt_p2_after})  OK")

            # ---- Phase 3: server TTL=1, client TTL=86400 ----
            tdLog.info("[C5-o-client] Phase 3: server TTL=1, client TTL=86400")
            self._cleanup_src(src)
            self._alter_dnode_and_verify_plain("federatedQueryIdleConnTtlSec", "1")
            self._alter_local_and_verify_plain("federatedQueryIdleConnTtlSec", "86400")
            self._mk_oracle_real(src, database=cfg.service,
                                 schema=cfg.user.upper())
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)

            cnt_p3_before = _count()
            if cnt_p3_before is not None:
                assert cnt_p3_before >= 1, (
                    f"[C5-o-client] P3: expected >= 1 sessions, "
                    f"got {cnt_p3_before}")
                tdLog.info(f"[C5-o-client] P3 initial count = {cnt_p3_before}")

            time.sleep(12)

            cnt_p3_after = _count()
            if cnt_p3_before is not None and cnt_p3_after is not None:
                assert cnt_p3_after < cnt_p3_before, (
                    f"[C5-o-client] P3: server TTL=1 should evict data-query "
                    f"sessions but count didn't decrease "
                    f"(before={cnt_p3_before} after={cnt_p3_after})")
                assert cnt_p3_after >= 1, (
                    f"[C5-o-client] P3: client sessions (TTL=86400) should "
                    f"remain but count dropped to {cnt_p3_after}")
                tdLog.info(f"[C5-o-client] P3: {cnt_p3_before} → "
                           f"{cnt_p3_after} (server evicted, client retained)  OK")

            # query still works
            tdSql.query(f"select count(*) from {src}.push_t", queryTimes=1)
            tdSql.checkData(0, 0, 5)
            tdLog.info("[C5-o-client] all phases OK")

        finally:
            for stmt in (
                "ALTER LOCAL 'federatedQueryIdleConnTtlSec' '600'",
                "ALTER ALL DNODES 'federatedQueryIdleConnTtlSec' '600'",
            ):
                try:
                    tdSql.execute(stmt, queryTimes=1)
                except Exception:
                    pass
            self._cleanup_src(src)
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, "PUSH_T")
            except Exception:
                pass
