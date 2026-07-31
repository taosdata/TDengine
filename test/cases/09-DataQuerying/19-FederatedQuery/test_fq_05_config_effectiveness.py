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
    TSDB_CODE_EXT_FEDERATED_DISABLED,
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


class TestFq05ConfigEffectiveness(FederatedQueryVersionedMixin):
    """FQ-05: Configuration Effectiveness."""

    _BASELINE_DIR = os.path.join(os.path.dirname(__file__), "ans")

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        # Restore federatedQueryConnectTimeoutMs to default (5000 ms) in case a
        # previous pytest session's fq_08 left it at a different value.  This
        # test file's test_001_defaults asserts the value equals 5000.
        try:
            tdSql.execute("ALTER ALL DNODES 'federatedQueryConnectTimeoutMs' '5000'", queryTimes=1)
        except Exception:
            pass

    def teardown_class(self):
        # Drop all external sources created by this test file (prefix fq05_)
        # so they do not pollute subsequent test files via SHOW EXTERNAL SOURCES.
        try:
            tdSql.query("show external sources", queryTimes=1)
            own = [
                row[0] for row in (tdSql.queryResult or [])
                if str(row[0]).startswith("fq05_")
            ]
            for _src in own:
                try:
                    tdSql.execute(f"drop database if exists {_src}", queryTimes=1)
                    tdSql.execute(f"drop external source if exists {_src}", queryTimes=1)
                except Exception:
                    pass
        except Exception:
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
        tmp_content = "\n".join(self._rec_blocks) + "\n"
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
        self._query_dnode_var(param)
        tdSql.checkData(0, 2, value)

    def _alter_local_and_verify(self, param, value):
        """ALTER LOCAL → SHOW LOCAL VARIABLES → assert value."""
        tdSql.execute(f"ALTER LOCAL '{param}' '{value}'", queryTimes=1)
        self._query_local_var(param)
        tdSql.checkData(0, 1, value)

    @staticmethod
    def _block_port(port):
        """Add iptables DROP rule to silently drop outgoing TCP to 127.0.0.1:port."""
        import subprocess
        subprocess.run(
            ["iptables", "-I", "OUTPUT", "-p", "tcp",
             "-d", "127.0.0.1", "--dport", str(port), "-j", "DROP"],
            check=True, capture_output=True)

    @staticmethod
    def _unblock_port(port):
        """Remove iptables DROP rule (idempotent)."""
        import subprocess
        subprocess.run(
            ["iptables", "-D", "OUTPUT", "-p", "tcp",
             "-d", "127.0.0.1", "--dport", str(port), "-j", "DROP"],
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
        """FQ-05-001: Verify all config default values via SHOW VARIABLES."""
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
        """FQ-05-002: ALTER config values and verify via SHOW VARIABLES."""
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
                tdLog.warning("[C4] background slow-query thread did not finish")
            if bg_errors:
                tdLog.warning(f"[C4] background thread error: {bg_errors[0]}")

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
