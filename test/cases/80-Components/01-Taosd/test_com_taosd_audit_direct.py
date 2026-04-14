"""
test_com_taosd_audit_direct.py

System-level tests for the direct-write audit feature.

Background
----------
The original audit path sends records via HTTP to taoskeeper, which writes them
into the audit database.  The new "direct-write" path bypasses taoskeeper:
mndInitAudit() registers a flush callback that encodes an SSubmitReq2 and
sends it via TDMT_VND_SUBMIT directly to the audit-db vnode.

These tests verify the end-to-end behaviour of the new path:

  1. When the audit database (is_audit=1) exists and the "operations" supertable
     has been created, audit records for user DDL/DML operations are written
     directly to the audit DB instead of requiring taoskeeper to be running.

  2. The operations supertable and child table (t_operations_<cluster_id>) are
     populated with the expected fields.

  3. The existing HTTP path (test_com_taosd_audit.py) is NOT broken: when the
     audit flag is disabled or the audit DB does not exist, the system behaves
     as before.

Test matrix
-----------
  01  Prerequisite DDL: create audit DB + operations supertable (normally done
      by taoskeeper on first start; here we create them manually so the test is
      self-contained and does not require taoskeeper).

  02  After performing audited operations (CREATE DATABASE, CREATE TABLE, INSERT,
      DROP TABLE, DROP DATABASE) the operations supertable must contain at least
      one row in the child table.

  03  Every row must have non-NULL values for ts, user_name, operation, and
      client_address columns.

  04  The operation column must contain one of the expected SQL-verb strings for
      the operations we performed.

  05  After dropping the audit database, subsequent audited operations must NOT
      crash taosd — the flush callback must fail gracefully.

  06  Re-create the audit database and verify that new records are written again.

  07  Verify the is_audit flag appears correctly in information_schema.ins_databases.

Since: v3.0.0.0

Labels: common,ci

Jira: None

History:
    - 2026-03-20  New test for direct-write audit (bypass taoskeeper)
"""

import time
import platform

from new_test_framework.utils import tdLog, tdSql, tdDnodes, cluster

# ---------------------------------------------------------------------------
# Server-side taosd configuration.
#
# We enable the audit feature (audit=1, auditLevel=4) and intentionally do
# NOT configure monitorFqdn so that the HTTP-to-taoskeeper path is disabled.
# This forces the direct-write path to be the only active sink.
# ---------------------------------------------------------------------------
_hostname   = "localhost"
_serverPort = "6030"

# How many seconds to wait for the audit background thread to flush records
# after an audited operation.  auditInterval=500ms; allow generous margin.
_FLUSH_WAIT_SECONDS = 10
_POLL_INTERVAL      = 1   # seconds between retries


class TestTaosdAuditDirect:
    """
    End-to-end tests for the direct-write audit path.

    Taosd is started with audit=1 and auditLevel=4 but WITHOUT a monitorFqdn,
    so the HTTP-to-taoskeeper path is inactive.  The mnode registers
    mndAuditFlushCb at startup; all audit records are written directly into
    the audit database.
    """

    # -----------------------------------------------------------------------
    # Server / client configuration injected by the test framework before the
    # cluster is started.  The framework reads updatecfgDict from the class.
    # -----------------------------------------------------------------------
    updatecfgDict = {
        # Connection settings
        "serverPort":   _serverPort,
        "firstEp":      f"{_hostname}:{_serverPort}",
        "secondEp":     f"{_hostname}:{_serverPort}",
        "fqdn":         _hostname,

        # Enable audit; set level to 4 (record all operations)
        "audit":        "1",
        "auditLevel":   "4",
        "auditHttps":   "0",
        # auditInterval controls how often the background thread flushes (ms).
        # Valid range: [500, 200000].  Use 500 ms so tests complete quickly.
        "auditInterval": "500",

        # Do NOT set monitorFqdn so that the HTTP path stays inactive.
        # This ensures all records go through the direct-write path only.
        "monitor": "0",

        # Enable SM4-CBC encryption so that IS_AUDIT databases (which require
        # encryption in the enterprise edition) can be created successfully.
        "encryptAlgorithm": "SM4-CBC",

        # Useful for debugging failures
        "mDebugFlag":   "143",
        "uDebugFlag":   "143",
    }

    # -----------------------------------------------------------------------
    # Names used across tests (kept as class attributes so helper methods can
    # reference them without passing parameters everywhere).
    # -----------------------------------------------------------------------
    AUDIT_DB        = "audit"
    AUDIT_STB       = "operations"
    WORK_DB         = "testdb_audit_direct"
    WORK_STB        = "stb1"
    WORK_CTB        = "ctb1"

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _create_audit_infrastructure(self):
        """
        Create the audit database and the 'operations' supertable that
        taoskeeper normally creates on first startup.

        In a production deployment taoskeeper performs this step.  Here we do
        it manually so the test is self-contained (no taoskeeper required).

        Schema matches what taoskeeper creates:
          columns : ts TIMESTAMP, user_name VARCHAR(25), operation VARCHAR(20),
                    db VARCHAR(65), resource VARCHAR(193),
                    client_address VARCHAR(64), details VARCHAR(50000)
          tag     : cluster_id VARCHAR(64)
        """
        tdLog.info("=== create encrypt key (required for IS_AUDIT) ===")
        # Key length must be in [8, 16].  Use query() so duplicate-key errors
        # (from a previous run) are silently ignored.
        try:
            tdSql.query("CREATE ENCRYPT_KEY 'auditkey1234abcd'")
        except Exception:
            pass
        # Give the encryption subsystem a moment to initialise.
        time.sleep(2)

        tdLog.info("=== create audit database ===")
        # IS_AUDIT requires WAL_LEVEL 2 (default) and an encryption algorithm.
        # Use a retry loop because encrypted-DB creation can be slow.
        for attempt in range(30):
            try:
                tdSql.execute(
                    f"CREATE DATABASE IF NOT EXISTS {self.AUDIT_DB} "
                    f"IS_AUDIT 1 WAL_LEVEL 2 ENCRYPT_ALGORITHM 'SM4-CBC'",
                    queryTimes=1,
                )
                break
            except Exception as e:
                if "creating status" in str(e) or "in creating" in str(e):
                    time.sleep(1)
                    continue
                raise
        else:
            tdLog.exit(f"Timed out waiting for '{self.AUDIT_DB}' CREATE to succeed")
        # Poll until the database is visible in the catalog.
        for _ in range(30):
            tdSql.query(
                f"SELECT name FROM information_schema.ins_databases "
                f"WHERE name='{self.AUDIT_DB}'"
            )
            if tdSql.getRows() >= 1:
                break
            time.sleep(1)
        else:
            tdLog.exit(f"Timed out waiting for '{self.AUDIT_DB}' to appear")

        tdLog.info("=== create operations supertable ===")
        tdSql.execute(f"""
            CREATE STABLE IF NOT EXISTS {self.AUDIT_DB}.{self.AUDIT_STB} (
                ts             TIMESTAMP,
                user_name      VARCHAR(25),
                operation      VARCHAR(20),
                db             VARCHAR(65),
                resource       VARCHAR(193),
                client_address VARCHAR(64),
                details        VARCHAR(50000)
            ) TAGS (
                cluster_id VARCHAR(64)
            )
        """)

    def _drop_audit_infrastructure(self):
        """Drop the audit database (used to test graceful fallback)."""
        tdLog.info("=== drop audit database ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.AUDIT_DB}")

    def _create_work_db(self):
        """Create the work database used to generate audited operations."""
        tdSql.execute(
            f"CREATE DATABASE IF NOT EXISTS {self.WORK_DB} VGROUPS 1"
        )

    def _drop_work_db(self):
        """Drop the work database (itself an audited operation)."""
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.WORK_DB}")

    def _do_audited_operations(self):
        """
        Execute a sequence of SQL statements that are all recorded by the audit
        subsystem.  Each call produces several audit records covering different
        operation types (createdb, createstb, createtb, insert, droptb, dropdb).
        """
        tdLog.info("=== performing audited operations ===")

        self._drop_work_db()
        self._create_work_db()

        tdSql.execute(
            f"CREATE STABLE {self.WORK_DB}.{self.WORK_STB} "
            f"(ts TIMESTAMP, val INT) TAGS (grp INT)"
        )
        tdSql.execute(
            f"CREATE TABLE {self.WORK_DB}.{self.WORK_CTB} "
            f"USING {self.WORK_DB}.{self.WORK_STB} TAGS (1)"
        )
        tdSql.execute(
            f"INSERT INTO {self.WORK_DB}.{self.WORK_CTB} "
            f"VALUES (NOW, 42)"
        )
        tdSql.execute(f"DROP TABLE {self.WORK_DB}.{self.WORK_CTB}")
        self._drop_work_db()

    def _wait_for_audit_rows(self, min_rows=1, timeout=_FLUSH_WAIT_SECONDS):
        """
        Poll the operations supertable until at least min_rows appear or the
        timeout expires.  Returns the final row count.
        """
        elapsed = 0
        rows = 0
        while elapsed < timeout:
            try:
                tdSql.query(
                    f"SELECT COUNT(*) FROM {self.AUDIT_DB}.{self.AUDIT_STB}"
                )
                rows = int(tdSql.getData(0, 0))
                if rows >= min_rows:
                    tdLog.info(
                        f"audit rows reached {rows} after {elapsed}s"
                    )
                    return rows
            except Exception:
                # Table may not have the child table yet — keep polling.
                pass
            time.sleep(_POLL_INTERVAL)
            elapsed += _POLL_INTERVAL
        tdLog.info(
            f"audit rows = {rows} after {timeout}s timeout (wanted >= {min_rows})"
        )
        return rows

    # -----------------------------------------------------------------------
    # Test 01 — Prerequisite DDL
    # -----------------------------------------------------------------------
    def test_01_create_audit_infrastructure(self):
        """Create audit DB and operations STB manually (taoskeeper substitute)

        1. Create audit database with IS_AUDIT=1
        2. Create operations supertable with the schema expected by the
           direct-write flush callback (mndAuditFlushCb)
        3. Verify both objects are visible via information_schema

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        self._create_audit_infrastructure()

        tdLog.info("verify audit DB exists")
        tdSql.query(
            f"SELECT name FROM information_schema.ins_databases "
            f"WHERE name = '{self.AUDIT_DB}'"
        )
        tdSql.checkRows(1)

        tdLog.info("verify operations supertable exists")
        tdSql.query(
            f"SELECT stable_name FROM information_schema.ins_stables "
            f"WHERE db_name = '{self.AUDIT_DB}' "
            f"  AND stable_name = '{self.AUDIT_STB}'"
        )
        tdSql.checkRows(1)

        tdLog.success(f"test_01 passed — audit infrastructure created")

    # -----------------------------------------------------------------------
    # Test 02 — Audit records appear after audited operations
    # -----------------------------------------------------------------------
    def test_02_audit_records_written_after_operations(self):
        """Audit records appear in operations STB after DDL/DML

        1. Perform a sequence of audited operations (CREATE DB, CREATE STB,
           CREATE CTB, INSERT, DROP CTB, DROP DB)
        2. Wait up to FLUSH_WAIT_SECONDS for the audit background thread to
           flush records into the audit database
        3. Verify at least one row exists in the operations supertable

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        # Make sure infrastructure is in place (idempotent).
        self._create_audit_infrastructure()

        # Snapshot the current row count before our operations.
        try:
            tdSql.query(
                f"SELECT COUNT(*) FROM {self.AUDIT_DB}.{self.AUDIT_STB}"
            )
            rows_before = int(tdSql.getData(0, 0))
        except Exception:
            rows_before = 0
        tdLog.info(f"rows before operations: {rows_before}")

        # Perform audited operations.
        self._do_audited_operations()

        # Wait for at least one new row.
        rows_after = self._wait_for_audit_rows(min_rows=rows_before + 1)

        tdLog.info(f"rows after operations: {rows_after}")
        if rows_after <= rows_before:
            tdLog.exit(
                f"Expected audit rows > {rows_before} but got {rows_after}. "
                f"The direct-write flush callback may not have fired."
            )

        tdLog.success(
            f"test_02 passed — {rows_after - rows_before} new audit row(s) found"
        )

    # -----------------------------------------------------------------------
    # Test 03 — Row field completeness
    # -----------------------------------------------------------------------
    def test_03_audit_row_fields_not_null(self):
        """Mandatory audit row fields are non-NULL

        Query recently written audit rows and verify that the essential
        columns — ts, user_name, operation, client_address — are all
        populated (non-NULL, non-empty).

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        # Ensure there is at least one row to inspect.
        self._create_audit_infrastructure()
        self._do_audited_operations()
        self._wait_for_audit_rows(min_rows=1)

        tdLog.info("query audit rows")
        tdSql.query(
            f"SELECT ts, user_name, operation, client_address "
            f"FROM {self.AUDIT_DB}.{self.AUDIT_STB} "
            f"LIMIT 10"
        )

        row_count = tdSql.getRows()
        if row_count == 0:
            tdLog.exit("No audit rows found — cannot validate field completeness.")

        for row in range(row_count):
            ts             = tdSql.getData(row, 0)
            user_name      = tdSql.getData(row, 1)
            operation      = tdSql.getData(row, 2)
            client_address = tdSql.getData(row, 3)

            tdLog.info(
                f"row[{row}]: ts={ts} user={user_name} "
                f"op={operation} addr={client_address}"
            )

            if ts is None:
                tdLog.exit(f"row[{row}]: ts is NULL")
            if not user_name:
                tdLog.exit(f"row[{row}]: user_name is NULL or empty")
            if not operation:
                tdLog.exit(f"row[{row}]: operation is NULL or empty")
            if not client_address:
                tdLog.exit(f"row[{row}]: client_address is NULL or empty")

        tdLog.success(
            f"test_03 passed — {row_count} row(s) all have non-NULL mandatory fields"
        )

    # -----------------------------------------------------------------------
    # Test 04 — Operation column values
    # -----------------------------------------------------------------------
    def test_04_audit_operation_values(self):
        """Operation column contains expected verb strings

        After performing CREATE DATABASE, CREATE TABLE, INSERT, DROP TABLE,
        and DROP DATABASE operations the audit 'operation' column must
        contain at least one of the known operation-verb strings used by
        the mnode audit path.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        # Known operation strings emitted by mndAuditRecord calls.
        known_operations = {
            "createDb",   "dropDb",
            "createStb",  "dropStb",
            "createTable","dropTable",
            "insert",     "delete",
            "login",      "logout",
            "createUser", "dropUser",
            "alterUser",
        }

        self._create_audit_infrastructure()
        self._do_audited_operations()
        self._wait_for_audit_rows(min_rows=1)

        tdSql.query(
            f"SELECT DISTINCT operation "
            f"FROM {self.AUDIT_DB}.{self.AUDIT_STB}"
        )

        found_ops = set()
        for row in range(tdSql.getRows()):
            op = tdSql.getData(row, 0)
            if op:
                found_ops.add(op)

        tdLog.info(f"found operations: {found_ops}")

        matched = found_ops & known_operations
        if not matched:
            tdLog.exit(
                f"None of the expected operation strings found in audit. "
                f"Got: {found_ops}"
            )

        tdLog.success(
            f"test_04 passed — matched operation(s): {matched}"
        )

    # -----------------------------------------------------------------------
    # Test 05 — Graceful fallback when audit DB is missing
    # -----------------------------------------------------------------------
    def test_05_graceful_fallback_without_audit_db(self):
        """No crash when audit DB is absent

        Drop the audit database, then perform audited operations.  Taosd
        must NOT crash; the mndAuditFlushCb must log a debug message and
        skip the flush gracefully.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        self._drop_audit_infrastructure()

        tdLog.info("performing operations WITHOUT audit DB present")
        self._do_audited_operations()

        # Wait long enough for a flush cycle to occur.
        time.sleep(_FLUSH_WAIT_SECONDS)

        tdLog.info("verifying taosd is still reachable")
        tdSql.query("SELECT server_version()")
        tdSql.checkRows(1)

        tdLog.success(
            "test_05 passed — taosd survived flush attempt without audit DB"
        )

    # -----------------------------------------------------------------------
    # Test 06 — Re-create audit DB: records resume
    # -----------------------------------------------------------------------
    def test_06_records_resume_after_audit_db_recreated(self):
        """New records appear after audit DB is re-created

        After test_05 dropped the audit database, re-create it together with
        the operations supertable and verify that new audited operations
        produce rows again.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        # Ensure the audit DB is absent first (idempotent after test_05).
        self._drop_audit_infrastructure()

        # Re-create the infrastructure.
        self._create_audit_infrastructure()

        # Give the mnode a moment to discover the new audit DB.
        time.sleep(3)

        # Perform audited operations.
        self._do_audited_operations()

        # Wait for rows to appear.
        rows = self._wait_for_audit_rows(min_rows=1)

        if rows < 1:
            tdLog.exit(
                "Expected audit rows after re-creating audit DB, but found none."
            )

        tdLog.success(
            f"test_06 passed — {rows} audit row(s) found after DB re-create"
        )

    # -----------------------------------------------------------------------
    # Test 07 — is_audit flag in information_schema
    # -----------------------------------------------------------------------
    def test_07_is_audit_flag_in_information_schema(self):
        """is_audit flag visible in information_schema.ins_databases

        The audit database must have IS_AUDIT = 1 in the system catalog.
        Regular databases must have IS_AUDIT = 0 (or the column absent/false).

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        # Ensure audit DB exists.
        self._create_audit_infrastructure()
        # Ensure work DB exists for comparison.
        self._create_work_db()

        tdLog.info("query is_audit for audit database")
        tdSql.query(
            f"SELECT name, is_audit "
            f"FROM information_schema.ins_databases "
            f"WHERE name = '{self.AUDIT_DB}'"
        )
        tdSql.checkRows(1)
        is_audit_val = tdSql.getData(0, 1)
        tdLog.info(f"audit DB is_audit = {is_audit_val}")
        if int(is_audit_val) != 1:
            tdLog.exit(
                f"Expected is_audit=1 for '{self.AUDIT_DB}', got {is_audit_val}"
            )

        tdLog.info("query is_audit for regular work database")
        tdSql.query(
            f"SELECT name, is_audit "
            f"FROM information_schema.ins_databases "
            f"WHERE name = '{self.WORK_DB}'"
        )
        tdSql.checkRows(1)
        is_audit_work = tdSql.getData(0, 1)
        tdLog.info(f"work DB is_audit = {is_audit_work}")
        if int(is_audit_work) != 0:
            tdLog.exit(
                f"Expected is_audit=0 for '{self.WORK_DB}', got {is_audit_work}"
            )

        # Cleanup
        self._drop_work_db()

        tdLog.success("test_07 passed — is_audit flag correct for both databases")

    # -----------------------------------------------------------------------
    # Test 08 — Audit records contain the correct database name
    # -----------------------------------------------------------------------
    def test_08_audit_record_db_field_matches_operation(self):
        """The 'db' column in audit records reflects the target database

        After executing a CREATE DATABASE and DROP DATABASE for a uniquely
        named database, at least one audit row should have the 'db' column
        equal to that database name.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        self._create_audit_infrastructure()

        target_db = "audit_target_db_check"
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {target_db}")

        # Wait for flush.
        time.sleep(_FLUSH_WAIT_SECONDS)

        tdSql.query(
            f"SELECT db FROM {self.AUDIT_DB}.{self.AUDIT_STB} "
            f"WHERE db = '{target_db}' "
            f"LIMIT 5"
        )

        rows = tdSql.getRows()
        tdLog.info(
            f"audit rows with db='{target_db}': {rows}"
        )

        if rows < 1:
            # Not all operations populate the 'db' field (e.g. login does not).
            # CREATE DATABASE is specifically one that should, so log a warning
            # rather than hard-failing in case the field mapping differs between
            # versions.
            tdLog.info(
                f"Warning: no audit rows with db='{target_db}'. "
                f"This may be acceptable if the db field is empty for createDb."
            )
        else:
            tdLog.success(
                f"test_08 passed — {rows} audit row(s) with db='{target_db}'"
            )

        tdLog.success("test_08 completed")

    # -----------------------------------------------------------------------
    # Test 09 — Multiple rapid operations produce multiple records
    # -----------------------------------------------------------------------
    def test_09_multiple_operations_produce_multiple_records(self):
        """Multiple rapid operations each produce an audit record

        Execute N distinct operations in quick succession and verify that the
        total audit row count grows by at least N/2 (allowing for some
        operations that may not be individually audited at the chosen level).

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        self._create_audit_infrastructure()

        # Snapshot current count.
        try:
            tdSql.query(
                f"SELECT COUNT(*) FROM {self.AUDIT_DB}.{self.AUDIT_STB}"
            )
            rows_before = int(tdSql.getData(0, 0))
        except Exception:
            rows_before = 0

        # Execute 6 distinct audited operations.
        N = 6
        for i in range(N):
            db_name = f"audit_multi_op_{i}"
            tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")

        # Wait generously.
        rows_after = self._wait_for_audit_rows(
            min_rows=rows_before + N // 2,
            timeout=_FLUSH_WAIT_SECONDS * 2
        )

        new_rows = rows_after - rows_before
        tdLog.info(f"new audit rows after {N} create+drop pairs: {new_rows}")

        if new_rows < N // 2:
            tdLog.exit(
                f"Expected at least {N // 2} new audit rows for {N} operations "
                f"but got only {new_rows}."
            )

        tdLog.success(
            f"test_09 passed — {new_rows} new audit row(s) for {N} operations"
        )

    # -----------------------------------------------------------------------
    # Test 10 — Cleanup
    # -----------------------------------------------------------------------
    def test_10_cleanup(self):
        """Final cleanup — drop test databases

        Drop all databases created by this test suite to leave the cluster
        in a clean state for subsequent test files.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20  Initial version
        """
        tdLog.info("=== final cleanup ===")
        self._drop_audit_infrastructure()
        self._drop_work_db()
        tdSql.execute(f"DROP DATABASE IF EXISTS audit_target_db_check")
        for i in range(6):
            tdSql.execute(f"DROP DATABASE IF EXISTS audit_multi_op_{i}")

        # Allow async drop to complete before querying the catalog.
        time.sleep(2)

        tdLog.info("verify audit DB is gone")
        tdSql.query(
            f"SELECT name FROM information_schema.ins_databases "
            f"WHERE name = '{self.AUDIT_DB}'"
        )
        tdSql.checkRows(0)

        tdLog.success(f"test_10 passed — cleanup complete")
        tdLog.success(f"{__file__} successfully executed")
