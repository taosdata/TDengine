from new_test_framework.utils import tdLog, tdSql
import time


class TestRestoreParallel:
    """Test RESTORE DNODE / RESTORE VNODE ON DNODE with PARALLEL option.

    This test verifies:
    1. RESTORE DNODE ... PARALLEL N syntax is accepted (parse-level)
    2. RESTORE VNODE ON DNODE ... PARALLEL N syntax is accepted (parse-level)
    3. RESTORE without PARALLEL (default behavior, backward compatible)
    4. PARALLEL 0 (unlimited) is accepted
    5. Negative PARALLEL values are rejected at syntax level
    6. Missing integer after PARALLEL is rejected
    7. Non-integer PARALLEL values are rejected

    Since: v3.4.0.0

    Labels: cluster, restore, parallel

    Jira: None

    History:
        - 2026-07-10 Created

    """

    dnode_nums = 3

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def waitTransactionDone(self, timeout=120):
        """Wait until all transactions are finished.

        RESTORE DNODE triggers a transaction; here we poll `show transactions` until it is empty.
        """
        for i in range(timeout):
            tdSql.query("show transactions")
            if tdSql.queryRows == 0:
                return True
            time.sleep(1)
        tdLog.info(f"transactions not done within {timeout}s")
        return False

    def _check_parse_ok(self, sql):
        """Execute SQL; a syntax error is treated as failure, while business errors are tolerated.

        A RESTORE command with correct syntax enters the translate/execution stage. If it fails
        due to business reasons such as dnode state, it means the syntax passed and is considered
        a pass; only syntax-level errors are treated as failures.
        """
        try:
            tdSql.execute(sql)
            tdLog.info(f"parse ok (executed): {sql}")
        except Exception as e:
            msg = str(e).lower()
            # A syntax error means parsing failed and should not appear for valid syntax
            if "syntax error" in msg:
                tdLog.exit(f"unexpected syntax error for valid sql: {sql}, err: {e}")
            else:
                # Business-level error (e.g. dnode does not need restore / dnode offline); syntax already passed
                tdLog.info(f"parse ok (business err tolerated): {sql}, err: {e}")

    def test_restore_dnode_parallel_syntax(self):
        """Test RESTORE DNODE with PARALLEL parameter syntax.

        Verify that RESTORE DNODE <id> PARALLEL <n> syntax can be parsed correctly.
        Since in a single-node environment dnode 1 may not be in a state that needs restoring,
        business-level errors are tolerated; only that no syntax error is reported at the parse level.
        """
        # Valid syntax: with PARALLEL
        self._check_parse_ok("restore dnode 1 parallel 4")
        self._check_parse_ok("restore dnode 1 parallel 0")
        self._check_parse_ok("restore dnode 1 parallel 1")
        self._check_parse_ok("restore dnode 1 parallel 100")

        # Valid syntax: without PARALLEL (backward compatible)
        self._check_parse_ok("restore dnode 1")

        tdLog.success(f"{__file__} test_restore_dnode_parallel_syntax successfully executed")

    def test_restore_vnode_parallel_syntax(self):
        """Test RESTORE VNODE ON DNODE with PARALLEL parameter syntax.

        Verify that RESTORE VNODE ON DNODE <id> PARALLEL <n> syntax can be parsed correctly.
        """
        # Valid syntax: with PARALLEL
        self._check_parse_ok("restore vnode on dnode 1 parallel 2")
        self._check_parse_ok("restore vnode on dnode 1 parallel 0")
        self._check_parse_ok("restore vnode on dnode 1 parallel 1")

        # Valid syntax: without PARALLEL (backward compatible)
        self._check_parse_ok("restore vnode on dnode 1")

        tdLog.success(f"{__file__} test_restore_vnode_parallel_syntax successfully executed")

    def test_restore_parallel_negative_rejected(self):
        """Test that negative PARALLEL values are rejected.

        The NK_INTEGER token does not include a minus sign; the - in PARALLEL -1 is a separate
        token, so the parser should report an error directly.
        """
        tdSql.error("restore dnode 1 parallel -1")
        tdLog.info("restore dnode with negative PARALLEL correctly rejected")

        tdSql.error("restore vnode on dnode 1 parallel -1")
        tdLog.info("restore vnode on dnode with negative PARALLEL correctly rejected")

        tdLog.success(f"{__file__} test_restore_parallel_negative_rejected successfully executed")

    def test_restore_parallel_missing_value_rejected(self):
        """Test that PARALLEL without an integer value is rejected.

        PARALLEL must be followed by an integer; a missing integer should raise a syntax error.
        """
        tdSql.error("restore dnode 1 parallel")
        tdLog.info("restore dnode with missing PARALLEL value correctly rejected")

        tdSql.error("restore vnode on dnode 1 parallel")
        tdLog.info("restore vnode on dnode with missing PARALLEL value correctly rejected")

        tdLog.success(f"{__file__} test_restore_parallel_missing_value_rejected successfully executed")

    def test_restore_parallel_non_integer_rejected(self):
        """Test that non-integer PARALLEL values are rejected.

        A non-integer after PARALLEL (such as an identifier or a float) should raise a syntax error.
        """
        tdSql.error("restore dnode 1 parallel abc")
        tdLog.info("restore dnode with non-integer PARALLEL correctly rejected")

        tdSql.error("restore dnode 1 parallel 1.5")
        tdLog.info("restore dnode with float PARALLEL correctly rejected")

        tdLog.success(f"{__file__} test_restore_parallel_non_integer_rejected successfully executed")
