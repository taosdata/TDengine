"""Community edition: enterprise-only features must fail with a clear reminder.

Requirement: TS-6507051705 / TS-7204

Before this change a community-edition user running an enterprise-only SQL
statement either received a bare ``Operation not supported [0x80000100]`` or,
for a few statements (e.g. ``SPLIT VGROUP``, ``BALANCE VGROUP LEADER``), the
statement silently did nothing because the community mnode stub returns
success.

The community build now rejects these statements in the parser
(``parTranslater.c``, macro ``PAR_ENTERPRISE_ONLY``) with a user-visible
message that names the unsupported feature::

    Operation not supported in community edition: <FEATURE>

The generic form of the message is also logged on the server side, while the
detailed form is written into the request's ``msgBuf`` and returned verbatim by
``taos_errstr()``, so it reaches the client even though the error code stays
``TSDB_CODE_OSS_NOT_SUPPORT``.

The test is community-only; on an enterprise build every statement below would
legitimately succeed, so the whole class is skipped there.
"""

import pytest

from new_test_framework.utils import tdLog, tdSql

ENTERPRISE_MSG = "Operation not supported in community edition"

# Enterprise-only statements a community build must reject with an explicit
# reminder: (statement, feature name expected in the message).
#
# The statements are rejected at the very top of each translate*() function,
# before any object/parameter validation, so the parser refuses them even when
# the referenced database/table/vgroup does not exist. This mirrors how the
# community mnode stubs behave (they either answer OPS_NOT_SUPPORT or, worse,
# silently return success) — see the mnd* community stubs.
#
# Keep this list in sync with every PAR_ENTERPRISE_ONLY(pCxt, ...) call site in
# parTranslater.c: 37 call sites cover the 35 distinct feature strings below
# (BEGIN/COMMIT/ROLLBACK share one "Transaction" message).
ENTERPRISE_ONLY_SQL = [
    # ---- database / vgroup maintenance -----------------------------------
    ("compact database test_ee", "COMPACT DATABASE"),
    ("compact test_ee. vgroups in (1)", "COMPACT VGROUPS"),
    ("rollup database test_ee", "ROLLUP DATABASE"),
    ("rollup test_ee. vgroups in (1)", "ROLLUP VGROUPS"),
    ("kill compact 1", "KILL COMPACT"),
    # ---- backup / restore -------------------------------------------------
    ("create full backup on dnode 1 to '/tmp/ee_backup'", "BACKUP DATABASE"),
    ("kill backup 1", "KILL BACKUP"),
    ("restore dnode 1", "RESTORE DNODE"),
    # ---- view / rsma ------------------------------------------------------
    ("create view test_ee.view_ee as select * from test_ee.t_ee", "CREATE VIEW"),
    ("drop view test_ee.view_ee", "DROP VIEW"),
    ("show create view test_ee.view_ee", "SHOW CREATE VIEW"),
    (
        "create rsma if not exists rsma_ee on test_ee.t_ee function(avg(c1)) interval(1d, 2d)",
        "CREATE RSMA",
    ),
    ("alter rsma test_ee.rsma_ee function(avg(c1))", "ALTER RSMA"),
    ("drop rsma test_ee.rsma_ee", "DROP RSMA"),
    ("show create rsma test_ee.rsma_ee", "SHOW CREATE RSMA"),
    # ---- cluster balancing ------------------------------------------------
    ("balance vgroup leader", "BALANCE VGROUP LEADER"),
    ("split vgroup 1", "SPLIT VGROUP"),
    # ---- security: token / totp / encryption ------------------------------
    ("create token tk_ee from user root", "CREATE TOKEN"),
    ("alter token tk_ee enable 1", "ALTER TOKEN"),
    ("drop token tk_ee", "DROP TOKEN"),
    ("create totp_secret for user root", "CREATE TOTP SECRET"),
    ("drop totp_secret from user root", "DROP TOTP SECRET"),
    ("alter system set svr_key 'ee_new_key'", "ALTER ENCRYPT KEY"),
    ("alter system set key_expiration 90 days strategy 'delayed'", "ALTER KEY EXPIRATION"),
    # ---- analytics nodes ---------------------------------------------------
    ("create anode 'ep'", "CREATE ANODE"),
    ("update anode 1", "UPDATE ANODE"),
    ("drop anode 1", "DROP ANODE"),
    # ---- transactions ------------------------------------------------------
    ("begin", "Transaction (BEGIN/COMMIT/ROLLBACK)"),
    ("commit", "Transaction (BEGIN/COMMIT/ROLLBACK)"),
    ("rollback", "Transaction (BEGIN/COMMIT/ROLLBACK)"),
    # ---- federated query: external sources --------------------------------
    (
        "create external source src_ee TYPE='mysql' HOST='127.0.0.1' PORT=3306 "
        "USER='u' PASSWORD='p' DATABASE=test",
        "CREATE EXTERNAL SOURCE",
    ),
    ("alter external source src_ee set host='10.0.0.1'", "ALTER EXTERNAL SOURCE"),
    ("drop external source src_ee", "DROP EXTERNAL SOURCE"),
    ("refresh external source src_ee", "REFRESH EXTERNAL SOURCE"),
    ("show external sources", "SHOW EXTERNAL SOURCES"),
    ("describe external source src_ee", "DESCRIBE EXTERNAL SOURCE"),
    ("show external source src_ee capability", "SHOW EXTERNAL SOURCE CAPABILITY"),
]


class TestEnterpriseFeatureReminder:

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    def _is_community_edition(self):
        tdSql.query("select server_version()")
        return "community" in str(tdSql.getData(0, 0)).lower()

    def test_01_enterprise_only_sql_reports_clear_error(self):
        """Community edition reports a clear enterprise-only reminder

        1. Skip the check on enterprise builds, where these statements are supported
        2. Run every enterprise-only statement on a community build
        3. Verify it fails and that the error message names the unsupported feature

        Covers all PAR_ENTERPRISE_ONLY call sites in parTranslater.c (37 call
        sites / 35 distinct feature strings; BEGIN/COMMIT/ROLLBACK share the
        "Transaction" message): database & vgroup maintenance, backup & restore,
        view & rsma, cluster balancing, token/totp/encryption, analytics nodes,
        transactions, and federated external sources.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-13 kailixu Add community edition enterprise-only reminder check (TS-6507051705)
            - 2026-09-14 kailixu Cover every PAR_ENTERPRISE_ONLY call site (37 call sites / 35 feature strings)
        """
        if not self._is_community_edition():
            pytest.skip("community edition only; these features are supported on enterprise edition")

        failures = []
        for sql, feature in ENTERPRISE_ONLY_SQL:
            tdLog.info(f"check enterprise-only reminder for: {sql}")
            try:
                # Bypass tdSql.execute's retry loop: a rejected statement must
                # not be retried, and the raw message must be inspected as-is.
                tdSql.cursor.execute(sql)
                failures.append(f"{sql}: expected an error but the statement succeeded")
            except Exception as e:
                msg = str(e)
                if ENTERPRISE_MSG not in msg:
                    failures.append(f"{sql}: missing the enterprise reminder, got: {msg}")
                elif feature not in msg:
                    failures.append(f"{sql}: expected feature '{feature}' in the message, got: {msg}")

        if failures:
            raise AssertionError(
                f"{len(failures)} enterprise-only reminder check(s) failed:\n  " + "\n  ".join(failures)
            )

        tdLog.info(f"all {len(ENTERPRISE_ONLY_SQL)} enterprise-only statements reported a clear reminder")
