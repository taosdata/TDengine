# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
import glob
import os
import time

from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.srvCtl import sc

DB_NAME = "test_vtable_ref_idem"
VCTB = "vtb_ref_idem_ctb0"
VNTB = "vtb_ref_idem_ntb0"

# metaAlterTableColumnRef / metaRemoveTableColumnRef emit exactly one of these per successful
# statement. END means the entry was rewritten and the row-schema / colRef versions were bumped
# (the meta-cache invalidation we want to avoid); SKIP means the request matched what was already
# stored and nothing was written.
KEYOP_PHASES = ("START", "END", "SKIP")


class TestVtableAlterRefIdempotent:
    def setup_class(cls):
        TestVtableAlterRefIdempotent.prepare_tables()

    @staticmethod
    def prepare_tables():
        tdLog.info("prepare org tables."
        )
        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"create database {DB_NAME};")
        tdSql.execute(f"use {DB_NAME};")

        # bigint_col_b exists so that "re-point to another column of the same source table" can be
        # expressed without also changing the column type: ALTER COLUMN REF rejects a type
        # mismatch in the parser, long before the idempotence guard this file is about.
        tdSql.execute(
            "CREATE STABLE `vtb_org_stb` ("
            "ts timestamp, int_col int, bigint_col bigint, bigint_col_b bigint, bool_col bool"
            ") TAGS (int_tag int)"
        )
        for i in range(4):
            tdSql.execute(
                f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i});"
            )
            tdSql.execute(
                f"INSERT INTO `vtb_org_child_{i}` "
                f"VALUES (now, {i}, {i * 100}, {i * 1000}, true);"
            )

        tdLog.info("prepare virtual tables.")
        tdSql.execute(
            "CREATE STABLE `vtb_ref_idem_stb` ("
            "ts timestamp, int_col int, bigint_col bigint, bool_col bool"
            ") TAGS (int_tag int) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE `{VCTB}` ("
            "int_col FROM vtb_org_child_0.int_col, "
            "bigint_col FROM vtb_org_child_0.bigint_col, "
            "bool_col FROM vtb_org_child_0.bool_col) "
            "USING vtb_ref_idem_stb TAGS (0)"
        )
        tdSql.execute(
            f"CREATE VTABLE `{VNTB}` ("
            "ts timestamp, "
            "int_col int from vtb_org_child_0.int_col, "
            "bigint_col bigint from vtb_org_child_0.bigint_col, "
            "bool_col bool from vtb_org_child_0.bool_col)"
        )

    # --- taosd log evidence -------------------------------------------------
    #
    # There is no SQL-visible handle on the vnode-side schema version of a virtual table, so the
    # KEYOP markers the ALTER path already emits are what the assertions read. They are counted
    # as deltas against a baseline taken immediately before each statement, and filtered by table
    # name so an unrelated table's ALTER cannot be mistaken for this one's.

    @staticmethod
    def _log_files():
        log_dir = sc.dnodeLogPath(1)
        files = sorted(glob.glob(os.path.join(log_dir, "taosdlog.*")))
        if not files:
            raise AssertionError(f"no taosd log files under {log_dir}")
        return files

    @classmethod
    def _counts(cls, table):
        """Count the vtable-alter KEYOP lines logged for `table`, per phase."""
        needle = f"table {table} "
        counts = {phase: 0 for phase in KEYOP_PHASES}
        for path in cls._log_files():
            try:
                with open(path, "rb") as log_file:
                    content = log_file.read().decode("utf-8", errors="ignore")
            except FileNotFoundError:
                # Rotated away between glob and open; the remaining files still hold the
                # markers this test wrote moments ago.
                continue
            for line in content.splitlines():
                if needle not in line:
                    continue
                for phase in KEYOP_PHASES:
                    if f"[KEYOP] op=vtable-alter phase={phase}" in line:
                        counts[phase] += 1
        return counts

    @classmethod
    def _wait_for_decisions(cls, table, baseline, expected, timeout=30):
        """Wait until `expected` statements have been resolved to an END or a SKIP in the log.

        Waiting on the total rather than on one phase keeps a regression visible: a broken guard
        still logs the same number of lines, it just logs END where SKIP belongs, so the caller's
        split assertion fails instead of this helper timing out.
        """
        deadline = time.time() + timeout
        while True:
            counts = cls._counts(table)
            # A negative delta means taosd rotated away a log file that the baseline had counted,
            # so the deltas no longer describe the statements this call issued. Say that outright
            # instead of letting it surface as a bogus "expected 3 skips but saw -4".
            regressed = [
                phase for phase in KEYOP_PHASES if counts[phase] < baseline[phase]
            ]
            if regressed:
                raise AssertionError(
                    f"taosd log rotated during the test for {table}: phase(s) {regressed} went "
                    f"backwards (counts={counts}, baseline={baseline}); the KEYOP deltas are not "
                    f"usable, re-run with a larger log retention"
                )
            decided = (counts["END"] - baseline["END"]) + (counts["SKIP"] - baseline["SKIP"])
            if decided >= expected:
                return counts
            if time.time() >= deadline:
                raise AssertionError(
                    f"only {decided} of {expected} vtable-alter decisions reached the taosd log "
                    f"for {table} within {timeout}s (counts={counts}, baseline={baseline})"
                )
            time.sleep(0.5)

    @classmethod
    def _run_alters(cls, table, statements, expect_end, expect_skip):
        baseline = cls._counts(table)
        for statement in statements:
            tdSql.execute(statement)
        counts = cls._wait_for_decisions(table, baseline, expect_end + expect_skip)

        end_delta = counts["END"] - baseline["END"]
        skip_delta = counts["SKIP"] - baseline["SKIP"]
        tdLog.info(
            f"{table}: {len(statements)} statement(s) -> END +{end_delta}, SKIP +{skip_delta}"
        )
        assert end_delta == expect_end, (
            f"{table}: expected {expect_end} persisted vtable-alter(s) but saw {end_delta}; "
            f"statements={statements}"
        )
        assert skip_delta == expect_skip, (
            f"{table}: expected {expect_skip} skipped vtable-alter(s) but saw {skip_delta}; "
            f"statements={statements}"
        )

    @staticmethod
    def check_col_ref(table, col_name, col_ref):
        tdSql.query(
            f"select col_source from information_schema.ins_columns "
            f"where db_name='{DB_NAME}' and table_name='{table}' and col_name='{col_name}'"
        )
        tdSql.checkData(0, 0, col_ref)

    # --- tests --------------------------------------------------------------

    def test_repeated_identical_ref_alter_is_skipped(self):
        """Alter: repeated identical column ref on a virtual child table

        1. the first re-point of a column reference is persisted
        2. three byte-identical repeats of it are skipped: no entry write, no schema version bump
        3. the reference, the column meta and the query result are unchanged by the repeats

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-10-31 Created for the IDMP 6-hour full-rebind meta churn fix

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. a real re-point (child_0 -> child_1) must be written through.
        self._run_alters(
            VCTB,
            [f"alter vtable {VCTB} alter column int_col set vtb_org_child_1.int_col;"],
            expect_end=1,
            expect_skip=0,
        )
        self.check_col_ref(VCTB, "int_col", f"{DB_NAME}.vtb_org_child_1.int_col")

        # 2. IDMP re-binds every table on a 6-hour timer without diffing first; those repeats
        #    must not touch the entry.
        repeat = f"alter vtable {VCTB} alter column int_col set vtb_org_child_1.int_col;"
        self._run_alters(VCTB, [repeat] * 3, expect_end=0, expect_skip=3)

        # 3. the skipped statements left the reference and the data path intact.
        self.check_col_ref(VCTB, "int_col", f"{DB_NAME}.vtb_org_child_1.int_col")
        tdSql.query(f"select count(*) from {VCTB};")
        tdSql.checkRows(1)

    def test_changed_ref_still_persisted_after_skips(self):
        """Alter: a genuine ref change is still applied after idempotent repeats

        1. repeat an identical ref alter so the skip path has run
        2. re-point the same column to another table, then to another column
        3. each genuine change is persisted and visible in ins_columns

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-10-31 Created for the IDMP 6-hour full-rebind meta churn fix

        """
        tdSql.execute(f"use {DB_NAME};")

        self._run_alters(
            VCTB,
            [f"alter vtable {VCTB} alter column bigint_col set vtb_org_child_2.bigint_col;"],
            expect_end=1,
            expect_skip=0,
        )
        self._run_alters(
            VCTB,
            [f"alter vtable {VCTB} alter column bigint_col set vtb_org_child_2.bigint_col;"] * 2,
            expect_end=0,
            expect_skip=2,
        )

        # A different source table is a change.
        self._run_alters(
            VCTB,
            [f"alter vtable {VCTB} alter column bigint_col set vtb_org_child_3.bigint_col;"],
            expect_end=1,
            expect_skip=0,
        )
        self.check_col_ref(VCTB, "bigint_col", f"{DB_NAME}.vtb_org_child_3.bigint_col")

        # So is a different source column on the same table. It has to be another bigint column:
        # a type mismatch is rejected by the parser and would never reach the guard.
        self._run_alters(
            VCTB,
            [f"alter vtable {VCTB} alter column bigint_col set vtb_org_child_3.bigint_col_b;"],
            expect_end=1,
            expect_skip=0,
        )
        self.check_col_ref(VCTB, "bigint_col", f"{DB_NAME}.vtb_org_child_3.bigint_col_b")

    def test_rebind_after_set_null_is_not_skipped(self):
        """Alter: re-binding a cleared reference is a real change

        1. clear a column reference with SET NULL
        2. bind it back to exactly the reference it had before
        3. the re-bind must be persisted, not skipped, because the column currently has no ref

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-10-31 Created for the IDMP 6-hour full-rebind meta churn fix

        """
        tdSql.execute(f"use {DB_NAME};")

        self._run_alters(
            VCTB,
            [f"alter vtable {VCTB} alter column bool_col set vtb_org_child_1.bool_col;"],
            expect_end=1,
            expect_skip=0,
        )

        # SET NULL goes through metaRemoveTableColumnRef, which emits the same KEYOP markers;
        # re-binding the identical reference afterwards must be written, not skipped.
        self._run_alters(
            VCTB,
            [
                f"alter vtable {VCTB} alter column bool_col set NULL;",
                f"alter vtable {VCTB} alter column bool_col set vtb_org_child_1.bool_col;",
            ],
            expect_end=2,
            expect_skip=0,
        )
        self.check_col_ref(VCTB, "bool_col", f"{DB_NAME}.vtb_org_child_1.bool_col")

    def test_repeated_identical_ref_alter_on_virtual_normal_table(self):
        """Alter: repeated identical column ref on a virtual normal table

        1. the first re-point is persisted
        2. identical repeats are skipped on the normal-table path too, where the row schema is
           owned by the entry itself rather than by a super table
        3. a genuine change afterwards is still persisted

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-10-31 Created for the IDMP 6-hour full-rebind meta churn fix

        """
        tdSql.execute(f"use {DB_NAME};")

        self._run_alters(
            VNTB,
            [f"alter vtable {VNTB} alter column int_col set vtb_org_child_1.int_col;"],
            expect_end=1,
            expect_skip=0,
        )
        self.check_col_ref(VNTB, "int_col", f"{DB_NAME}.vtb_org_child_1.int_col")

        self._run_alters(
            VNTB,
            [f"alter vtable {VNTB} alter column int_col set vtb_org_child_1.int_col;"] * 3,
            expect_end=0,
            expect_skip=3,
        )
        self.check_col_ref(VNTB, "int_col", f"{DB_NAME}.vtb_org_child_1.int_col")

        self._run_alters(
            VNTB,
            [f"alter vtable {VNTB} alter column int_col set vtb_org_child_2.int_col;"],
            expect_end=1,
            expect_skip=0,
        )
        self.check_col_ref(VNTB, "int_col", f"{DB_NAME}.vtb_org_child_2.int_col")

        tdSql.query(f"select count(*) from {VNTB};")
        tdSql.checkRows(1)
