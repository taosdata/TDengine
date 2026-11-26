# Copyright (c) 2026 TDengine Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL") as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql


class TestVstbAlterColConflictMsg:
    """ALTER STABLE on a vstb conflicting with a vchild own column names the source.

    Defect 7160321463: ALTER STABLE vstb ADD COLUMN <name> hit 0x525 "Table column
    already exists" and DROP COLUMN hit 0x2602 "Invalid column name" without pointing
    at the virtual child table that actually owns the conflicting column. The fix
    enriches the error message with the child table (and column) name.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def setup_method(self, method):
        tdSql.execute("drop database if exists vconf_db")
        tdSql.execute("create database vconf_db vgroups 1")
        tdSql.execute("use vconf_db")
        # virtual super table + source table for column references
        tdSql.execute("create stable vst (ts timestamp, v int) tags (loc int) virtual 1")
        tdSql.execute("create table org_src (ts timestamp, src int)")
        # virtual child table referencing org_src
        tdSql.execute("create vtable vct (v from org_src.src) using vst tags (1)")
        # give the virtual child an own column that the stable does not have
        # (type must match the reference column org_src.src, which is INT)
        tdSql.execute("alter vtable vct add column DoorOpen int from org_src.src")

    def teardown_method(self, method):
        tdSql.execute("drop database if exists vconf_db")

    def test_add_column_conflict_names_child(self):
        """ADD COLUMN on vstb conflicting with a vchild own column names the child

        1. the stable itself has no DoorOpen column (DESCRIBE confirms)
        2. ADD COLUMN DoorOpen fails with error code 0x525 (column already exists)
        3. the error message names the conflicting child table vct

        Catalog:
            - VirtualTable:DDL

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-06 Yihao Deng Created for defect 7160321463

        """
        # 1. confirm the stable itself has no such column (DESCRIBE lower-cases names)
        tdSql.query("describe vst")
        for i in range(tdSql.queryRows):
            assert str(tdSql.getData(i, 0)).lower() != "dooropen", f"DoorOpen unexpectedly on vst: row {i}"

        # 2. ADD COLUMN -> 0x525 with child table name in the message
        #    error code must stay 0x525 (column already exists, the fix only enriches the text)
        tdSql.error(
            "alter stable vst add column DoorOpen bool",
            expectedErrno=0x0525,
            expectErrInfo="already exists",
            fullMatched=False,
        )
        # the message must point at the conflicting child table
        tdSql.error(
            "alter stable vst add column DoorOpen bool",
            expectedErrno=0x0525,
            expectErrInfo="vct",
            fullMatched=False,
        )

    def test_drop_column_conflict_hints_at_child(self):
        """DROP COLUMN on vstb for a column only a vchild owns gives a clear message

        1. DROP COLUMN DoorOpen fails (the column is not on the stable)
        2. the error message mentions the column may exist on a virtual child table

        Catalog:
            - VirtualTable:DDL

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-06 Yihao Deng Created for defect 7160321463

        """
        tdSql.error(
            "alter stable vst drop column DoorOpen",
            expectedErrno=0x2602,
            expectErrInfo="own column",
            fullMatched=False,
        )

    def test_no_false_positive_when_no_conflict(self):
        """ADD COLUMN on vstb without any conflict still succeeds

        1. add a new column the stable and no vchild owns
        2. the alter succeeds and the column appears in DESCRIBE

        Catalog:
            - VirtualTable:DDL

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-06 Yihao Deng Created for defect 7160321463

        """
        tdSql.execute("alter stable vst add column NewCol int")
        tdSql.query("describe vst")
        # TDengine lower-cases identifiers, so the column is reported as "newcol"
        found = any(str(tdSql.getData(i, 0)).lower() == "newcol" for i in range(tdSql.queryRows))
        assert found, "NewCol not found on vst after successful ADD COLUMN"
