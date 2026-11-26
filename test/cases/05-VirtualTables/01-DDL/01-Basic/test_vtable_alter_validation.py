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

from new_test_framework.utils import tdLog, tdSql


DB = "test_vtable_alter_validation"
INVALID_ALTER = 0x2649
DUPLICATED_COLUMN = 0x263C


class TestVtableAlterValidation:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB}")
        tdSql.execute(f"USE {DB}")
        tdSql.execute("CREATE STABLE src (ts TIMESTAMP, v INT, w INT) TAGS (tg INT, other INT)")
        tdSql.execute("CREATE TABLE src_child USING src TAGS (7, 8)")
        for name in ("vst_empty", "vst_child"):
            tdSql.execute(
                f"CREATE STABLE {name} (ts TIMESTAMP, v INT, w INT) "
                "TAGS (tg INT, other INT) VIRTUAL 1"
            )
        tdSql.execute(
            "CREATE VTABLE vc (v FROM src_child.v, w FROM src_child.w) "
            "USING vst_child TAGS (7, 8)"
        )
        tdSql.execute(
            "CREATE VTABLE vn (ts TIMESTAMP, v INT FROM src_child.v, w INT FROM src_child.w) "
            "TAGS (tg INT = 7)"
        )
        tdSql.execute("CREATE TABLE nt (ts TIMESTAMP, v INT, w INT) TAGS (tg INT = 7)")

    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")

    # --- impl ---
    def do_reject_virtual_super_table(self):
        for table in ("vst_empty", "vst_child"):
            tdSql.query(f"SHOW CREATE STABLE {table}")
            original_schema = tdSql.getData(0, 1)
            for clause in (
                "ADD COLUMN extra INT",
                "DROP COLUMN w",
                "DROP COLUMN tg",
                "ADD TAG extra_tag INT",
                "DROP TAG other",
                "RENAME COLUMN w renamed",
                "COMMENT 'changed'",
            ):
                tdSql.error(
                    f"ALTER VTABLE {table} {clause}",
                    expectedErrno=INVALID_ALTER,
                    expectErrInfo="cannot alter virtual super table using alter vtable",
                    fullMatched=False,
                )
            tdSql.query(f"SHOW CREATE STABLE {table}")
            tdSql.checkData(0, 1, original_schema)

        tdSql.error("ALTER VTABLE src DROP COLUMN w", expectErrInfo="non-virtual table", fullMatched=False)
        # Both supported super-table spellings must still perform real schema changes.
        tdSql.execute("ALTER STABLE vst_empty ADD COLUMN extra INT")
        tdSql.query("DESC vst_empty")
        tdSql.checkKeyData("extra", 1, "INT")
        tdSql.execute("ALTER TABLE vst_empty DROP COLUMN extra")
        tdSql.query("DESC vst_empty")
        tdSql.checkRows(5)
        print("do_reject_virtual_super_table ................ [passed]")

    def do_reject_drop_tag_as_column(self):
        for keyword, table in (
            ("STABLE", "src"),
            ("TABLE", "src"),
            ("STABLE", "vst_empty"),
            ("TABLE", "vst_empty"),
            ("TABLE", "nt"),
            ("TABLE", "vn"),
            ("VTABLE", "vn"),
            ("TABLE", "vc"),
            ("VTABLE", "vc"),
        ):
            tdSql.error(
                f"ALTER {keyword} {table} DROP COLUMN tg",
                expectedErrno=INVALID_ALTER,
                expectErrInfo="Cannot drop tag tg using DROP COLUMN",
                fullMatched=False,
            )
            tdSql.query(f"DESC {table}")
            tdSql.checkKeyData("tg", 3, "TAG")
        tdSql.execute("ALTER VTABLE vc ADD TAG own_tag INT")
        tdSql.error(
            "ALTER VTABLE vc DROP COLUMN own_tag",
            expectErrInfo="Cannot drop tag own_tag using DROP COLUMN",
            fullMatched=False,
        )
        tdSql.execute("ALTER VTABLE vc DROP TAG own_tag")
        print("do_reject_drop_tag_as_column ................ [passed]")

    def do_tag_error_messages(self):
        for table in ("vc", "vn"):
            for reference in ("NULL", "src_child.v"):
                tdSql.error(
                    f"ALTER VTABLE {table} ALTER COLUMN tg SET {reference}",
                    expectedErrno=INVALID_ALTER,
                    expectErrInfo="Cannot alter column reference for tag tg",
                    fullMatched=False,
                )
            tdSql.error(
                f"ALTER VTABLE {table} ADD COLUMN tg INT FROM src_child.v",
                expectedErrno=DUPLICATED_COLUMN,
                expectErrInfo="Column tg already exists as a tag",
                fullMatched=False,
            )
            # The data-column reference operations remain valid.
            tdSql.execute(f"ALTER VTABLE {table} ALTER COLUMN v SET NULL")
            tdSql.execute(f"ALTER VTABLE {table} ALTER COLUMN v SET src_child.v")
            tdSql.query(f"DESC {table}")
            tdSql.checkKeyData("tg", 3, "TAG")

        tdSql.error(
            "ALTER VTABLE vc DROP TAG tg",
            expectedErrno=INVALID_ALTER,
            expectErrInfo="Tag tg belongs to super table, cannot drop it from child table",
            fullMatched=False,
        )
        tdSql.execute("ALTER VTABLE vc ADD TAG own_tag INT")
        tdSql.error(
            "ALTER VTABLE vc ADD COLUMN own_tag INT FROM src_child.v",
            expectedErrno=DUPLICATED_COLUMN,
            expectErrInfo="Column own_tag already exists as a tag",
            fullMatched=False,
        )
        tdSql.execute("ALTER VTABLE vc DROP TAG own_tag")
        tdSql.execute("ALTER VTABLE vc ADD COLUMN extra INT FROM src_child.w")
        tdSql.execute("ALTER VTABLE vc DROP COLUMN extra")
        tdSql.query("DESC vc")
        tdSql.checkRows(5)
        print("do_tag_error_messages ....................... [passed]")

    # --- main ---
    def test_vtable_alter_validation(self):
        """Virtual-table ALTER target and tag validation

        1. Reject ALTER VTABLE on empty and populated virtual super tables
        2. Reject DROP COLUMN on inherited and owned tags without changing the schema
        3. Explain tag reference, duplicate-name and inherited-tag DROP errors
        4. Preserve legal super-table, data-column reference and own-schema operations

        Catalog:
            - VirtualTable

        Since: v3.4.3.9

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 Simon Guan Created for defect 7158513684

        """
        self.do_reject_virtual_super_table()
        self.do_reject_drop_tag_as_column()
        self.do_tag_error_messages()
