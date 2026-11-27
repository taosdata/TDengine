# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# or later ("AGPL"), as published by TAOS Data.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql

DB_NAME = "test_vtable_own_col_rebind"


class TestVtableOwnColRebind:
    def setup_class(cls):
        TestVtableOwnColRebind.prepare_tables()

    @staticmethod
    def prepare_tables():
        tdLog.info("prepare org tables.")
        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"create database {DB_NAME};")
        tdSql.execute(f"use {DB_NAME};")

        # two runs of the same analysis task: rebinding _13 -> _14 is the incident scenario
        tdSql.execute("create table ana_13 (ts timestamp, c1 int, reason nchar(32));")
        tdSql.execute("create table ana_14 (ts timestamp, c1 int, reason nchar(32));")
        tdSql.execute("insert into ana_13 values (now, 13, 'r13');")
        tdSql.execute("insert into ana_14 values (now, 14, 'r14');")

        tdLog.info("prepare virtual tables.")
        tdSql.execute("create stable vstb (ts timestamp, c1 int) tags (t1 int) virtual 1;")
        # own column: explicit type + FROM binding, name not part of the vstb schema
        tdSql.execute(
            "create vtable vctb_own0 ("
            "c1 from ana_13.c1, "
            "reason nchar(32) from ana_13.reason"
            ") using vstb tags (1);"
        )
        # cascaded column only: the alter path that already worked before the fix
        tdSql.execute(
            "create vtable vctb_own1 (c1 from ana_13.c1) using vstb tags (2);"
        )

    @staticmethod
    def check_col_ref(table, col_name, col_ref):
        tdSql.query(
            f"select col_source from information_schema.ins_columns "
            f"where db_name='{DB_NAME}' and table_name='{table}' and col_name='{col_name}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, col_ref)

    @staticmethod
    def check_col_visible(table, col_name):
        """desc must keep listing the own column across its ref changes"""
        tdSql.query(f"desc {table};")
        names = [row[0] for row in tdSql.queryResult]
        assert col_name in names, f"column {col_name} missing from desc {table}: {names}"

    def test_rebind_own_column_declared_at_create(self):
        """Alter: rebind and clear an own column of a virtual child table

        An own column (declared with an explicit type and a FROM binding at CREATE
        VTABLE) is stored in the vchild's own schema area, not in the vstb row
        schema. Changing its binding must work exactly like a cascaded column:
        rebind to the second run of the analysis table, clear with SET NULL, bind
        back, and repeat an identical rebind - with the column and its binding
        visible in desc / show create / ins_columns after every step.

        Before the fix every one of these statements failed with 0x0526 because
        the vnode-side column lookup never scanned the own schema area.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-11-04 Yihao Deng Created for defect 7155574571

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. rebind to the second analysis table (the _13 -> _14 scenario)
        tdSql.execute("alter vtable vctb_own0 alter column reason set ana_14.reason;")
        self.check_col_ref("vctb_own0", "reason", f"{DB_NAME}.ana_14.reason")
        self.check_col_visible("vctb_own0", "reason")
        tdSql.query("select reason from vctb_own0;")
        tdSql.checkData(0, 0, "r14")

        # 2. clear the binding: no source involved, the target column is the own column
        tdSql.execute("alter vtable vctb_own0 alter column reason set null;")
        self.check_col_ref("vctb_own0", "reason", None)
        self.check_col_visible("vctb_own0", "reason")

        # 3. an identical repeat after the clear is still a real change to the same ref
        tdSql.execute("alter vtable vctb_own0 alter column reason set null;")
        self.check_col_ref("vctb_own0", "reason", None)

        # 4. bind back to the first analysis table
        tdSql.execute("alter vtable vctb_own0 alter column reason set ana_13.reason;")
        self.check_col_ref("vctb_own0", "reason", f"{DB_NAME}.ana_13.reason")
        self.check_col_visible("vctb_own0", "reason")
        tdSql.query("select reason from vctb_own0;")
        tdSql.checkData(0, 0, "r13")

        # 5. an identical rebind succeeds and keeps the binding
        tdSql.execute("alter vtable vctb_own0 alter column reason set ana_13.reason;")
        self.check_col_ref("vctb_own0", "reason", f"{DB_NAME}.ana_13.reason")

        # 6. show create keeps carrying the own column with its current binding
        #    (fully-qualified form: `db`.`ana_13`.`reason`)
        tdSql.query("show create table vctb_own0;")
        create_sql = str(tdSql.queryResult)
        assert "`ana_13`.`reason`" in create_sql, f"own column binding missing from show create: {create_sql}"

    def test_rebind_own_column_added_by_alter(self):
        """Alter: rebind and clear an own column added by ADD COLUMN

        ADD COLUMN <col> <type> FROM <src> creates the same kind of own column
        after CREATE VTABLE; rebind / SET NULL / rebind must work on it too, and
        a cascaded column of the same table must keep altering as before.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-11-04 Yihao Deng Created for defect 7155574571

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. add an own column bound to the first analysis table
        tdSql.execute("alter vtable vctb_own1 add column reason nchar(32) from ana_13.reason;")
        self.check_col_ref("vctb_own1", "reason", f"{DB_NAME}.ana_13.reason")
        self.check_col_visible("vctb_own1", "reason")

        # 2. rebind to the second table, then clear
        tdSql.execute("alter vtable vctb_own1 alter column reason set ana_14.reason;")
        self.check_col_ref("vctb_own1", "reason", f"{DB_NAME}.ana_14.reason")
        tdSql.execute("alter vtable vctb_own1 alter column reason set null;")
        self.check_col_ref("vctb_own1", "reason", None)

        # 3. the cascaded column of the same table still alters (regression guard)
        tdSql.execute("alter vtable vctb_own1 alter column c1 set ana_14.c1;")
        self.check_col_ref("vctb_own1", "c1", f"{DB_NAME}.ana_14.c1")

        # 4. rebind the own column again after the cascaded alter
        tdSql.execute("alter vtable vctb_own1 alter column reason set ana_13.reason;")
        self.check_col_ref("vctb_own1", "reason", f"{DB_NAME}.ana_13.reason")
        self.check_col_visible("vctb_own1", "reason")

    @staticmethod
    def db_tables():
        tdSql.query(
            f"select table_name from information_schema.ins_tables "
            f"where db_name='{DB_NAME}' order by table_name"
        )
        return [row[0] for row in tdSql.queryResult]

    def test_txn_create_identical_rebind_rollback(self):
        """Alter: identical rebind of a table created in the same txn rolls back with it

        A no-op ALTER COLUMN REF inside a batch meta txn must not strand the table
        when the table itself was created by the same txn: the ALTER takes the
        versioned write path, so ROLLBACK restores the create mark and chain-deletes
        the table together with the txn. Checked for an own column and for a
        cascaded column; a COMMIT control keeps the table and its binding.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-11-04 Yihao Deng Added for the review round of defect 7155574571

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. own column: identical rebind (no-op) inside the txn, then ROLLBACK
        tdSql.execute("begin;")
        tdSql.execute(
            "create vtable vctb_txn_own ("
            "c1 from ana_13.c1, "
            "reason nchar(32) from ana_13.reason"
            ") using vstb tags (5);"
        )
        tdSql.execute("alter vtable vctb_txn_own alter column reason set ana_13.reason;")
        tdSql.execute("rollback;")
        assert "vctb_txn_own" not in self.db_tables(), (
            f"rollback left the same-txn created table behind: {self.db_tables()}"
        )

        # 2. cascaded column: identical rebind takes the shared no-op path too
        tdSql.execute("begin;")
        tdSql.execute("create vtable vctb_txn_cas (c1 from ana_13.c1) using vstb tags (6);")
        tdSql.execute("alter vtable vctb_txn_cas alter column c1 set ana_13.c1;")
        tdSql.execute("rollback;")
        assert "vctb_txn_cas" not in self.db_tables(), (
            f"rollback left the same-txn created table behind: {self.db_tables()}"
        )

        # 3. control: a real change inside the txn still rolls back cleanly
        tdSql.execute("begin;")
        tdSql.execute(
            "create vtable vctb_txn_chg ("
            "c1 from ana_13.c1, "
            "reason nchar(32) from ana_13.reason"
            ") using vstb tags (7);"
        )
        tdSql.execute("alter vtable vctb_txn_chg alter column reason set ana_14.reason;")
        tdSql.execute("rollback;")
        assert "vctb_txn_chg" not in self.db_tables()

        # 4. control: COMMIT keeps the table and the identical rebind keeps its binding
        tdSql.execute("begin;")
        tdSql.execute(
            "create vtable vctb_txn_keep ("
            "c1 from ana_13.c1, "
            "reason nchar(32) from ana_13.reason"
            ") using vstb tags (8);"
        )
        tdSql.execute("alter vtable vctb_txn_keep alter column reason set ana_13.reason;")
        tdSql.execute("commit;")
        assert "vctb_txn_keep" in self.db_tables()
        self.check_col_ref("vctb_txn_keep", "reason", f"{DB_NAME}.ana_13.reason")

    def test_txn_repeated_identical_rebind_rollback(self):
        """Alter: repeated identical rebinds in one txn still roll back with the table

        The first no-op rebind of a table created in the same txn takes the
        versioned write path, so the second one sees the entry already marked
        PRE_ALTER of this txn and takes the plain no-op skip. ROLLBACK must
        still delete the new version, restore the create mark and chain-delete
        the table. Checked for an own column and for a cascaded column.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: alter,virtual,integration,functional

        Jira: None

        History:
            - 2026-11-04 Yihao Deng Added for the second review round of defect 7155574571

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. own column: two identical no-op rebinds inside the txn, then ROLLBACK
        tdSql.execute("begin;")
        tdSql.execute(
            "create vtable vctb_txn_own2 ("
            "c1 from ana_13.c1, "
            "reason nchar(32) from ana_13.reason"
            ") using vstb tags (10);"
        )
        tdSql.execute("alter vtable vctb_txn_own2 alter column reason set ana_13.reason;")
        tdSql.execute("alter vtable vctb_txn_own2 alter column reason set ana_13.reason;")
        tdSql.execute("rollback;")
        assert "vctb_txn_own2" not in self.db_tables(), (
            f"rollback left the same-txn created table behind: {self.db_tables()}"
        )

        # 2. cascaded column: same double no-op sequence
        tdSql.execute("begin;")
        tdSql.execute("create vtable vctb_txn_cas2 (c1 from ana_13.c1) using vstb tags (11);")
        tdSql.execute("alter vtable vctb_txn_cas2 alter column c1 set ana_13.c1;")
        tdSql.execute("alter vtable vctb_txn_cas2 alter column c1 set ana_13.c1;")
        tdSql.execute("rollback;")
        assert "vctb_txn_cas2" not in self.db_tables(), (
            f"rollback left the same-txn created table behind: {self.db_tables()}"
        )

        # 3. real change first, then a no-op on the PRE_ALTER entry: rollback still restores
        #    the pre-txn binding (the first ALTER provides the version row the undo deletes)
        tdSql.execute("begin;")
        tdSql.execute("create vtable vctb_txn_mix (c1 from ana_13.c1) using vstb tags (12);")
        tdSql.execute("alter vtable vctb_txn_mix alter column c1 set ana_14.c1;")
        tdSql.execute("alter vtable vctb_txn_mix alter column c1 set ana_14.c1;")
        tdSql.execute("rollback;")
        assert "vctb_txn_mix" not in self.db_tables(), (
            f"rollback left the same-txn created table behind: {self.db_tables()}"
        )

    def test_show_create_keeps_unbound_own_column(self):
        """Show: SHOW CREATE keeps an own column whose binding was cleared

        An own column with its binding cleared must still be listed by SHOW CREATE
        with its declared type and no FROM clause: the column lives only in the
        vchild's own area, so omitting the field would drop the column from any
        replayed create. The bare `name TYPE` declaration replays exactly: the
        replayed table keeps the column, its type and its unbound state (schema,
        types and binding status stay identical - the acceptance taosdump relies
        on for its SHOW CREATE VTABLE ... FULL round-trip). An unbound cascaded
        column stays omitable: its schema is inherited from the vstb, so the
        replayed table keeps the column and DESC matches.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual,integration,functional

        Jira: None

        History:
            - 2026-11-04 Yihao Deng Added for the review round of defect 7155574571
            - 2026-11-05 Yihao Deng Bare own-column declarations became replayable (wpan P1)

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. clear the own column binding; the column must survive everywhere
        tdSql.execute("alter vtable vctb_own0 alter column reason set null;")
        self.check_col_ref("vctb_own0", "reason", None)
        self.check_col_visible("vctb_own0", "reason")
        tdSql.query("show create table vctb_own0;")
        create_sql = tdSql.queryResult[0][1]
        assert "`reason` NCHAR(32)" in create_sql, (
            f"unbound own column missing from show create: {create_sql}"
        )
        assert "NCHAR(32) FROM" not in create_sql, f"unbound own column still carries FROM: {create_sql}"

        # 2. the bare declaration replays: schema, type and unbound state are preserved
        replay_sql = create_sql.replace("`vctb_own0`", "`vctb_own0_replay`", 1)
        tdSql.execute(replay_sql)
        self.check_col_visible("vctb_own0_replay", "reason")
        self.check_col_ref("vctb_own0_replay", "reason", None)
        tdSql.query("desc vctb_own0;")
        orig_cols = [row[0] for row in tdSql.queryResult]
        tdSql.query("desc vctb_own0_replay;")
        replay_cols = [row[0] for row in tdSql.queryResult]
        assert orig_cols == replay_cols, f"replay differs: {orig_cols} vs {replay_cols}"

        # 3. an unbound cascaded column is inherited from the vstb: replay works and matches
        tdSql.execute("create vtable vctb_cas0 (c1 from ana_13.c1) using vstb tags (9);")
        tdSql.execute("alter vtable vctb_cas0 alter column c1 set null;")
        self.check_col_ref("vctb_cas0", "c1", None)
        tdSql.query("show create table vctb_cas0;")
        cas_sql = tdSql.queryResult[0][1]
        assert "`c1`" not in cas_sql, f"unbound cascaded column should be inherited, not listed: {cas_sql}"
        tdSql.execute(cas_sql.replace("`vctb_cas0`", "`vctb_cas0_replay`", 1))
        tdSql.query("desc vctb_cas0;")
        orig_cols = [row[0] for row in tdSql.queryResult]
        tdSql.query("desc vctb_cas0_replay;")
        replay_cols = [row[0] for row in tdSql.queryResult]
        assert orig_cols == replay_cols, f"replayed cascaded-only table differs: {orig_cols} vs {replay_cols}"

    @staticmethod
    def col_meta(table, col_name):
        """(col_type, col_source) of one column from ins_columns"""
        tdSql.query(
            f"select col_type, col_source from information_schema.ins_columns "
            f"where db_name='{DB_NAME}' and table_name='{table}' and col_name='{col_name}'"
        )
        tdSql.checkRows(1)
        return tdSql.queryResult[0]

    def test_bare_own_column_declaration_roundtrip(self):
        """Create: a bare `name TYPE` own column is created unbound and replays

        CREATE VTABLE ... USING accepts a bare `name TYPE` declaration next to
        cascaded `name FROM ...` refs (the SHOW CREATE replay form). The column
        behaves exactly like a bound own column after SET NULL (same type, no
        source), can be bound later with ALTER ... SET, and both the plain and
        the FULL SHOW CREATE output replay into an identical table - the
        guarantee taosdump's backup/restore round-trip relies on. An untyped
        `name FROM ...` entry stays invalid for a virtual NORMAL table.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: create,virtual,integration,functional

        Jira: None

        History:
            - 2026-11-05 Yihao Deng Added for the wpan P1 round of defect 7155574571

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. bare declaration mixed with a cascaded ref creates the own column unbound
        tdSql.execute("create vtable vctb_bare (c1 from ana_13.c1, note nchar(32)) using vstb tags (12);")
        self.check_col_visible("vctb_bare", "note")
        assert self.col_meta("vctb_bare", "note") == ("NCHAR(32)", None), self.col_meta("vctb_bare", "note")
        self.check_col_ref("vctb_bare", "c1", f"{DB_NAME}.ana_13.c1")

        # 2. same state as a bound own column after SET NULL
        tdSql.execute("create vtable vctb_clr (c1 from ana_13.c1, note nchar(32) from ana_13.reason) using vstb tags (13);")
        tdSql.execute("alter vtable vctb_clr alter column note set null;")
        assert self.col_meta("vctb_bare", "note") == self.col_meta("vctb_clr", "note"), (
            f"bare-declared and cleared own columns differ: "
            f"{self.col_meta('vctb_bare', 'note')} vs {self.col_meta('vctb_clr', 'note')}"
        )

        # 3. the bare column can be bound afterwards and then serves data
        tdSql.execute("alter vtable vctb_bare alter column note set ana_13.reason;")
        self.check_col_ref("vctb_bare", "note", f"{DB_NAME}.ana_13.reason")
        tdSql.query("select note from vctb_bare;")
        tdSql.checkData(0, 0, "r13")

        # 4. plain SHOW CREATE of the bound form replays identically (schema + binding)
        tdSql.query("show create table vctb_bare;")
        bound_sql = tdSql.queryResult[0][1]
        tdSql.execute(bound_sql.replace("`vctb_bare`", "`vctb_bare_replay`", 1))
        tdSql.query("desc vctb_bare;")
        orig_cols = [row[0] for row in tdSql.queryResult]
        tdSql.query("desc vctb_bare_replay;")
        replay_cols = [row[0] for row in tdSql.queryResult]
        assert orig_cols == replay_cols, f"replay differs: {orig_cols} vs {replay_cols}"
        assert self.col_meta("vctb_bare_replay", "note") == ("NCHAR(32)", f"{DB_NAME}.ana_13.reason")

        # 5. the FULL form (what taosdump reads) replays identically for the unbound state
        tdSql.execute("alter vtable vctb_bare alter column note set null;")
        tdSql.query("show create vtable vctb_bare full;")
        full_sql = tdSql.queryResult[0][1]
        if isinstance(full_sql, bytes):
            full_sql = full_sql.decode()
        assert "`note` NCHAR(32)" in full_sql and "NCHAR(32) FROM" not in full_sql, full_sql
        tdSql.execute(full_sql.replace("`vctb_bare`", "`vctb_bare_full_replay`", 1))
        assert self.col_meta("vctb_bare_full_replay", "note") == ("NCHAR(32)", None)
        tdSql.query("desc vctb_bare;")
        orig_cols = [row[0] for row in tdSql.queryResult]
        tdSql.query("desc vctb_bare_full_replay;")
        replay_cols = [row[0] for row in tdSql.queryResult]
        assert orig_cols == replay_cols, f"full-form replay differs: {orig_cols} vs {replay_cols}"

        # 6. an untyped `name FROM ...` entry stays invalid for a virtual NORMAL table
        tdSql.error("create vtable vntb_bad_ntb (c1 from ana_13.c1);",
                    expectErrInfo="column c1 in a virtual normal table requires a type declaration")

        # 7. partial references never enter the no-binding skip paths: a lone column name
        #    or an empty segment is rejected by the parser before any validation runs
        tdSql.error("create vtable vctb_bad_seg (note nchar(32) from secret);", expectedErrno=0x2600)
        tdSql.error("create vtable vctb_bad_dots (c1 from ana_13..c1);", expectedErrno=0x2600)

        # 8. a condition never rides on a bare declaration: WHERE without FROM is a syntax
        #    error, and a filter on an internal reference is rejected explicitly - neither
        #    can reach the bare-column skip with a condition attached
        tdSql.error("create vtable vctb_bare_where (note nchar(32) where note > 'a') using vstb tags (15);",
                    expectedErrno=0x2600)
        tdSql.error("create vtable vctb_ref_where (note nchar(32) from ana_13.reason where note > 'a') "
                    "using vstb tags (16);", expectedErrno=0x6213)

        # 9. PRIMARY KEY is a normal-table column option: an own column of a vchild has no
        #    key semantics, the combination is rejected explicitly
        tdSql.error("create vtable vctb_own_pk (note nchar(32) primary key) using vstb tags (17);",
                    expectErrInfo="column note cannot be a primary key in a virtual child table")

    def test_bare_own_column_decimal_roundtrip(self):
        """Create: a bare DECIMAL own column keeps its typeMod across SHOW CREATE

        A bare declaration with a parameterized type (DECIMAL(precision,scale))
        exercises the typeMod chain end to end: the create request carries
        COL_HAS_TYPE_MOD + typeMod, the vnode persists it in the own-column
        ext schemas and SHOW CREATE re-renders DECIMAL(p,s) through
        formatOwnSchemaType. The replayed table must show the identical type.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: create,virtual,integration,functional

        Jira: None

        History:
            - 2026-11-05 Yihao Deng Added for the review round of defect 7155574571

        """
        tdSql.execute(f"use {DB_NAME};")

        # 1. bare DECIMAL declaration: unbound, exact type in ins_columns
        tdSql.execute("create vtable vctb_dec (amount decimal(10,2)) using vstb tags (14);")
        self.check_col_visible("vctb_dec", "amount")
        assert self.col_meta("vctb_dec", "amount") == ("DECIMAL(10,2)", None), self.col_meta("vctb_dec", "amount")

        # 2. SHOW CREATE renders the declared precision/scale, no FROM
        tdSql.query("show create table vctb_dec;")
        dec_sql = tdSql.queryResult[0][1]
        assert "`amount` DECIMAL(10,2)" in dec_sql, dec_sql
        assert "DECIMAL(10,2) FROM" not in dec_sql, dec_sql

        # 3. the replay keeps schema, type and the unbound state
        tdSql.execute(dec_sql.replace("`vctb_dec`", "`vctb_dec_replay`", 1))
        assert self.col_meta("vctb_dec_replay", "amount") == ("DECIMAL(10,2)", None)
        tdSql.query("desc vctb_dec;")
        orig_cols = [row[0] for row in tdSql.queryResult]
        tdSql.query("desc vctb_dec_replay;")
        replay_cols = [row[0] for row in tdSql.queryResult]
        assert orig_cols == replay_cols, f"decimal replay differs: {orig_cols} vs {replay_cols}"

        # 4. binding the decimal column afterwards works and shows through
        tdSql.execute("create table ana_dec(ts timestamp, dec_val decimal(10,2));")
        tdSql.execute("insert into ana_dec values(now, 12.34);")
        tdSql.execute("alter vtable vctb_dec alter column amount set ana_dec.dec_val;")
        self.check_col_ref("vctb_dec", "amount", f"{DB_NAME}.ana_dec.dec_val")
        tdSql.query("select amount from vctb_dec;")
        tdSql.checkData(0, 0, 12.34)
