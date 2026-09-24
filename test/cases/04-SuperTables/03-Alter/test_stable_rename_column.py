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

import time

import taos

from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck
from new_test_framework.utils.sql import TDSql


class TestStableRenameColumn:
    """ALTER STABLE / virtual STABLE RENAME COLUMN and RENAME TAG.

    Covers the newly-enabled super-table (and virtual super-table) column
    rename, the pre-existing tag rename, and the dependency checks that decide
    whether a rename is allowed:

      - privilege (column-level and row-level, for both users and roles) is
        RESTRICT: a referenced column cannot be renamed;
      - a tag referenced by a stream (partition/rollup) binds by colId, same
        as any other column, so renaming it is deliberately NOT restricted
        (DROP TAG on that same tag remains restricted via COL_REF_BY_STM,
        since dropping actually removes the colId the stream is bound to;
        see stream_rollup_basic.py::check_stable_rollup_tag_frozen);
      - view / topic / plain stream columns bind by colId (or are weak
        references) and are deliberately NOT checked.

    Normal-table rename lives on the vnode path and never enters the mnode, so
    its privilege RESTRICT is reached through a synchronous mnode round trip
    (TDMT_MND_CHECK_COL_REF_BY_PRIV) instead of an inline scan. Those cases -
    plain and virtual normal table, RENAME COLUMN and RENAME TAG, user and role
    grants, long table names - live in test_vtable_normal_tags.py, method
    test_rename_tag_priv_restrict / test_rename_column_priv_restrict. The
    normal-table assertions kept here are the ones that must agree with the
    stable path: timestamp column and composite primary key handling, plus the
    stream output table in its normal-table form.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = "db_rename_col"

    def prepare_db(self):
        tdSql.execute(f"drop database if exists {self.dbname}")
        tdSql.execute(f"create database {self.dbname} keep 36500d")
        tdSql.execute(f"use {self.dbname}")

    def check_col_exists(self, table, colname, exists=True):
        tdSql.query(f"describe {self.dbname}.{table}")
        names = [row[0] for row in tdSql.queryResult]
        if exists:
            assert colname in names, f"{colname} expected in {table}, got {names}"
        else:
            assert colname not in names, f"{colname} unexpected in {table}, got {names}"

    def check_composite_key(self, table, colname):
        """Assert DESCRIBE still marks colname as the composite primary key.

        The note lives in the 4th describe column and is rendered from the COL_IS_KEY flag,
        so it is the user-visible proof that a rename did not disturb key semantics.
        """
        tdSql.query(f"describe {self.dbname}.{table}")
        rows = [row for row in tdSql.queryResult if row[0] == colname]
        assert len(rows) == 1, f"expect exactly one {table}.{colname} row, got {tdSql.queryResult}"
        assert rows[0][3] == "COMPOSITE KEY", f"{table}.{colname} lost its key note: {rows[0]}"

    def wait_table_exists(self, table, timeout=60):
        """Wait for a table to appear, for tables a stream creates on its own schedule.

        Uses describe rather than an information_schema lookup so it works for both a stable and
        a normal table without the caller having to know which one the stream produced.
        """
        for _ in range(timeout):
            # exit=False makes a missing table return False instead of raising and logging an error
            if tdSql.query(f"describe {self.dbname}.{table}", queryTimes=1, exit=False) is not False:
                return
            time.sleep(1)
        assert False, f"table {self.dbname}.{table} did not appear within {timeout}s"

    def wait_table_exists_while_writing(self, table, feed_table, timeout=120):
        """Wait for a stream output table, writing fresh rows on every poll.

        A stream skips rows timestamped before its own start time, so a single up-front insert can
        be swallowed entirely and the output table never gets created. Writing on each iteration
        guarantees some batch lands inside a window the stream actually computes.
        """
        for _ in range(timeout):
            if tdSql.query(f"describe {self.dbname}.{table}", queryTimes=1, exit=False) is not False:
                return
            tdSql.execute(f"insert into {feed_table} values(now, 1)")
            time.sleep(1)
        assert False, f"table {self.dbname}.{table} did not appear within {timeout}s"

    def get_col_id(self, table, colname):
        """Read the stable colId straight from information_schema."""
        tdSql.query(
            f"select col_id from information_schema.ins_columns "
            f"where db_name='{self.dbname}' and table_name='{table}' and col_name='{colname}'"
        )
        assert tdSql.getRows() == 1, f"expect exactly one row for {table}.{colname}, got {tdSql.queryResult}"
        return tdSql.getData(0, 0)

    def rename_basic_stable(self):
        tdLog.info("========== basic super-table column & tag rename")
        tdSql.execute(
            f"create table {self.dbname}.meters "
            f"(ts timestamp, current float, voltage int, phase float) "
            f"tags (location binary(64), groupId int)"
        )
        tdSql.execute(f"create table {self.dbname}.d0 using {self.dbname}.meters tags('beijing', 1)")
        tdSql.execute(f"create table {self.dbname}.d1 using {self.dbname}.meters tags('shanghai', 2)")
        tdSql.execute(f"insert into {self.dbname}.d0 values(now, 1.1, 220, 0.5)")
        tdSql.execute(f"insert into {self.dbname}.d1 values(now, 2.2, 221, 0.6)")

        # TC-05: colId must survive the rename, since data and in-flight queries bind by colId
        old_col_id = self.get_col_id("meters", "voltage")

        # TC-03: rename a normal column on the super table (new capability)
        tdSql.execute(f"alter stable {self.dbname}.meters rename column voltage volt")
        self.check_col_exists("meters", "volt", True)
        self.check_col_exists("meters", "voltage", False)
        # every child table sees the new name too (schema is owned by the stable)
        self.check_col_exists("d0", "volt", True)
        self.check_col_exists("d1", "volt", True)
        # historical data written before the rename is still readable under the new name
        tdSql.query(f"select volt from {self.dbname}.d0")
        tdSql.checkData(0, 0, 220)
        # old name no longer resolvable
        tdSql.error(f"select voltage from {self.dbname}.d0")

        # TC-05 assertion: same colId before and after
        new_col_id = self.get_col_id("meters", "volt")
        assert old_col_id == new_col_id, f"colId changed by rename: {old_col_id} -> {new_col_id}"

        # rename a tag (pre-existing capability, both syntaxes)
        tdSql.execute(f"alter stable {self.dbname}.meters rename tag groupId gid")
        self.check_col_exists("meters", "gid", True)
        tdSql.execute(f"alter table {self.dbname}.meters rename column current cur")
        self.check_col_exists("meters", "cur", True)

        # tag values are untouched and queryable under the new tag name
        tdSql.query(f"select count(*) from {self.dbname}.meters where gid = 1")
        tdSql.checkData(0, 0, 1)

    def rename_back_to_original(self):
        tdLog.info("========== TC-25: rename a column and then rename it back")
        col_id_before = self.get_col_id("meters", "volt")
        tdSql.execute(f"alter stable {self.dbname}.meters rename column volt voltage")
        self.check_col_exists("meters", "voltage", True)
        self.check_col_exists("meters", "volt", False)
        # colId is stable across both renames, so the original data is still addressable
        assert self.get_col_id("meters", "voltage") == col_id_before
        tdSql.query(f"select voltage from {self.dbname}.d0")
        tdSql.checkData(0, 0, 220)
        # restore the name the later cases expect
        tdSql.execute(f"alter stable {self.dbname}.meters rename column voltage volt")

    def rename_error_cases(self):
        tdLog.info("========== rename error cases")
        # TC-06: old column does not exist
        tdSql.error(
            f"alter stable {self.dbname}.meters rename column not_exist x",
            expectErrInfo="Column does not exist",
            fullMatched=False,
        )
        # TC-07: new name duplicates an existing column
        tdSql.error(
            f"alter stable {self.dbname}.meters rename column volt cur",
            expectErrInfo="Column already exists",
            fullMatched=False,
        )
        # TC-07: new name duplicates an existing *tag* -> reported with the same code as above,
        # so callers only ever handle one "name already in use" error
        tdSql.error(
            f"alter stable {self.dbname}.meters rename column volt location",
            expectErrInfo="Column already exists",
            fullMatched=False,
        )
        # TC-10: child table cannot rename a column (schema belongs to the stable)
        tdSql.error(
            f"alter table {self.dbname}.d0 rename column volt v2",
            expectErrInfo="Invalid alter table statement",
            fullMatched=False,
        )
        # renaming a tag that does not exist
        tdSql.error(f"alter stable {self.dbname}.meters rename tag no_such_tag x")

    def rename_invalid_names(self):
        tdLog.info("========== TC-11: illegal new column names")
        too_long = "c" * 200
        tdSql.error(f"alter stable {self.dbname}.meters rename column volt {too_long}")
        # a reserved keyword is not a usable bare identifier
        tdSql.error(f"alter stable {self.dbname}.meters rename column volt select")
        # empty / missing new name is a syntax error
        tdSql.error(f"alter stable {self.dbname}.meters rename column volt")
        tdSql.error(f"alter stable {self.dbname}.meters rename column volt ''")

    def rename_timestamp_column(self):
        """TC-08b: the timestamp column CAN be renamed, on both table kinds.

        It carries no COL_IS_KEY flag, so it is not a composite primary key. The
        runtime locates it by PRIMARYKEY_TIMESTAMP_COL_ID rather than by name and
        the _wstart / _c0 / _rowts pseudo columns do not depend on its spelling,
        so the rename is as safe as renaming any other column. The normal-table
        path has always allowed it; the stable path matches that.
        """
        tdLog.info("========== TC-08b: timestamp column rename is allowed")
        tdSql.execute(f"create table {self.dbname}.tsstb (ts timestamp, tc1 int) tags(tt int)")
        tdSql.execute(f"create table {self.dbname}.ts0 using {self.dbname}.tsstb tags(1)")
        tdSql.execute(f"insert into {self.dbname}.ts0 values('2026-01-01 00:00:00', 7)")
        ts_col_id = self.get_col_id("tsstb", "ts")

        tdSql.execute(f"alter stable {self.dbname}.tsstb rename column ts ts_renamed")
        self.check_col_exists("tsstb", "ts_renamed", True)
        self.check_col_exists("tsstb", "ts", False)
        self.check_col_exists("ts0", "ts_renamed", True)
        # colId is unchanged, so the pre-rename row is still addressable
        assert self.get_col_id("tsstb", "ts_renamed") == ts_col_id
        tdSql.query(f"select tc1 from {self.dbname}.ts0 where ts_renamed = '2026-01-01 00:00:00'")
        tdSql.checkData(0, 0, 7)
        # pseudo columns resolve by colId and are unaffected by the new name
        tdSql.query(f"select _wstart, count(*) from {self.dbname}.tsstb interval(1d)")
        tdSql.checkData(0, 1, 1)
        # and it renames back
        tdSql.execute(f"alter stable {self.dbname}.tsstb rename column ts_renamed ts")
        self.check_col_exists("tsstb", "ts", True)

        # normal table: same behaviour, which is the pre-existing 3.0 semantics
        tdSql.execute(f"create table {self.dbname}.tsntb (ts timestamp, nc1 int)")
        tdSql.execute(f"insert into {self.dbname}.tsntb values('2026-01-01 00:00:00', 9)")
        tdSql.execute(f"alter table {self.dbname}.tsntb rename column ts ts_renamed")
        self.check_col_exists("tsntb", "ts_renamed", True)
        tdSql.query(f"select nc1 from {self.dbname}.tsntb where ts_renamed = '2026-01-01 00:00:00'")
        tdSql.checkData(0, 0, 9)

    def rename_with_composite_primary_key(self):
        """TC-08: the composite primary key column CAN be renamed, on both table kinds.

        Being a key is carried by the COL_IS_KEY flag and the colId, never by the column
        name: write encoding, tsdb read/cache, the planner, the stream reader and tmq scan
        all derive it from flags, and DESCRIBE renders "COMPOSITE KEY" from flags too. So a
        rename leaves key semantics intact, exactly like the timestamp column. DROP COLUMN
        and MODIFY COLUMN remain rejected - they destroy the key or change its width.
        """
        tdLog.info("========== TC-08: composite primary key column rename is allowed")
        tdSql.execute(
            f"create table {self.dbname}.pkstb "
            f"(ts timestamp, id int primary key, val int) tags(t1 int)"
        )
        tdSql.execute(f"create table {self.dbname}.pk0 using {self.dbname}.pkstb tags(1)")
        tdSql.execute(f"insert into {self.dbname}.pk0 values('2026-01-01 00:00:00', 5, 50)")
        pk_col_id = self.get_col_id("pkstb", "id")

        tdSql.execute(f"alter stable {self.dbname}.pkstb rename column id id2")
        self.check_col_exists("pkstb", "id2", True)
        self.check_col_exists("pkstb", "id", False)
        self.check_col_exists("pk0", "id2", True)
        # colId is unchanged, so the pre-rename row is still addressable by the new name
        assert self.get_col_id("pkstb", "id2") == pk_col_id
        tdSql.query(f"select val from {self.dbname}.pk0 where id2 = 5")
        tdSql.checkData(0, 0, 50)
        # the column is still the composite primary key: DESCRIBE still marks it, and
        # inserting a duplicate (ts, pk) pair still collapses onto the same row
        self.check_composite_key("pkstb", "id2")
        tdSql.execute(f"insert into {self.dbname}.pk0 values('2026-01-01 00:00:00', 5, 51)")
        tdSql.query(f"select val from {self.dbname}.pk0 where id2 = 5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 51)
        # and a different pk value at the same ts is still a distinct row
        tdSql.execute(f"insert into {self.dbname}.pk0 values('2026-01-01 00:00:00', 6, 60)")
        tdSql.query(f"select val from {self.dbname}.pk0 order by id2")
        tdSql.checkRows(2)

        # DROP / MODIFY on the renamed pk column are still rejected
        tdSql.error(
            f"alter stable {self.dbname}.pkstb drop column id2",
            expectErrInfo="composite primary key column cannot be added, modified, or dropped",
            fullMatched=False,
        )
        # a plain column on the same table renames fine
        tdSql.execute(f"alter stable {self.dbname}.pkstb rename column val val_ok")
        self.check_col_exists("pkstb", "val_ok", True)
        # so does the timestamp column of a table that has a composite pk
        tdSql.execute(f"alter stable {self.dbname}.pkstb rename column ts ts_ok")
        self.check_col_exists("pkstb", "ts_ok", True)

        # normal table with a composite pk: same behaviour on the vnode path
        tdSql.execute(f"create table {self.dbname}.pkntb (ts timestamp, id int primary key, val int)")
        tdSql.execute(f"insert into {self.dbname}.pkntb values('2026-01-01 00:00:00', 5, 50)")
        tdSql.execute(f"alter table {self.dbname}.pkntb rename column id id2")
        self.check_col_exists("pkntb", "id2", True)
        self.check_composite_key("pkntb", "id2")
        tdSql.query(f"select val from {self.dbname}.pkntb where id2 = 5")
        tdSql.checkData(0, 0, 50)
        tdSql.error(
            f"alter table {self.dbname}.pkntb drop column id2",
            expectErrInfo="composite primary key column cannot be added, modified, or dropped",
            fullMatched=False,
        )
        tdSql.execute(f"alter table {self.dbname}.pkntb rename column val val_ok")
        self.check_col_exists("pkntb", "val_ok", True)

    def rename_virtual_stable(self):
        tdLog.info("========== virtual super-table column & tag rename")
        tdSql.execute(f"create table {self.dbname}.src (ts timestamp, s1 int, s2 int)")
        tdSql.execute(
            f"create stable {self.dbname}.vstb (ts timestamp, vc1 int, vc2 int) "
            f"tags (vt int) virtual 1"
        )
        tdSql.execute(
            f"create vtable {self.dbname}.vc0 ({self.dbname}.src.s1, {self.dbname}.src.s2) "
            f"using {self.dbname}.vstb tags(1)"
        )
        # rename a column on the virtual super table
        tdSql.execute(f"alter stable {self.dbname}.vstb rename column vc1 vc1_new")
        self.check_col_exists("vstb", "vc1_new", True)
        self.check_col_exists("vc0", "vc1_new", True)
        # rename a tag on the virtual super table
        tdSql.execute(f"alter stable {self.dbname}.vstb rename tag vt vt_new")
        self.check_col_exists("vstb", "vt_new", True)

    def safe_with_virtual_colref(self):
        """TC-19b: a virtual table's colRef is NOT checked, so the rename succeeds.

        SColRef binds the source column by name (refColName, there is no
        refColId) and lives in the referencing vnode's metadata with no reverse
        index, so no single node can enumerate the referrers. The rename is
        therefore allowed and the virtual table's column goes dangling - a
        documented known limitation, not an oversight.
        """
        tdLog.info("========== virtual colRef is not checked: rename succeeds, reference dangles")
        tdSql.execute(f"create table {self.dbname}.vsrc2 (ts timestamp, r1 int, r2 int)")
        tdSql.execute(f"insert into {self.dbname}.vsrc2 values(now, 10, 20)")
        tdSql.execute(
            f"create vtable {self.dbname}.vref0 "
            f"(ts timestamp, rc1 int from {self.dbname}.vsrc2.r1, "
            f"rc2 int from {self.dbname}.vsrc2.r2)"
        )
        tdSql.query(f"select rc1 from {self.dbname}.vref0")
        tdSql.checkData(0, 0, 10)

        # renaming the referenced source column is allowed (no detection)
        tdSql.execute(f"alter table {self.dbname}.vsrc2 rename column r1 r1_renamed")
        self.check_col_exists("vsrc2", "r1_renamed", True)
        # the colRef still points at the old name, so reading that column no longer resolves;
        # accept either an error or a NULL/absent value depending on how the vnode reports it
        try:
            tdSql.query(f"select rc1 from {self.dbname}.vref0")
            tdLog.info(f"vref0.rc1 after source rename -> {tdSql.queryResult}")
        except Exception as e:
            tdLog.info(f"vref0.rc1 read failed after source rename, as expected: {e}")
        # the unrelated column of the same virtual table is untouched
        tdSql.query(f"select rc2 from {self.dbname}.vref0")
        tdSql.checkData(0, 0, 20)

        # same story when the source is a super table read through a child table
        tdSql.execute(f"create table {self.dbname}.stb_ref (ts timestamp, sr1 int, sr2 int) tags(st int)")
        tdSql.execute(f"create table {self.dbname}.ctb_ref using {self.dbname}.stb_ref tags(1)")
        tdSql.execute(f"insert into {self.dbname}.ctb_ref values(now, 7, 8)")
        tdSql.execute(
            f"create vtable {self.dbname}.vref1 "
            f"(ts timestamp, rc1 int from {self.dbname}.ctb_ref.sr1)"
        )
        tdSql.execute(f"alter stable {self.dbname}.stb_ref rename column sr1 sr1_renamed")
        self.check_col_exists("stb_ref", "sr1_renamed", True)

    def restrict_virtual_stable_with_children(self):
        """A virtual super table that has child VSTs cannot be altered at all.

        This is a pre-existing VST inheritance restriction rather than something
        this feature introduces, but rename goes down the same ALTER path, so it
        inherits the rejection and that needs pinning down.
        """
        tdLog.info("========== VST with child VSTs rejects rename")
        tdSql.execute(
            f"create stable {self.dbname}.pvstb (ts timestamp, pc1 int, pc2 int) "
            f"tags(pt int) virtual 1"
        )
        # a VST that inherits from pvstb makes pvstb a parent; VST inheritance is an enterprise
        # feature, so skip cleanly where it is unavailable
        try:
            tdSql.execute(
                f"create stable {self.dbname}.cvstb (ts timestamp, cc1 int) "
                f"tags(ct int) base on {self.dbname}.pvstb virtual 1"
            )
        except Exception as e:
            tdLog.info(f"VST inheritance unsupported here, skip child-VST case: {e}")
            return

        tdSql.error(
            f"alter stable {self.dbname}.pvstb rename column pc1 pc1_x",
            expectErrInfo="child VSTs",
            fullMatched=False,
        )
        tdSql.error(
            f"alter stable {self.dbname}.pvstb rename tag pt pt_x",
            expectErrInfo="child VSTs",
            fullMatched=False,
        )

    def safe_as_db_owner(self):
        """A db owner renaming its own table must not be blocked by its own ownership.

        Ownership grants full DDL over the object, so the RESTRICT scan must not
        mistake the owner's implicit rights for a column reference. The second
        half checks the converse: once the owner grants someone a column-level
        privilege, the owner is subject to RESTRICT like anybody else - ownership
        is not an escape hatch.
        """
        tdLog.info("========== db owner can rename its own columns, but RESTRICT still applies")
        tdSql.execute("drop user if exists u_owner")
        tdSql.execute("create user u_owner pass 'taosdata1@'")
        tdSql.execute("grant create database to u_owner")
        tdSql.execute("drop user if exists u_grantee")
        tdSql.execute("create user u_grantee pass 'taosdata1@'")

        owner_db = "db_rc_owner"
        try:
            conn = taos.connect(user="u_owner", password="taosdata1@")
        except Exception as e:
            tdLog.info(f"cannot connect as u_owner, skip owner case: {e}")
            tdSql.execute("drop user u_owner")
            tdSql.execute("drop user u_grantee")
            return

        # Pre-clean as root: u_owner only holds CREATE DATABASE, and for a db that does not exist
        # the mnode runs the DROP DB privilege check by name *before* honouring IF EXISTS, so
        # "drop database if exists" as u_owner would fail with "Permission denied or target object
        # not exist" instead of succeeding silently. The owner can drop the db once it owns it.
        tdSql.execute(f"drop database if exists {owner_db}")

        ownerSql = TDSql()
        ownerSql.init(conn.cursor())
        try:
            ownerSql.execute(f"create database {owner_db} keep 36500d")
            ownerSql.execute(f"use {owner_db}")
            ownerSql.execute("create table stb_own (ts timestamp, oc1 int, oc2 int) tags(ot int)")
            ownerSql.execute("create table ctb_own using stb_own tags(1)")
            ownerSql.execute("insert into ctb_own values(now, 1, 2)")

            # owner renames its own column and tag: allowed
            ownerSql.execute("alter stable stb_own rename column oc1 oc1_ok")
            ownerSql.execute("alter stable stb_own rename tag ot ot_ok")
            ownerSql.query("select oc1_ok from ctb_own")
            ownerSql.checkData(0, 0, 1)

            # A column-level privilege on the owned table makes the owner hit RESTRICT too.
            # GRANT/REVOKE require the SYSSEC system role (PRIV_GRANT_PRIVILEGE in tpriv.c), which
            # owning a database does not confer, so root issues the grant; the point of the case is
            # that RESTRICT applies to the *owner's* rename regardless of who granted.
            tdSql.execute(f"grant select(ts,oc1_ok) on table {owner_db}.stb_own to u_grantee")
            ownerSql.error(
                "alter stable stb_own rename column oc1_ok oc1_z",
                expectErrInfo="referenced by user privilege",
                fullMatched=False,
            )
            # an unreferenced column on the same owned table is still fine
            ownerSql.execute("alter stable stb_own rename column oc2 oc2_ok")
            tdSql.execute(f"revoke select(ts,oc1_ok) on table {owner_db}.stb_own from u_grantee")
            ownerSql.execute("alter stable stb_own rename column oc1_ok oc1_final")
        finally:
            ownerSql.close()
            tdSql.execute(f"drop database if exists {owner_db}")
            tdSql.execute("drop user u_owner")
            tdSql.execute("drop user u_grantee")

    def restrict_by_tsma(self):
        tdLog.info("========== TC-09: a column used by a TSMA cannot be renamed")
        # a TSMA is backed by a stream, so it needs an snode to be created
        tdStream.ensureSnode(1)
        tdSql.execute(f"create table {self.dbname}.tstb (ts timestamp, tc1 int, tc2 int) tags(tt int)")
        tdSql.execute(
            f"create tsma tsma_rc on {self.dbname}.tstb function(min(tc1), max(tc1)) interval(1m)"
        )
        # the stb path is validated in mnode, which reports the column as used by a TSMA
        tdSql.error(
            f"alter stable {self.dbname}.tstb rename column tc1 tc1_x",
            expectErrInfo="tsma",
            fullMatched=False,
        )
        # dropping the TSMA releases the restriction
        tdSql.execute(f"drop tsma {self.dbname}.tsma_rc")
        tdSql.execute(f"alter stable {self.dbname}.tstb rename column tc1 tc1_ok")
        self.check_col_exists("tstb", "tc1_ok", True)

    def restrict_by_privilege(self):
        tdLog.info("========== RESTRICT: column-level / tag-cond privilege")
        tdSql.execute(f"create table {self.dbname}.pstb (ts timestamp, pc1 int, pc2 int) tags(pt int)")
        # Use two independent users: a table-level SELECT policy is per (user,table), so a
        # column-list grant and a row-level tag-cond grant on the same table would collide.
        tdSql.execute("drop user if exists u_col")
        tdSql.execute("drop user if exists u_tag")
        tdSql.execute("create user u_col pass 'taosdata1@'")
        tdSql.execute("create user u_tag pass 'taosdata1@'")

        # column-level privilege references pc1 -> rename must be rejected. A user grant reports
        # the user-specific code, distinct from the role one asserted in the role case below.
        tdSql.execute(f"grant select(ts,pc1) on table {self.dbname}.pstb to u_col")
        tdSql.error(
            f"alter stable {self.dbname}.pstb rename column pc1 pc1_x",
            expectErrInfo="referenced by user privilege",
            fullMatched=False,
        )
        # a column NOT covered by any privilege can still be renamed
        tdSql.execute(f"alter stable {self.dbname}.pstb rename column pc2 pc2_x")
        self.check_col_exists("pstb", "pc2_x", True)

        # tag referenced by a row-level (tag cond) privilege -> rename rejected
        tdSql.execute(f"grant select on table {self.dbname}.pstb with pt = 1 to u_tag")
        tdSql.error(
            f"alter stable {self.dbname}.pstb rename tag pt pt_x",
            expectErrInfo="referenced by user privilege",
            fullMatched=False,
        )

        # TC-18: after revoking, rename succeeds (RESTRICT is releasable)
        tdSql.execute(f"revoke select(ts,pc1) on table {self.dbname}.pstb from u_col")
        tdSql.execute(f"revoke select on table {self.dbname}.pstb from u_tag")
        tdSql.execute(f"alter stable {self.dbname}.pstb rename column pc1 pc1_ok")
        self.check_col_exists("pstb", "pc1_ok", True)
        tdSql.execute(f"alter stable {self.dbname}.pstb rename tag pt pt_ok")
        self.check_col_exists("pstb", "pt_ok", True)
        tdSql.execute("drop user u_col")
        tdSql.execute("drop user u_tag")

    def restrict_by_role_privilege(self):
        """TC-14b: a grant made through a ROLE must block the rename too.

        Roles carry the same four table-policy hashes as users and the privilege
        check treats them identically, so the RESTRICT scan has to cover
        SDB_ROLE as well - otherwise a column-level grant made via a role would
        bypass it entirely and leave authorization silently out of sync.
        """
        tdLog.info("========== RESTRICT: column-level privilege granted via a role")
        tdSql.execute(f"create table {self.dbname}.rstb (ts timestamp, rc1 int, rc2 int) tags(rt int)")
        try:
            tdSql.execute("drop role if exists r_col")
            tdSql.execute("create role r_col")
        except Exception as e:
            # roles are an enterprise feature; skip cleanly on community builds
            tdLog.info(f"roles unsupported in this build, skip role RESTRICT case: {e}")
            return

        tdSql.execute(f"grant select(ts,rc1) on table {self.dbname}.rstb to r_col")
        # a role grant reports the role-specific code, so the message tells the operator which
        # kind of principal to go fix (user vs role); the two codes must not be interchangeable
        tdSql.error(
            f"alter stable {self.dbname}.rstb rename column rc1 rc1_x",
            expectErrInfo="referenced by role privilege",
            fullMatched=False,
        )
        # an unreferenced column on the same table is unaffected
        tdSql.execute(f"alter stable {self.dbname}.rstb rename column rc2 rc2_ok")
        self.check_col_exists("rstb", "rc2_ok", True)

        tdSql.execute(f"revoke select(ts,rc1) on table {self.dbname}.rstb from r_col")
        tdSql.execute(f"alter stable {self.dbname}.rstb rename column rc1 rc1_ok")
        self.check_col_exists("rstb", "rc1_ok", True)
        tdSql.execute("drop role r_col")

    def restrict_by_tag_cond_is_conservative(self):
        """The tag-cond check matches by substring, so it can over-reject.

        SPrivTblPolicy.cond is the condition AST serialized to JSON text (e.g.
        {"ColName":"pt","TableName":"fpstb","ColId":3,...}). The check looks for
        the column name as a substring of that text, which never misses a real
        reference but does reject some columns that are not actually referenced:

          - a name that is a substring of the referenced tag name ("p" inside "pt")
          - a name that is a substring of any other JSON *value*, e.g. the table
            name carried by the condition ("fp" inside "fpstb")
          - a name that collides with a JSON structural key ("ColId", "ColName");
            the match is case-sensitive, so lowercase "id"/"name" do NOT collide

        This is a documented known limitation (FS section 10): over-rejecting is
        acceptable, silently letting authorization desynchronize is not. The case
        pins the behaviour so a future precise implementation is a deliberate
        change rather than an accident.
        """
        tdLog.info("========== tag-cond RESTRICT is conservative: known false rejection")
        tdSql.execute(
            f"create table {self.dbname}.fpstb (ts timestamp, p int, fp int, plain int, `ColId` int) tags(pt int)"
        )
        tdSql.execute("drop user if exists u_fp")
        tdSql.execute("create user u_fp pass 'taosdata1@'")
        tdSql.execute(f"grant select on table {self.dbname}.fpstb with pt = 1 to u_fp")

        # none of these three columns is referenced by the condition (which only uses pt), but each
        # is a substring of the serialized condition, so the rename is rejected.
        for col in ["p", "fp", "ColId"]:
            tdSql.error(
                f"alter stable {self.dbname}.fpstb rename column `{col}` {col}_x",
                expectErrInfo="referenced by user privilege",
                fullMatched=False,
            )
        # a name that shares nothing with the serialized condition is renamed normally
        tdSql.execute(f"alter stable {self.dbname}.fpstb rename column plain plain_ok")
        self.check_col_exists("fpstb", "plain_ok", True)
        # the tag actually named in the condition is of course rejected as well
        tdSql.error(
            f"alter stable {self.dbname}.fpstb rename tag pt pt_x",
            expectErrInfo="referenced by user privilege",
            fullMatched=False,
        )

        # revoking the row-level grant releases all of them
        tdSql.execute(f"revoke select on table {self.dbname}.fpstb from u_fp")
        for col in ["p", "fp", "ColId"]:
            # quote the new name as well: an unquoted identifier is folded to lower case
            tdSql.execute(f"alter stable {self.dbname}.fpstb rename column `{col}` `{col}_ok`")
            self.check_col_exists("fpstb", f"{col}_ok", True)
        tdSql.execute("drop user u_fp")

    def safe_with_table_and_db_privilege(self):
        """A grant with no column list is not a column reference, so it must not block.

        A table-level SELECT (cols == NULL means "all columns") and a database-level
        grant both authorize the column implicitly, by object rather than by name,
        so a rename cannot desynchronize them. Only an explicit column list or a
        row-level condition pins a name down. Getting this wrong would make every
        rename on any granted table fail.
        """
        tdLog.info("========== table-level / db-level grants are not column references")
        tdSql.execute(f"create table {self.dbname}.tbpriv (ts timestamp, tc1 int) tags(tt int)")
        tdSql.execute(f"create table {self.dbname}.dbpriv (ts timestamp, dc1 int) tags(dt int)")
        tdSql.execute("drop user if exists u_tb")
        tdSql.execute("create user u_tb pass 'taosdata1@'")

        # table-level SELECT: no column list, no condition
        tdSql.execute(f"grant select on table {self.dbname}.tbpriv to u_tb")
        tdSql.execute(f"alter stable {self.dbname}.tbpriv rename column tc1 tc1_ok")
        self.check_col_exists("tbpriv", "tc1_ok", True)
        tdSql.execute(f"alter stable {self.dbname}.tbpriv rename tag tt tt_ok")
        self.check_col_exists("tbpriv", "tt_ok", True)

        # database-level grant: likewise carries no column reference
        tdSql.execute(f"grant read on database {self.dbname} to u_tb")
        tdSql.execute(f"alter stable {self.dbname}.dbpriv rename column dc1 dc1_ok")
        self.check_col_exists("dbpriv", "dc1_ok", True)

        tdSql.execute(f"revoke read on database {self.dbname} from u_tb")
        tdSql.execute(f"revoke select on table {self.dbname}.tbpriv from u_tb")
        tdSql.execute("drop user u_tb")

    def safe_with_system_role(self):
        """Built-in system roles must not be scanned by the RESTRICT check.

        System roles (SRoleObj.sys) are shipped by the server rather than granted
        by an operator, and their policies are not something a user can revoke.
        Including them would make renames fail with an error whose remedy the
        operator cannot perform, so the scan skips them. Renaming on a plain
        table with no user/role grant of its own must simply succeed.
        """
        tdLog.info("========== system roles are skipped by the RESTRICT scan")
        tdSql.execute(f"create table {self.dbname}.sysstb (ts timestamp, sc1 int) tags(st int)")
        try:
            tdSql.query("select name, role_type from information_schema.ins_roles")
        except Exception as e:
            tdLog.info(f"roles unsupported in this build, skip system-role case: {e}")
            return
        sys_roles = [r[0] for r in tdSql.queryResult or [] if str(r[1]).lower() == "system"]
        tdLog.info(f"system roles present: {sys_roles}")

        # Renaming must succeed even though system roles exist and carry broad built-in policies.
        tdSql.execute(f"alter stable {self.dbname}.sysstb rename column sc1 sc1_ok")
        self.check_col_exists("sysstb", "sc1_ok", True)
        tdSql.execute(f"alter stable {self.dbname}.sysstb rename tag st st_ok")
        self.check_col_exists("sysstb", "st_ok", True)

    def restrict_with_long_table_name(self):
        """TC-14c: the RESTRICT check must still fire for a very long table name.

        The privilege policy hash is keyed by "<objType>.<acctId>.<db>.<table>".
        If that key is built in an undersized buffer the name is silently
        truncated and the lookup never matches, so the rename would be allowed
        while a column privilege still points at the old name.
        """
        tdLog.info("========== RESTRICT: long table name must not bypass the check")
        long_tb = "t" + "l" * 180
        tdSql.execute(
            f"create table {self.dbname}.{long_tb} (ts timestamp, lc1 int, lc2 int) tags(lt int)"
        )
        tdSql.execute("drop user if exists u_long")
        tdSql.execute("create user u_long pass 'taosdata1@'")
        tdSql.execute(f"grant select(ts,lc1) on table {self.dbname}.{long_tb} to u_long")
        tdSql.error(
            f"alter stable {self.dbname}.{long_tb} rename column lc1 lc1_x",
            expectErrInfo="referenced by user privilege",
            fullMatched=False,
        )
        tdSql.execute(f"revoke select(ts,lc1) on table {self.dbname}.{long_tb} from u_long")
        tdSql.execute(f"alter stable {self.dbname}.{long_tb} rename column lc1 lc1_ok")
        self.check_col_exists(long_tb, "lc1_ok", True)
        tdSql.execute("drop user u_long")

    def safe_with_view(self):
        """TC-13: views are NOT checked, matching DROP COLUMN.

        A view keeps only its query text, so there is no dependency list to
        consult. Renaming a source column therefore succeeds and the view fails
        at query time until it is recreated. The important regression here is
        that an unrelated view over a *different* table that happens to have a
        column of the same name must not block the rename.
        """
        tdLog.info("========== view is a weak reference: rename succeeds, no false rejection")
        tdSql.execute(f"create table {self.dbname}.stbx (ts timestamp, c0 int) tags(tx int)")
        tdSql.execute(f"create table {self.dbname}.stby (ts timestamp, c0 int) tags(ty int)")
        try:
            tdSql.execute(f"create view {self.dbname}.v_y as select * from {self.dbname}.stby")
        except Exception as e:
            tdLog.info(f"views unsupported in this build, skip view case: {e}")
            return

        # stby.c0 is referenced by v_y; renaming stbx.c0 must NOT be rejected because of it
        tdSql.execute(f"alter stable {self.dbname}.stbx rename column c0 c0_new")
        self.check_col_exists("stbx", "c0_new", True)

        # a view over the renamed table itself: rename still succeeds, the view then fails
        tdSql.execute(f"create view {self.dbname}.v_x as select ts, c0_new from {self.dbname}.stbx")
        tdSql.execute(f"alter stable {self.dbname}.stbx rename column c0_new c0_final")
        tdSql.error(f"select * from {self.dbname}.v_x")
        # recreating the view against the new name restores it
        tdSql.execute(f"drop view {self.dbname}.v_x")
        tdSql.execute(f"create view {self.dbname}.v_x as select ts, c0_final from {self.dbname}.stbx")
        tdSql.query(f"select * from {self.dbname}.v_x")
        # the unrelated view was never affected
        tdSql.query(f"select * from {self.dbname}.v_y")

    def safe_with_topic(self):
        """TC-16: a topic binds columns by colId, so rename is allowed."""
        tdLog.info("========== topic column reference is colId-bound: rename succeeds")
        tdSql.execute(f"create table {self.dbname}.tpstb (ts timestamp, tp1 int, tp2 int) tags(tpt int)")
        tdSql.execute(f"create table {self.dbname}.tp0 using {self.dbname}.tpstb tags(1)")
        tdSql.execute(f"insert into {self.dbname}.tp0 values(now, 10, 20)")
        tdSql.execute(f"create topic topic_rc as select ts, tp1 from {self.dbname}.tpstb")

        tdSql.execute(f"alter stable {self.dbname}.tpstb rename column tp1 tp1_new")
        self.check_col_exists("tpstb", "tp1_new", True)
        # data is still readable through the renamed column
        tdSql.query(f"select tp1_new from {self.dbname}.tp0")
        tdSql.checkData(0, 0, 10)
        tdSql.execute("drop topic topic_rc")

    def safe_rename_but_restrict_drop_by_stream(self):
        tdLog.info("========== safe: tag referenced by stream (partition by tag) can be renamed")
        # a stream needs an snode to run; provision one before creating the stream
        tdStream.ensureSnode(1)
        tdSql.execute(f"create table {self.dbname}.sstb (ts timestamp, sc1 int) tags(sgroup int)")
        tdSql.execute(f"create table {self.dbname}.s0 using {self.dbname}.sstb tags(1)")
        tdSql.execute(
            f"create stream {self.dbname}.str0 interval(10s) sliding(10s) "
            f"from {self.dbname}.sstb partition by tbname, sgroup "
            f"into {self.dbname}.str0_out as "
            f"select _twstart as ts, count(*) as cnt from %%tbname "
            f"where ts >= _twstart and ts < _twend"
        )

        # TC-12: the tag is referenced by the stream partition, but partition/rollup binds by
        # colId (not name), so renaming it is safe and allowed while the stream is still alive.
        tdSql.execute(f"alter stable {self.dbname}.sstb rename tag sgroup sgroup_x")
        self.check_col_exists("sstb", "sgroup_x", True)

        # Contrast: DROP TAG on that same tag is still rejected, because dropping actually
        # removes the colId the stream is bound to (unlike rename, which keeps it).
        tdSql.error(
            f"alter stable {self.dbname}.sstb drop tag sgroup_x",
            expectErrInfo="referenced by stream",
            fullMatched=False,
        )

        # TC-17: a normal column bound by colId is rename-safe even with the stream present
        tdSql.execute(f"alter stable {self.dbname}.sstb rename column sc1 sc1_ok")
        self.check_col_exists("sstb", "sc1_ok", True)

        # TC-18: dropping the stream releases the drop-tag restriction too
        tdSql.execute(f"drop stream {self.dbname}.str0")
        tdSql.execute(f"alter stable {self.dbname}.sstb rename tag sgroup_x sgroup_ok")
        self.check_col_exists("sstb", "sgroup_ok", True)

    def stream_keeps_writing_after_rename(self):
        """Renaming a trigger column / partition tag must not disturb stream output.

        The colId argument for allowing these renames is only convincing if the
        stream actually keeps producing after one, so this drives real data
        through a stream across a rename of both a trigger column and the
        partition tag, and asserts rows keep landing in the output table.
        """
        tdLog.info("========== stream output survives renaming its trigger column and partition tag")
        tdStream.ensureSnode(1)
        tdSql.execute(f"create table {self.dbname}.wstb (ts timestamp, wc1 int) tags(wg int)")
        tdSql.execute(f"create table {self.dbname}.w0 using {self.dbname}.wstb tags(1)")
        tdSql.execute(
            f"create stream {self.dbname}.str_w interval(1s) sliding(1s) "
            f"from {self.dbname}.wstb partition by tbname, wg "
            f"into {self.dbname}.str_w_out as "
            f"select _twstart as ts, count(*) as cnt from %%tbname "
            f"where ts >= _twstart and ts < _twend"
        )
        # A stream only computes windows at or after its own start time, which the mnode sets a few
        # seconds after CREATE STREAM (visible as "Running start from" in ins_streams). Rows written
        # with timestamps before that point are silently skipped, so the data has to be live (now)
        # and written only once the trigger task is actually Running - a fixed historical timestamp
        # would produce an empty output table and make this case fail for reasons unrelated to
        # the rename.
        tdStream.checkStreamStatus(f"str_w")
        time.sleep(10)
        tdSql.execute(f"insert into {self.dbname}.w0 values(now, 1)(now + 1s, 2)")

        # rename both the trigger column and the partition tag while the stream is live
        tdSql.execute(f"alter stable {self.dbname}.wstb rename column wc1 wc1_new")
        tdSql.execute(f"alter stable {self.dbname}.wstb rename tag wg wg_new")

        # feed data addressed by the NEW names and wait for the windows to close
        tdSql.execute(f"insert into {self.dbname}.w0 values(now + 2s, 3)(now + 3s, 4)(now + 9s, 5)")

        # The output table must carry non-empty windows from before AND after the rename. Empty
        # windows (cnt = 0) are emitted for every interval the stream ticks through, so the filter
        # is what makes the assertion about the data rather than about elapsed time.
        tdSql.checkRowsLoop(
            4, f"select * from {self.dbname}.str_w_out where cnt > 0", loopCount=60, waitTime=1
        )
        # the renamed tag is still queryable and still carries its original value
        tdSql.query(f"select count(*) from {self.dbname}.wstb where wg_new = 1")
        tdSql.checkData(0, 0, 5)
        tdSql.execute(f"drop stream {self.dbname}.str_w")

    def safe_with_stream_output_table(self):
        """A stream's OUTPUT table is not checked, matching DROP COLUMN/TAG/STABLE.

        Identifying an output stable is cheap and exact (mndCheckDropStbForStream
        compares outStbUid), but the existing DROP paths do not check it either,
        so blocking only rename would make the weakest operation the most
        restricted one while genuinely destructive drops stay open. Rename is
        therefore allowed here; the stream keeps writing by colId, but SHOW CREATE
        STREAM then disagrees with the actual table (a documented consequence).

        Covers all three output shapes: stream-created stable, stream bound to a
        pre-existing stable, and the normal-table output form.
        """
        tdLog.info("========== stream output tables are not checked: rename succeeds")
        tdStream.ensureSnode(1)

        # (1) output stable created by the stream itself
        tdSql.execute(f"create table {self.dbname}.ostb (ts timestamp, oc1 int) tags(og int)")
        tdSql.execute(f"create table {self.dbname}.o0 using {self.dbname}.ostb tags(1)")
        tdSql.execute(
            f"create stream {self.dbname}.str_o1 interval(10s) sliding(10s) "
            f"from {self.dbname}.ostb partition by tbname "
            f"into {self.dbname}.out_created as "
            f"select _twstart as ts, count(*) as cnt from %%tbname "
            f"where ts >= _twstart and ts < _twend"
        )
        # wait for the output stable instead of assuming it is ready after a fixed delay
        self.wait_table_exists("out_created")
        tdSql.execute(f"alter stable {self.dbname}.out_created rename column cnt cnt_renamed")
        self.check_col_exists("out_created", "cnt_renamed", True)
        # SHOW CREATE STREAM still reports the original column list - the documented mismatch
        tdSql.query(f"show create stream {self.dbname}.str_o1")

        # (2) output stable pre-created and bound by the stream
        tdSql.execute(f"create table {self.dbname}.bstb (ts timestamp, bc1 int) tags(bg int)")
        tdSql.execute(f"create table {self.dbname}.b0 using {self.dbname}.bstb tags(1)")
        # Binding a pre-created output stable requires its tags to match the ones the stream would
        # generate: "partition by tbname" produces a tag literally named tag_tbname, and any other
        # name is rejected at CREATE STREAM with "Out table tag name mismatch".
        tdSql.execute(
            f"create table {self.dbname}.out_bound (ts timestamp, cnt bigint) tags(tag_tbname varchar(64))"
        )
        tdSql.execute(
            f"create stream {self.dbname}.str_o2 interval(10s) sliding(10s) "
            f"from {self.dbname}.bstb partition by tbname "
            f"into {self.dbname}.out_bound as "
            f"select _twstart as ts, count(*) as cnt from %%tbname "
            f"where ts >= _twstart and ts < _twend"
        )
        # out_bound was pre-created above, so it exists already; no wait needed
        tdSql.execute(f"alter stable {self.dbname}.out_bound rename column cnt cnt_b")
        self.check_col_exists("out_bound", "cnt_b", True)
        # a tag on the bound output stable renames too
        tdSql.execute(f"alter stable {self.dbname}.out_bound rename tag tag_tbname tbn_b")
        self.check_col_exists("out_bound", "tbn_b", True)
        # and the trigger table's own columns/tags remain renameable (colId-bound)
        tdSql.execute(f"alter stable {self.dbname}.bstb rename column bc1 bc1_ok")
        self.check_col_exists("bstb", "bc1_ok", True)
        # this tag is NOT part of the stream's partition, so it is unreferenced either way
        tdSql.execute(f"alter stable {self.dbname}.bstb rename tag bg bg_ok")
        self.check_col_exists("bstb", "bg_ok", True)

        # (3) normal-table output form: rename goes through vnode, never reaches mnode
        tdSql.execute(f"create table {self.dbname}.nstb (ts timestamp, nc1 int) tags(ng int)")
        tdSql.execute(f"create table {self.dbname}.n0 using {self.dbname}.nstb tags(1)")
        tdSql.execute(
            f"create stream {self.dbname}.str_o3 interval(10s) sliding(10s) "
            f"from {self.dbname}.nstb "
            f"into {self.dbname}.out_ntb as "
            f"select _twstart as ts, count(*) as cnt from {self.dbname}.nstb "
            f"where ts >= _twstart and ts < _twend"
        )
        # Unlike the two stable forms above, a normal-table output is not created at CREATE STREAM:
        # it appears only when the stream first writes a result, which needs data timestamped at or
        # after the stream's start time plus one closed 10s window. Renaming before that point fails
        # with "Table does not exist", so wait for the table to actually show up.
        # A single insert can still land before the stream's start time, in which case it is skipped
        # and the output table is never created. Keep feeding while waiting so at least one batch is
        # guaranteed to fall inside a window the stream actually computes.
        tdStream.checkStreamStatus(f"str_o3")
        self.wait_table_exists_while_writing("out_ntb", f"{self.dbname}.n0")
        tdSql.execute(f"alter table {self.dbname}.out_ntb rename column cnt cnt_n")
        self.check_col_exists("out_ntb", "cnt_n", True)

        tdSql.execute(f"drop stream {self.dbname}.str_o1")
        tdSql.execute(f"drop stream {self.dbname}.str_o2")
        tdSql.execute(f"drop stream {self.dbname}.str_o3")

    def propagate_to_other_session(self):
        """TC-22: another session picks up the new name within a heartbeat cycle.

        The renaming session refreshes its catalog straight from the alter
        response; every other session learns about it through the stable-info
        heartbeat, so a second connection must converge on the new name (and stop
        resolving the old one) without being reconnected.
        """
        tdLog.info("========== TC-22: other sessions see the new name after a heartbeat")
        tdSql.execute(f"create table {self.dbname}.hbstb (ts timestamp, hc1 int) tags(ht int)")
        tdSql.execute(f"create table {self.dbname}.hb0 using {self.dbname}.hbstb tags(1)")
        tdSql.execute(f"insert into {self.dbname}.hb0 values(now, 11)")

        conn = taos.connect()
        otherSql = TDSql()
        otherSql.init(conn.cursor())
        try:
            # prime the other session's catalog cache with the OLD schema
            otherSql.query(f"select hc1 from {self.dbname}.hb0")
            otherSql.checkData(0, 0, 11)

            tdSql.execute(f"alter stable {self.dbname}.hbstb rename column hc1 hc1_hb")
            # the renaming session sees it at once
            self.check_col_exists("hbstb", "hc1_hb", True)

            # the other session converges within a few heartbeat cycles
            converged = False
            for _ in range(60):
                try:
                    otherSql.query(f"select hc1_hb from {self.dbname}.hb0")
                    if otherSql.queryResult and otherSql.queryResult[0][0] == 11:
                        converged = True
                        break
                except Exception:
                    pass
                time.sleep(1)
            assert converged, "other session never picked up the renamed column"
            # and the old name stops resolving there too
            otherSql.error(f"select hc1 from {self.dbname}.hb0")
        finally:
            otherSql.close()

    def show_create_reflects_new_name(self):
        """TC-24-adjacent: SHOW CREATE output must carry the new column name."""
        tdLog.info("========== SHOW CREATE TABLE/STABLE reflects the rename")
        tdSql.execute(f"create table {self.dbname}.scstb (ts timestamp, sc_a int) tags(sc_t int)")
        tdSql.execute(f"create table {self.dbname}.sc0 using {self.dbname}.scstb tags(1)")
        tdSql.execute(f"alter stable {self.dbname}.scstb rename column sc_a sc_a_new")
        tdSql.execute(f"alter stable {self.dbname}.scstb rename tag sc_t sc_t_new")

        tdSql.query(f"show create stable {self.dbname}.scstb")
        create_sql = str(tdSql.queryResult)
        assert "sc_a_new" in create_sql, f"new column name missing from SHOW CREATE: {create_sql}"
        assert "sc_t_new" in create_sql, f"new tag name missing from SHOW CREATE: {create_sql}"

        tdSql.query(f"show create table {self.dbname}.sc0")
        child_sql = str(tdSql.queryResult)
        tdLog.info(f"show create table for child: {child_sql}")

    def persist_after_restart(self):
        """TC-23: the new name and the bumped schema version survive a restart."""
        tdLog.info("========== TC-23: rename persists across taosd restart")
        tdSql.execute(f"create table {self.dbname}.rstb2 (ts timestamp, rc1 int) tags(rt int)")
        tdSql.execute(f"create table {self.dbname}.r0 using {self.dbname}.rstb2 tags(1)")
        tdSql.execute(f"insert into {self.dbname}.r0 values(now, 42)")
        tdSql.execute(f"alter stable {self.dbname}.rstb2 rename column rc1 rc1_new")
        col_id_before = self.get_col_id("rstb2", "rc1_new")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use {self.dbname}")
        self.check_col_exists("rstb2", "rc1_new", True)
        self.check_col_exists("rstb2", "rc1", False)
        self.check_col_exists("r0", "rc1_new", True)
        tdSql.query(f"select rc1_new from {self.dbname}.r0")
        tdSql.checkData(0, 0, 42)
        assert self.get_col_id("rstb2", "rc1_new") == col_id_before

    def test_stable_rename_column(self):
        """Alter: super-table and virtual super-table RENAME COLUMN / RENAME TAG

        1. Basic super-table normal-column rename (new capability) + tag rename + colId stability
        2. Rename back to the original name
        3. Error cases: missing column, duplicate name, child table, illegal names
        3a. Timestamp column and composite primary key column rename are both allowed; DROP /
            MODIFY on the primary key column stay rejected
        4. Virtual super-table column and tag rename; colRef is not checked; VST with children
           rejects the rename
        5. TSMA blocks rename until dropped
        6. RESTRICT: column-level / tag-cond privilege blocks rename, releasable, and reports
           user vs role in the error; tag-cond matching is conservative and may over-reject
        7. Not a column reference: table-level and db-level grants, and built-in system roles;
           RESTRICT still fires for a very long table name
        8. A db owner can rename its own columns but is still subject to RESTRICT
        9. Not checked by design: view (weak reference), topic (colId-bound)
        10. Streams bind by colId: renaming a trigger column or partition tag is allowed and the
            stream keeps producing; DROP TAG on a referenced tag is still rejected; output tables
            (stream-created, bound, and normal-table form) are not checked either
        11. Metadata propagation: other sessions converge within a heartbeat; SHOW CREATE reflects
            the new names; the rename persists across a taosd restart

        Catalog:
            - SuperTable:Alter

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        History:
            - 2026-07 Created for stb/vstb rename column feature
            - 2026-08 Added colId stability, TSMA, role/long-name RESTRICT, view and topic
              safety, restart persistence; removed silent skips from the stream case
            - 2026-08 Timestamp column rename is allowed (was wrongly rejected on the stable
              path only, contradicting the normal-table path); only COL_IS_KEY is restricted
            - 2026-08 Stream-referenced tag rename is allowed (colId-bound, matching
              stream_rollup_basic.py); only DROP TAG stays restricted. Added coverage from the
              manual verification script: user-vs-role error codes, conservative tag-cond
              matching, table/db-level grants, system roles, db owner, virtual colRef, child
              VSTs, stream output tables, live stream output across a rename, cross-session
              heartbeat propagation, and SHOW CREATE output
            - 2026-08 Composite primary key column rename is now allowed on both paths: key
              semantics ride on COL_IS_KEY plus colId, never on the column name. DROP COLUMN
              and MODIFY COLUMN on that column stay rejected
        """
        self.prepare_db()
        self.rename_basic_stable()
        self.rename_back_to_original()
        self.rename_error_cases()
        self.rename_invalid_names()
        self.rename_timestamp_column()
        self.rename_with_composite_primary_key()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.rename_virtual_stable()
        self.safe_with_virtual_colref()
        self.restrict_virtual_stable_with_children()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.restrict_by_tsma()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.restrict_by_privilege()
        self.restrict_by_role_privilege()
        self.restrict_by_tag_cond_is_conservative()
        self.safe_with_table_and_db_privilege()
        self.safe_with_system_role()
        self.restrict_with_long_table_name()
        tdStream.dropAllStreamsAndDbs()

        self.safe_as_db_owner()

        self.prepare_db()
        self.safe_with_view()
        self.safe_with_topic()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.safe_rename_but_restrict_drop_by_stream()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.stream_keeps_writing_after_rename()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.safe_with_stream_output_table()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.propagate_to_other_session()
        self.show_create_reflects_new_name()
        tdStream.dropAllStreamsAndDbs()

        self.prepare_db()
        self.persist_after_restart()
        tdStream.dropAllStreamsAndDbs()

        tdLog.success(f"{__file__} successfully executed")
