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
"""
Batch meta txn: the full ALTER action matrix inside a transaction.

Covers FS 《元数据 DDL 支持事务二期-支持更多操作类型 FS》 §4.11.3. Phase 1 only spelled out
6 of the 26 TSDB_ALTER_TABLE_* actions; the rest had a working code path but were never
confirmed by the FS nor covered by tests. Each case below drives one action through the
three states that matter for a transaction:

    in-txn visible  ->  ROLLBACK reverts  ->  COMMIT publishes

  m1  - MODIFY COLUMN (UPDATE_COLUMN_BYTES) on normal table and super table
  m2  - MODIFY TAG (UPDATE_TAG_BYTES) on super table
  m3  - RENAME COLUMN (UPDATE_COLUMN_NAME) on super table — new in this release
  m4  - ADD COLUMN with compression options (ADD_COLUMN_WITH_COMPRESS_OPTION)
  m5  - MODIFY COLUMN ENCODE/COMPRESS/LEVEL (UPDATE_COLUMN_COMPRESS) on a column that
        predates the txn
  m6  - Table options: TTL and COMMENT (UPDATE_OPTIONS)
  m7  - Virtual table column reference: ALTER COLUMN .. SET ref / SET NULL
        (ALTER_COLUMN_REF / REMOVE_COLUMN_REF) and ADD COLUMN .. FROM
        (ADD_COLUMN_WITH_COLUMN_REF)
  m8  - Virtual normal table tag reference: ADD TAG .. FROM / SET TAG = ref
        (ADD_TAG_WITH_TAG_REF / ALTER_TAG_REF)
  m9  - Multiple different actions on one super table in a single txn, verifying the
        accumulated-schema replay agrees with what COMMIT produces
  m10 - Tag index DDL (CREATE INDEX / DROP INDEX) is rejected inside a txn
  m11 - DROP TAG / RENAME TAG cascade onto an existing tag index inside a txn
  m12 - ADD/DROP BASE ON (virtual super table inheritance) inside a txn
"""

from new_test_framework.utils import tdLog, tdSql, tdCom

ERR_TXN_INVALID_OP = "Only table DDL (CREATE/DROP/ALTER TABLE) allowed in transaction"
ERR_VST_HAS_CHILD = "VST has child VSTs, cannot drop or alter"


class TestBatchMetaTxnOpMatrix:

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_mx_db")
        tdSql.execute("create database txn_mx_db vgroups 2 keep 36500")
        tdSql.execute("use txn_mx_db")

    def _reset(self):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("use txn_mx_db")
        # m12 creates cvst inheriting pvst/pvst2 and detaches it on its success path, but an
        # assertion failure before that leaves the child VST behind. Dropping a parent VST that
        # still has children is refused ("VST has child VSTs, cannot drop or alter"), which would
        # abort this reset and cascade into every later case, masking the real failure. Detach
        # first — that is what _drop_inheritance exists for — so the drops below can run in any
        # order. Harmless when cvst is absent: it uses execute_ignore_error.
        self._drop_inheritance("cvst", ["pvst2", "pvst"])
        for show, drop in [("show vtables", "drop table"),
                           ("show stables", "drop stable"),
                           ("show tables", "drop table")]:
            tdSql.query(show)
            for i in range(tdSql.queryRows):
                tdSql.execute(f"{drop} if exists {tdSql.queryResult[i][0]}")

    def _other(self):
        s = tdCom.newTdSql()
        s.execute("use txn_mx_db")
        return s

    def _desc(self, table, session=None):
        """{col_name: (type, length, note)} from DESCRIBE."""
        s = session or tdSql
        s.query(f"describe {table}")
        return {s.queryResult[i][0]: (s.queryResult[i][1],
                                      s.queryResult[i][2],
                                      s.queryResult[i][3])
                for i in range(s.queryRows)}

    def _col_len(self, table, col, session=None):
        return self._desc(table, session)[col][1]

    def _cols(self, table, session=None):
        return set(self._desc(table, session).keys())

    def _setup_stb(self, with_child=True):
        tdSql.execute("create table stb (ts timestamp, v int, s varchar(10)) "
                      "tags (grp int, name varchar(10))")
        if with_child:
            tdSql.execute("create table c1 using stb tags (1, 'a')")
            tdSql.execute("insert into c1 values(now, 1, 'x')")

    def _setup_vsrc(self):
        tdSql.execute("create table vsrc_stb (ts timestamp, c1 int, c2 int) tags (t1 int)")
        tdSql.execute("create table vsrc using vsrc_stb tags (7)")
        tdSql.execute("insert into vsrc values(now, 11, 22)")

    # =========================================================================
    # m1: MODIFY COLUMN (widen a varchar) on super table and normal table
    # =========================================================================

    def m1_modify_column_bytes(self):
        tdLog.info("======== m1_modify_column_bytes")
        self._reset()
        self._setup_stb()
        tdSql.execute("create table ntb (ts timestamp, v int, s varchar(10))")
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb modify column s varchar(30)")
        tdSql.execute("alter table ntb modify column s varchar(40)")
        assert self._col_len("stb", "s") == 30
        assert self._col_len("ntb", "s") == 40
        # Other session still sees the original widths.
        assert self._col_len("stb", "s", session=s2) == 10
        assert self._col_len("ntb", "s", session=s2) == 10

        tdSql.execute("ROLLBACK")
        assert self._col_len("stb", "s") == 10
        assert self._col_len("ntb", "s") == 10

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb modify column s varchar(30)")
        tdSql.execute("alter table ntb modify column s varchar(40)")
        tdSql.execute("COMMIT")
        assert self._col_len("stb", "s") == 30
        assert self._col_len("ntb", "s") == 40
        assert self._col_len("stb", "s", session=s2) == 30
        assert self._col_len("ntb", "s", session=s2) == 40

        s2.close()
        tdLog.info("m1 PASSED: MODIFY COLUMN isolation/rollback/commit")

    # =========================================================================
    # m2: MODIFY TAG (widen a tag) on super table
    # =========================================================================

    def m2_modify_tag_bytes(self):
        tdLog.info("======== m2_modify_tag_bytes")
        self._reset()
        self._setup_stb()
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb modify tag name varchar(30)")
        assert self._col_len("stb", "name") == 30
        assert self._col_len("stb", "name", session=s2) == 10

        tdSql.execute("ROLLBACK")
        assert self._col_len("stb", "name") == 10

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb modify tag name varchar(30)")
        tdSql.execute("COMMIT")
        assert self._col_len("stb", "name") == 30
        assert self._col_len("stb", "name", session=s2) == 30

        s2.close()
        tdLog.info("m2 PASSED: MODIFY TAG isolation/rollback/commit")

    # =========================================================================
    # m3: RENAME COLUMN on a super table (new capability this release)
    # =========================================================================

    def m3_rename_column_stb(self):
        tdLog.info("======== m3_rename_column_stb")
        self._reset()
        self._setup_stb()
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb rename column v volt")
        cols = self._cols("stb")
        assert "volt" in cols and "v" not in cols, cols
        # Child tables follow the super table's schema.
        assert "volt" in self._cols("c1")
        # Unchanged elsewhere.
        assert "v" in self._cols("stb", session=s2)

        tdSql.execute("ROLLBACK")
        assert "v" in self._cols("stb") and "volt" not in self._cols("stb")

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb rename column v volt")
        tdSql.execute("COMMIT")
        assert "volt" in self._cols("stb")
        assert "volt" in self._cols("stb", session=s2)
        # Renaming keeps colId, so existing data stays readable.
        tdSql.query("select volt from c1")
        assert tdSql.queryRows == 1

        s2.close()
        tdLog.info("m3 PASSED: RENAME COLUMN on stb isolation/rollback/commit")

    # =========================================================================
    # m4/m5: ADD COLUMN with compression options, then MODIFY its compression
    # =========================================================================

    def m4_column_compression_options(self):
        tdLog.info("======== m4_column_compression_options")
        self._reset()
        self._setup_stb()
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb add column c9 int "
                      "encode 'simple8b' compress 'lz4' level 'medium'")
        assert "c9" in self._cols("stb")
        assert "c9" not in self._cols("stb", session=s2)
        # Change the compression of the column added in this same txn.
        tdSql.execute("alter stable stb modify column c9 compress 'zstd'")

        tdSql.execute("ROLLBACK")
        assert "c9" not in self._cols("stb")

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb add column c9 int "
                      "encode 'simple8b' compress 'lz4' level 'medium'")
        tdSql.execute("alter stable stb modify column c9 compress 'zstd'")
        tdSql.execute("COMMIT")
        assert "c9" in self._cols("stb")
        assert "c9" in self._cols("stb", session=s2)

        s2.close()
        tdLog.info("m4 PASSED: ADD COLUMN with compress + MODIFY compress in txn")

    # =========================================================================
    # m5: MODIFY COLUMN ENCODE/COMPRESS/LEVEL (UPDATE_COLUMN_COMPRESS) on a
    #     PRE-EXISTING column.
    #
    #     Distinct from m4, which only modifies a column added in the same txn
    #     (so its whole existence rolls back) and never checks the stored codec.
    #     Here the column predates the txn, so ROLLBACK must restore the previous
    #     compression rather than remove the column, and the values are asserted.
    # =========================================================================

    def _codec(self, table, col, session=None):
        """(encode, compress, level) from DESCRIBE, which carries them past note."""
        s = session or tdSql
        s.query(f"describe {table}")
        for i in range(s.queryRows):
            if s.queryResult[i][0] == col:
                return (s.queryResult[i][4], s.queryResult[i][5], s.queryResult[i][6])
        raise AssertionError(f"column {col} not found in {table}")

    def m5_modify_column_compress(self):
        tdLog.info("======== m5_modify_column_compress")
        self._reset()
        tdSql.execute("create table stb (ts timestamp, v int, w int "
                      "encode 'simple8b' compress 'lz4' level 'medium') tags (grp int)")
        tdSql.execute("create table c1 using stb tags (1)")
        tdSql.execute("insert into c1 values(now, 1, 10)")
        s2 = self._other()

        before = self._codec("stb", "w")
        assert before[1] == "lz4", before

        # --- ROLLBACK restores the original codec on a pre-existing column.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb modify column w compress 'zstd'")
        assert self._codec("stb", "w")[1] == "zstd", self._codec("stb", "w")
        # Not visible to another session while uncommitted.
        assert self._codec("stb", "w", session=s2)[1] == "lz4", \
            self._codec("stb", "w", session=s2)
        tdSql.execute("ROLLBACK")
        assert self._codec("stb", "w") == before, \
            f"ROLLBACK did not restore codec: {self._codec('stb', 'w')} != {before}"

        # --- COMMIT publishes all three of encode / compress / level.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb modify column w compress 'zstd'")
        tdSql.execute("alter stable stb modify column w level 'high'")
        tdSql.execute("COMMIT")
        after = self._codec("stb", "w")
        assert after[1] == "zstd" and after[2] == "high", after
        assert self._codec("stb", "w", session=s2) == after, \
            self._codec("stb", "w", session=s2)

        # Data written before the codec change is still readable afterwards.
        tdSql.query("select w from stb")
        tdSql.checkData(0, 0, 10)
        tdSql.execute("insert into c1 values(now, 2, 20)")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 2)

        s2.close()
        tdLog.info("m5 PASSED: MODIFY COLUMN compress/level on pre-existing column")

    # =========================================================================
    # m6: table options — TTL and COMMENT
    #     TTL is a normal/child table option; ALTER STABLE .. TTL is not valid
    #     syntax, so the super table only exercises COMMENT here.
    # =========================================================================

    def m6_table_options(self):
        tdLog.info("======== m6_table_options")
        self._reset()
        self._setup_stb()
        tdSql.execute("create table ntb (ts timestamp, v int)")

        def ntb_opts():
            """(ttl, table_comment) of ntb read back from the catalog."""
            tdSql.query("select `ttl`, table_comment from information_schema.ins_tables "
                        f"where db_name = 'txn_mx_db' and table_name = 'ntb'")
            assert tdSql.queryRows == 1, f"ntb missing from ins_tables ({tdSql.queryRows} rows)"
            return tdSql.queryResult[0][0], tdSql.queryResult[0][1]

        # Baseline TTL is the db default; no comment.
        ttl0, cmt0 = ntb_opts()
        assert cmt0 in (None, ""), f"baseline comment should be empty, got {cmt0!r}"

        # --- ROLLBACK: the in-txn TTL/COMMENT must be reverted, not left behind.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb ttl 100")
        tdSql.execute("alter table ntb comment 'in-txn comment'")
        tdSql.execute("alter stable stb comment 'stb comment'")
        tdSql.execute("ROLLBACK")
        ttl_rb, cmt_rb = ntb_opts()
        assert ttl_rb == ttl0, f"ROLLBACK did not revert ttl: {ttl0} -> {ttl_rb}"
        assert cmt_rb in (None, ""), f"ROLLBACK did not revert comment: {cmt_rb!r}"

        # --- COMMIT: the options must be published to the catalog.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table ntb ttl 50")
        tdSql.execute("alter table ntb comment 'committed'")
        tdSql.execute("alter stable stb comment 'stb committed'")
        tdSql.execute("COMMIT")
        ttl_c, cmt_c = ntb_opts()
        assert ttl_c == 50, f"COMMIT did not publish ttl=50, got {ttl_c}"
        assert cmt_c == "committed", f"COMMIT did not publish comment, got {cmt_c!r}"

        # The stb comment is published too (super table path).
        tdSql.query("select table_comment from information_schema.ins_stables "
                    "where db_name = 'txn_mx_db' and stable_name = 'stb'")
        assert tdSql.queryRows == 1
        assert tdSql.queryResult[0][0] == "stb committed", \
            f"stb comment not published, got {tdSql.queryResult[0][0]!r}"

        # The table is still usable after going through the txn paths.
        tdSql.execute("insert into ntb values(now, 1)")
        tdSql.query("select * from ntb")
        assert tdSql.queryRows == 1

        tdLog.info("m6 PASSED: TTL/COMMENT in-txn visible/ROLLBACK-reverted/COMMIT-published")

    # =========================================================================
    # m7: virtual table column references
    #     ADD COLUMN .. FROM / ALTER COLUMN .. SET ref / ALTER COLUMN .. SET NULL
    # =========================================================================

    def m7_virtual_column_refs(self):
        tdLog.info("======== m7_virtual_column_refs")
        self._reset()
        self._setup_vsrc()
        tdSql.execute("create vtable vnt (ts timestamp, a int from txn_mx_db.vsrc.c1)")
        s2 = self._other()

        tdSql.execute("BEGIN")
        # Add a second column that references another source column.
        tdSql.execute("alter table vnt add column b int from txn_mx_db.vsrc.c2")
        assert "b" in self._cols("vnt")
        assert "b" not in self._cols("vnt", session=s2)
        # Re-point an existing reference, then drop the reference entirely.
        tdSql.execute("alter table vnt alter column a set txn_mx_db.vsrc.c2")
        tdSql.execute("alter table vnt alter column a set null")

        tdSql.execute("ROLLBACK")
        assert "b" not in self._cols("vnt")
        # The original reference still resolves after rollback.
        tdSql.query("select a from vnt")

        tdSql.execute("BEGIN")
        tdSql.execute("alter table vnt add column b int from txn_mx_db.vsrc.c2")
        tdSql.execute("alter table vnt alter column a set txn_mx_db.vsrc.c2")
        tdSql.execute("COMMIT")
        assert "b" in self._cols("vnt")
        assert "b" in self._cols("vnt", session=s2)
        tdSql.query("select a, b from vnt")

        s2.close()
        tdLog.info("m7 PASSED: virtual column refs isolation/rollback/commit")

    # =========================================================================
    # m8: virtual normal table tag references
    #     ADD TAG .. FROM / SET TAG = literal / SET TAG = ref
    # =========================================================================

    def m8_virtual_tag_refs(self):
        tdLog.info("======== m8_virtual_tag_refs")
        self._reset()
        self._setup_vsrc()
        tdSql.execute("create vtable vnt (ts timestamp, a int from txn_mx_db.vsrc.c1)")
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter table vnt add tag tg int from txn_mx_db.vsrc.t1")   # tag-ref
        tdSql.execute("alter table vnt add tag ot int")                          # owned tag
        tdSql.execute("alter table vnt set tag ot = 5")                          # owned value
        tags = self._cols("vnt")
        assert "tg" in tags and "ot" in tags, tags
        assert "tg" not in self._cols("vnt", session=s2)
        # owned -> ref conversion
        tdSql.execute("alter table vnt set tag ot = txn_mx_db.vsrc.t1")

        tdSql.execute("ROLLBACK")
        tags = self._cols("vnt")
        assert "tg" not in tags and "ot" not in tags, tags

        tdSql.execute("BEGIN")
        tdSql.execute("alter table vnt add tag tg int from txn_mx_db.vsrc.t1")
        tdSql.execute("alter table vnt add tag ot int")
        tdSql.execute("alter table vnt set tag ot = 5")
        tdSql.execute("COMMIT")
        tags = self._cols("vnt")
        assert "tg" in tags and "ot" in tags, tags
        assert "tg" in self._cols("vnt", session=s2)
        # Dropping a tag (owned or ref) also works in a txn.
        tdSql.execute("BEGIN")
        tdSql.execute("alter table vnt drop tag tg")
        assert "tg" not in self._cols("vnt")
        tdSql.execute("COMMIT")
        assert "tg" not in self._cols("vnt", session=s2)

        s2.close()
        tdLog.info("m8 PASSED: virtual tag refs isolation/rollback/commit")

    # =========================================================================
    # m9: several different actions on one super table in a single txn.
    #     Exercises the accumulated-schema replay: statement-time validation runs
    #     against base+prior ops, and COMMIT must reproduce exactly the same
    #     result. A mismatch here is what a drifted action set would look like.
    # =========================================================================

    def m9_mixed_actions_one_txn(self):
        tdLog.info("======== m9_mixed_actions_one_txn")
        self._reset()
        self._setup_stb()
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb add column extra int")
        tdSql.execute("alter stable stb modify column s varchar(50)")
        tdSql.execute("alter stable stb rename column v volt")
        tdSql.execute("alter stable stb add tag zone varchar(8)")
        tdSql.execute("alter stable stb modify tag name varchar(20)")
        tdSql.execute("alter stable stb drop column extra")

        # Schema the txn should observe after all six statements.
        desc = self._desc("stb")
        assert "extra" not in desc
        assert "volt" in desc and "v" not in desc
        assert desc["s"][1] == 50
        assert desc["zone"][2] == "TAG"
        assert desc["name"][1] == 20
        # Nothing visible outside yet.
        outside = self._desc("stb", session=s2)
        assert "volt" not in outside and outside["s"][1] == 10

        tdSql.execute("COMMIT")

        # COMMIT must publish exactly what the txn observed.
        for sess in (None, s2):
            d = self._desc("stb", session=sess)
            assert "extra" not in d, d
            assert "volt" in d and "v" not in d, d
            assert d["s"][1] == 50, d
            assert d["name"][1] == 20, d
            assert d["zone"][2] == "TAG", d

        s2.close()
        tdLog.info("m9 PASSED: mixed actions accumulate consistently through COMMIT")

    # =========================================================================
    # m10: tag index DDL is not transactional
    # =========================================================================

    def m10_index_ddl_rejected(self):
        tdLog.info("======== m10_index_ddl_rejected")
        self._reset()
        # A dedicated stb: the tag getting a user index must not be the first tag,
        # since an index on that one is created automatically at CREATE TABLE time.
        tdSql.execute("create table istb (ts timestamp, v int) "
                      "tags (grp int, name varchar(10), zone varchar(10))")
        tdSql.execute("create index idx_pre on istb (name)")

        tdSql.execute("BEGIN")
        tdSql.error("create index idx_new on istb (zone)", expectErrInfo=ERR_TXN_INVALID_OP)
        tdSql.error("drop index idx_pre", expectErrInfo=ERR_TXN_INVALID_OP)
        # The rejection does not poison the txn: table DDL still works.
        tdSql.execute("alter stable istb add column okcol int")
        assert "okcol" in self._cols("istb")
        tdSql.execute("ROLLBACK")
        assert "okcol" not in self._cols("istb")

        # The rejected statements had no effect: idx_pre survives, idx_new was
        # never created, so both index statements still work outside the txn.
        tdSql.execute("create index idx_new on istb (zone)")
        tdSql.execute("drop index idx_new")
        tdSql.execute("drop index idx_pre")

        tdLog.info("m10 PASSED: CREATE/DROP INDEX rejected in txn, allowed outside")

    # =========================================================================
    # m11: DROP TAG / RENAME TAG cascade to the tag index object inside a txn.
    #
    # FS §4.11.3: explicit index DDL is rejected in a txn (m10), but the *cascade*
    # from DROP TAG / RENAME TAG onto an existing index on that tag "continues to
    # work inside a txn — applied on COMMIT, undone on ROLLBACK".
    #
    # Regression: the cascade lived only in the non-txn path
    # (mndAlterStbAndUpdateTagIdxImp). The COMMIT path wrote the STB's own SDB logs
    # but never touched SIdxObj, which is a separate SDB object, so the schema
    # change committed while the index was left pointing at a dropped or renamed
    # tag — a dangling index object. ROLLBACK was always fine: it never appends the
    # index logs, so there is nothing to undo.
    # =========================================================================

    def _indexes(self, db="txn_mx_db"):
        """{index_name: column_name} for a database."""
        tdSql.query("select index_name, column_name from information_schema.ins_indexes "
                    f"where db_name = '{db}'")
        return {tdSql.queryResult[i][0]: tdSql.queryResult[i][1]
                for i in range(tdSql.queryRows)}

    def m11_tag_index_cascade_in_txn(self):
        tdLog.info("======== m11_tag_index_cascade_in_txn")
        self._reset()
        # As in m10, the indexed tag must not be the first one: the first tag gets an
        # automatic index (t1_<stb>) at CREATE TABLE time.
        tdSql.execute("create table cstb (ts timestamp, v int) "
                      "tags (grp int, zone varchar(10), area varchar(10))")
        tdSql.execute("create index cidx_zone on cstb (zone)")
        tdSql.execute("create index cidx_area on cstb (area)")
        assert self._indexes().get("cidx_zone") == "zone", self._indexes()

        # --- DROP TAG + COMMIT: the index on that tag must go with it.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cstb drop tag zone")
        tdSql.execute("COMMIT")
        idx = self._indexes()
        assert "zone" not in self._cols("cstb"), self._cols("cstb")
        assert "cidx_zone" not in idx, f"dangling index after DROP TAG in txn: {idx}"
        assert idx.get("cidx_area") == "area", idx

        # --- RENAME TAG + COMMIT: the index must follow the new tag name.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cstb rename tag area area2")
        tdSql.execute("COMMIT")
        idx = self._indexes()
        assert "area2" in self._cols("cstb"), self._cols("cstb")
        assert idx.get("cidx_area") == "area2", \
            f"index still on the old tag name after RENAME TAG in txn: {idx}"

        # --- ROLLBACK leaves both the tag and its index untouched.
        tdSql.execute("alter stable cstb add tag extra varchar(10)")
        tdSql.execute("create index cidx_extra on cstb (extra)")
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cstb drop tag extra")
        tdSql.execute("ROLLBACK")
        idx = self._indexes()
        assert "extra" in self._cols("cstb"), self._cols("cstb")
        assert idx.get("cidx_extra") == "extra", f"ROLLBACK lost the index: {idx}"

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cstb rename tag extra extra9")
        tdSql.execute("ROLLBACK")
        idx = self._indexes()
        assert idx.get("cidx_extra") == "extra", \
            f"ROLLBACK left the index on the in-txn name: {idx}"

        # The surviving indexes are still functional, not just present in the catalog.
        tdSql.execute("create table cc1 using cstb tags (1, 'a1', 'e1')")
        tdSql.execute("insert into cc1 values(now, 1)")
        tdSql.query("select count(*) from cstb where extra = 'e1'")
        tdSql.checkData(0, 0, 1)

        tdLog.info("m11 PASSED: tag index cascade applied on COMMIT, untouched by ROLLBACK")

    # =========================================================================
    # m12: ADD/DROP BASE ON (virtual super table inheritance) inside a txn.
    #
    # FS §4.11.4: the non-txn path validates "does the target VST already have
    # children" as a SERIAL pre-action, which cannot be reused in a txn — the check's
    # result could be stale by COMMIT time (the same txn may have just created a
    # child). In a txn the validation runs at statement time against the accumulated
    # virtual schema, and the check is re-issued as a COMMIT-time redo action.
    #
    # Writing this case surfaced two defects, both fixed alongside it:
    #   - the CHECK_HAS_CTB redo action left groupId 0, which TRN_EXEC_GROUP_PARALLEL
    #     (the mode the txn COMMIT trans uses) rejects, so COMMIT returned "Internal
    #     error". The non-txn path uses a serial trans, so 0 was fine there. This case
    #     fails without that fix.
    #   - mndCommitTxn destroyed pStreamActs and then fell through to _exit, which
    #     destroyed it again — a heap-use-after-free that killed the mnode. It only
    #     fires on the mndTxnApplyShadowOps failure path, which the groupId bug above
    #     happened to trigger; with groupId correct this case no longer reaches it, so
    #     the double-free fix is NOT covered here. Left fixed on its own merits.
    # =========================================================================

    def _drop_inheritance(self, child, parents):
        """Detach a child VST from its parents so _reset can drop them in any order."""
        for p in parents:
            tdSql.execute_ignore_error(f"alter stable {child} drop base on txn_mx_db.{p}")

    def m12_base_on_in_txn(self):
        tdLog.info("======== m12_base_on_in_txn")
        self._reset()
        tdSql.execute("create stable pvst (ts timestamp, pv int) tags (pt int) virtual 1")
        tdSql.execute("create stable pvst2 (ts timestamp, pw int) tags (pt2 int) virtual 1")
        # cvst inherits pvst at create time; pvst2 is attached/detached below.
        # BASE ON is only accepted on CREATE STABLE, not CREATE TABLE.
        tdSql.execute("create stable cvst (ts timestamp, own int) tags (ct int) "
                      "base on txn_mx_db.pvst virtual 1")
        base_cols = self._cols("cvst")
        assert {"pv", "pt", "own", "ct"} <= base_cols, base_cols
        assert "pw" not in base_cols and "pt2" not in base_cols, base_cols
        s2 = self._other()

        # --- ADD BASE ON: visible in-txn, invisible outside, reverted by ROLLBACK.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cvst add base on txn_mx_db.pvst2")
        assert {"pw", "pt2"} <= self._cols("cvst"), self._cols("cvst")
        assert "pw" not in self._cols("cvst", session=s2), self._cols("cvst", session=s2)
        tdSql.execute("ROLLBACK")
        assert self._cols("cvst") == base_cols, self._cols("cvst")

        # --- ADD BASE ON + COMMIT publishes the inherited columns and tags.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cvst add base on txn_mx_db.pvst2")
        tdSql.execute("COMMIT")
        assert {"pw", "pt2"} <= self._cols("cvst"), self._cols("cvst")
        assert {"pw", "pt2"} <= self._cols("cvst", session=s2), self._cols("cvst", session=s2)

        # --- DROP BASE ON: ROLLBACK keeps the inheritance.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cvst drop base on txn_mx_db.pvst2")
        assert "pw" not in self._cols("cvst"), self._cols("cvst")
        tdSql.execute("ROLLBACK")
        assert {"pw", "pt2"} <= self._cols("cvst"), self._cols("cvst")

        # --- DROP BASE ON + COMMIT detaches for everyone.
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable cvst drop base on txn_mx_db.pvst2")
        tdSql.execute("COMMIT")
        assert "pw" not in self._cols("cvst"), self._cols("cvst")
        assert "pw" not in self._cols("cvst", session=s2), self._cols("cvst", session=s2)

        # --- A parent that has children cannot be reshaped, inside a txn as well as
        #     outside. The statement fails; the txn is then only good for ROLLBACK.
        tdSql.execute("BEGIN")
        tdSql.error("alter stable pvst add tag extra int",
                    expectErrInfo=ERR_VST_HAS_CHILD)
        tdSql.execute("ROLLBACK")
        assert "extra" not in self._cols("pvst"), self._cols("pvst")

        # ADD COLUMN with a compress clause must be guarded exactly like plain ADD
        # COLUMN — the clause is only codec metadata, the column is still structural.
        # It used to be missing from the guard set, so this slipped through and left
        # the parent holding a column its children did not have. Run inside a fresh
        # txn so the guard is exercised on the in-txn (accumulated-schema) path, the
        # same path a real regression would surface on.
        tdSql.execute("BEGIN")
        tdSql.error("alter stable pvst add column plain int",
                    expectErrInfo=ERR_VST_HAS_CHILD)
        tdSql.error("alter stable pvst add column enc int "
                    "encode 'simple8b' compress 'lz4' level 'medium'",
                    expectErrInfo=ERR_VST_HAS_CHILD)
        tdSql.execute("ROLLBACK")
        pcols = self._cols("pvst")
        assert "plain" not in pcols and "enc" not in pcols, pcols
        # Parent and child stay in sync: no orphan column on either side.
        assert "enc" not in self._cols("cvst"), self._cols("cvst")

        s2.close()
        self._drop_inheritance("cvst", ["pvst2", "pvst"])
        tdLog.info("m12 PASSED: ADD/DROP BASE ON isolation/rollback/commit, "
                   "parent-with-children guard holds in txn")

    # =========================================================================
    # Test entry
    # =========================================================================

    def test_meta_batch_txn_op_matrix(self):
        """Batch meta txn: full ALTER action matrix inside a transaction.

        Verifies, each through in-txn visibility / ROLLBACK / COMMIT. Numbers match the
        m<N> case names, so a gap here means a genuinely missing case:
        m1.  MODIFY COLUMN (widen varchar) on super table and normal table
        m2.  MODIFY TAG (widen tag) on super table
        m3.  RENAME COLUMN on super table (new this release); child tables follow and
             data stays readable since colId is preserved
        m4.  ADD COLUMN with ENCODE/COMPRESS/LEVEL, then MODIFY that column's compression
             within the same txn
        m5.  MODIFY COLUMN compress/level on a column that predates the txn, so ROLLBACK
             restores the old codec instead of dropping the column
        m6.  Table options: TTL on a normal table, COMMENT on both normal and super table
        m7.  Virtual table column references: ADD COLUMN .. FROM, ALTER COLUMN .. SET ref,
             ALTER COLUMN .. SET NULL
        m8.  Virtual normal table tag references: ADD TAG .. FROM, ADD TAG (owned),
             SET TAG = literal, SET TAG = ref, DROP TAG
        m9.  Six different ALTER actions on one super table in a single txn: the schema
             observed in-txn must be exactly what COMMIT publishes (guards against the
             statement-time and COMMIT-time action sets drifting apart)
        m10. CREATE INDEX / DROP INDEX rejected inside a txn without poisoning it, and
             still working outside
        m11. DROP TAG / RENAME TAG cascade onto an existing tag index: applied on COMMIT
             (regression — the COMMIT path used to leave a dangling index object),
             untouched by ROLLBACK
        m12. ADD/DROP BASE ON virtual super table inheritance in a txn (regression — a
             groupId-0 redo action broke COMMIT under TRN_EXEC_GROUP_PARALLEL), plus the
             parent-with-children guard inside a txn

        Since: v3.3.6.0

        Labels: common,ci
        """
        self.m1_modify_column_bytes()
        self.m2_modify_tag_bytes()
        self.m3_rename_column_stb()
        self.m4_column_compression_options()
        self.m5_modify_column_compress()
        self.m6_table_options()
        self.m7_virtual_column_refs()
        self.m8_virtual_tag_refs()
        self.m9_mixed_actions_one_txn()
        self.m10_index_ddl_rejected()
        self.m11_tag_index_cascade_in_txn()
        self.m12_base_on_in_txn()

    @classmethod
    def teardown_class(cls):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("drop database if exists txn_mx_db")
        tdLog.debug("%s teardown done" % __file__)
