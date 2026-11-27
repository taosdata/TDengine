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
"""tag-ref sources extended to virtual-normal (vntb) and normal (ntb) tables.


Feature: a virtual table's tag-ref (`FROM db.tb.tag`) may now point at the tag of a
VIRTUAL NORMAL table or a plain NORMAL table, in addition to the pre-existing
child / virtual-child sources. Because a vntb tag may itself be a tag-ref, chains
resolve recursively to the physical terminal and the value follows the terminal
dynamically (a1.loc FROM b1.loc, b1.loc FROM c1.city  →  a1.loc == c1.city, and a
later SET TAG on c1 or a re-pointing on b1 is reflected on the next query).

Sections:
  1. Creation       — tag-ref from ntb-owned / vntb-owned / vntb-ref tags
  2. Chains         — two-hop and three-hop resolution, terminal type matrix
  3. Value follow   — terminal value change and chain re-pointing (SET TAG)
  4. Query paths    — projection, WHERE const filter, COUNT+WHERE, aggregates,
                      PARTITION BY tag, mixed with owned tags
  5. Presentation   — DESC / SHOW CREATE round-trip / SHOW TAGS / ins_tags
  6. DDL entries    — CREATE TAGS(... FROM ...), ALTER ADD TAG ... FROM, SET TAG =
  7. Negative       — type mismatch / missing tag / data-column ref / stb source /
                      circular via SET TAG / over-depth chain
  8. Boundaries     — dropped source fails fast at parse time (table not exist)
  9. VCTB hop       — chain through a virtual child: active ref wins over the stale
                      literal metaAlterTagRef leaves in ctbEntry.pTags (local + remote)
  10. Persistence   — chain and values survive a taosd restart
"""

import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes

DB = "td_trvns"
TS0 = 1700000000000
TSDB_CODE_VTABLE_CIRCULAR_REF = -2147458546
TSDB_CODE_PAR_TABLE_NOT_EXIST = -2147473917

SRC = {
    'c1': {'city': 'beijing', 'code': 100, 'data': [1, 2, 3]},
    'c2': {'city': 'shanghai', 'code': 200, 'data': [10, 20, 30]},
}


class TestTagRefVntbSource:
    """tag-ref with vntb/ntb sources: creation, chains, follow, queries, negatives."""

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    @classmethod
    def setup_class(cls):
        tdLog.info("=== setup: sources + chained virtual tables ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdSql.execute(f"CREATE DATABASE {DB} BUFFER 16;")
        tdSql.execute(f"USE {DB};")

        # physical child tables (terminal of most chains)
        tdSql.execute("CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
                      "TAGS (city NCHAR(20), code INT);")
        for tbl, info in SRC.items():
            tdSql.execute(f"CREATE TABLE {tbl} USING src_stb "
                          f"TAGS ('{info['city']}', {info['code']});")
            for j, v in enumerate(info['data']):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({TS0 + j * 1000}, {v});")

        # normal table with owned tags (ntb source)
        tdSql.execute("CREATE TABLE n1 (ts TIMESTAMP, v INT) "
                      "TAGS (loc NCHAR(16) = 'hangzhou', num INT = 42);")
        for j, v in enumerate([7, 8, 9]):
            tdSql.execute(f"INSERT INTO n1 VALUES ({TS0 + j * 1000}, {v});")

        # virtual normal tables: b1 has a ref tag, b2 has an owned tag
        tdSql.execute("CREATE VTABLE b1 (ts TIMESTAMP, val INT FROM c1.val) "
                      "TAGS (loc NCHAR(20) FROM c1.city);")
        tdSql.execute("CREATE VTABLE b2 (ts TIMESTAMP, val INT FROM c2.val) "
                      "TAGS (cnt INT = 7);")

        # chained virtual tables over vntb/ntb sources (the feature under test)
        tdSql.execute("CREATE VTABLE a_ref (ts TIMESTAMP, vv INT FROM b1.val) "
                      "TAGS (t_loc NCHAR(20) FROM b1.loc);")          # → c1.city (2 hops)
        tdSql.execute("CREATE VTABLE a_own (ts TIMESTAMP, val INT FROM b2.val) "
                      "TAGS (t_cnt INT FROM b2.cnt);")                # → 7 (vntb owned)
        tdSql.execute("CREATE VTABLE a_ntb (ts TIMESTAMP, v INT FROM n1.v) "
                      "TAGS (t_loc NCHAR(16) FROM n1.loc, t_num INT FROM n1.num);")  # ntb owned
        tdSql.execute("CREATE VTABLE a_mix (ts TIMESTAMP, vv INT FROM b1.val) "
                      "TAGS (t_loc NCHAR(20) FROM b1.loc, lit INT = 5);")  # ref + owned mix
        tdLog.info("=== setup complete ===")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _rows(self, sql):
        tdSql.query(sql)
        return [tuple(str(tdSql.getData(i, j)) for j in range(tdSql.queryCols))
                for i in range(tdSql.queryRows)]

    def _one(self, sql, desc=""):
        rows = self._rows(sql)
        assert len(rows) >= 1, f"{desc}: no rows"
        first = rows[0]
        for r in rows[1:]:
            assert r == first, f"{desc}: rows disagree: {rows}"
        tdLog.info(f"  PASS: {desc} -> {first} (x{len(rows)})")
        return first

    def _check_values(self, sql, expected, desc=""):
        actual = sorted(self._rows(sql))
        exp = sorted(tuple(str(v) for v in r) for r in expected)
        assert actual == exp, f"{desc}: mismatch\n  expected {exp}\n  actual   {actual}"
        tdLog.info(f"  PASS: {desc}")

    def _check_count(self, sql, expected_count, desc=""):
        tdSql.query(sql)
        # COUNT always returns one aggregate row, but guard explicitly: an empty
        # result here would make getData(0, 0) fail with an index error that masks
        # the real assertion below
        assert tdSql.queryRows >= 1, f"{desc}: COUNT query returned no rows, sql={sql}"
        actual = tdSql.getData(0, 0)
        assert int(actual) == expected_count, f"{desc}: expected {expected_count}, got {actual}"
        tdLog.info(f"  PASS: {desc} (count={expected_count})")

    # ==================================================================
    # 1. CREATION — tag-ref from ntb-owned / vntb-owned / vntb-ref tags
    # ==================================================================

    def test_create_from_vntb_ref_tag(self):
        """Creation: tag-ref pointing at a vntb's own tag-ref (b1.loc) succeeds and resolves

        through to the physical terminal (c1.city).

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "vntb-ref source")
        assert row[0] == "beijing", f"a_ref.loc should follow chain to c1.city, got {row[0]}"

    def test_create_from_vntb_owned_tag(self):
        """Creation: tag-ref pointing at a vntb's owned tag (b2.cnt) resolves to its literal.

        Verifies: Creation: tag-ref pointing at a vntb's owned tag (b2.cnt) resolves to its literal

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        row = self._one(f"SELECT t_cnt FROM {DB}.a_own;", "vntb-owned source")
        assert row[0] == "7", f"a_own.cnt should be 7 (b2.cnt owned), got {row[0]}"

    def test_create_from_ntb_owned_tag(self):
        """Creation: tag-ref pointing at a normal table's owned tags (n1.loc / n1.num).

        Verifies: Creation: tag-ref pointing at a normal table's owned tags (n1.loc / n1.num)

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        row = self._one(f"SELECT t_loc, t_num FROM {DB}.a_ntb;", "ntb-owned source")
        assert row[0] == "hangzhou" and row[1] == "42", f"a_ntb tags wrong: {row}"

    # ==================================================================
    # 2. CHAINS — hop depth and terminal type matrix
    # ==================================================================

    def test_chain_three_hop(self):
        """Chains: a3 → a_ref(ref) → b1(ref) → c1.city resolves across three hops.

        Verifies: Chains: a3 → a_ref(ref) → b1(ref) → c1.city resolves across three hops

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"CREATE VTABLE a3 (ts TIMESTAMP, vv INT FROM a_ref.vv) "
                      f"TAGS (t3_loc NCHAR(20) FROM a_ref.t_loc);")
        row = self._one(f"SELECT t3_loc FROM {DB}.a3;", "3-hop chain")
        assert row[0] == "beijing", f"a3.loc should reach c1.city through 3 hops, got {row[0]}"
        tdSql.execute(f"DROP VTABLE a3;")

    def test_chain_terminal_owned_to_ref(self):
        """Chains: terminal switch — b2.cnt converted owned→ref, a_own follows the new chain.

        Verifies: Chains: terminal switch — b2.cnt converted owned→ref, a_own follows the new chain

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        # b2.cnt: owned(7) → ref(c2.code=200)
        try:
            tdSql.execute(f"ALTER TABLE b2 SET TAG cnt = {DB}.c2.code;")
            row = self._one(f"SELECT t_cnt FROM {DB}.a_own;", "terminal owned→ref")
            assert row[0] == "200", f"a_own.cnt should follow to c2.code=200, got {row[0]}"
        finally:
            # restore for later sections; guarded so a restore failure cannot mask the
            # original assertion exception (same pattern as midchain_repoint)
            try:
                tdSql.execute(f"ALTER TABLE b2 SET TAG cnt = 7;")
            except Exception as e:
                tdLog.warning(f"restore b2.cnt literal failed (chain state may leak to later "
                              f"cases): {e}")
        row = self._one(f"SELECT t_cnt FROM {DB}.a_own;", "terminal restored")
        assert row[0] == "7"

    # ==================================================================
    # 3. VALUE FOLLOW — terminal change and chain re-pointing
    # ==================================================================

    def test_value_follows_terminal_set_tag(self):
        """Follow: SET TAG on the physical terminal (c1.city) is reflected through the chain.

        Verifies: Follow: SET TAG on the physical terminal (c1.city) is reflected through the chain

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        try:
            tdSql.execute(f"ALTER TABLE c1 SET TAG city='shenzhen';")
            row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "follow terminal change")
            assert row[0] == "shenzhen", f"a_ref.loc should follow c1.city change, got {row[0]}"
        finally:
            try:
                tdSql.execute(f"ALTER TABLE c1 SET TAG city='beijing';")
            except Exception as e:
                tdLog.warning(f"restore c1.city failed (state may leak to later cases): {e}")
        row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "restored")
        assert row[0] == "beijing"

    def test_value_follows_midchain_repoint(self):
        """Follow: re-pointing a mid-chain tag (b1.loc → c2.city) redirects the whole chain.

        Verifies: Follow: re-pointing a mid-chain tag (b1.loc → c2.city) redirects the whole chain

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        try:
            tdSql.execute(f"ALTER TABLE b1 SET TAG loc = {DB}.c2.city;")
            row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "mid-chain re-point")
            assert row[0] == "shanghai", f"a_ref.loc should follow b1 re-point to c2.city, got {row[0]}"
            # ref→owned conversion: b1.loc becomes a literal, chain collapses to it
            tdSql.execute(f"ALTER TABLE b1 SET TAG loc = 'nanjing';")
            row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "ref→owned collapse")
            assert row[0] == "nanjing", f"a_ref.loc should read b1's literal, got {row[0]}"
        finally:
            # restore the original chain; a restore failure must not mask the original
            # assertion exception -- log it and let the original error propagate
            try:
                tdSql.execute(f"ALTER TABLE b1 SET TAG loc = {DB}.c1.city;")
            except Exception as e:
                tdLog.warning(f"restore b1.loc ref failed (chain state may leak to later "
                              f"cases): {e}")
        row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "chain restored")
        assert row[0] == "beijing"

    # ==================================================================
    # 4. QUERY PATHS — projection / filter / aggregates / partition
    # ==================================================================

    def test_query_projection_with_data(self):
        """Query: const-ref tag projects alongside data columns from the source.

        Verifies: Query: const-ref tag projects alongside data columns from the source

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, query

        Jira: None

        History:
            - 2026-09-16 Created
        """
        self._check_values(f"SELECT t_loc, vv FROM {DB}.a_ref;",
                           [("beijing", "1"), ("beijing", "2"), ("beijing", "3")],
                           "projection with data")

    def test_query_where_const_filter(self):
        """Query: WHERE on a const-ref tag filters as a table-level constant.

        Verifies: Query: WHERE on a const-ref tag filters as a table-level constant

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, query

        Jira: None

        History:
            - 2026-09-16 Created
        """
        self._check_count(f"SELECT COUNT(*) FROM {DB}.a_ref WHERE t_loc='beijing';", 3,
                          "COUNT+WHERE hit")
        self._check_count(f"SELECT COUNT(*) FROM {DB}.a_ref WHERE t_loc='nowhere';", 0,
                          "COUNT+WHERE miss")

    def test_query_where_on_ntb_source(self):
        """Query: WHERE on a tag resolved from an ntb source.

        Verifies: Query: WHERE on a tag resolved from an ntb source

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, query

        Jira: None

        History:
            - 2026-09-16 Created
        """
        self._check_count(f"SELECT COUNT(*) FROM {DB}.a_ntb WHERE t_loc='hangzhou';", 3,
                          "ntb-source filter hit")

    def test_query_aggregate_and_distinct(self):
        """Query: aggregates and DISTINCT over a const-ref tag collapse to one value.

        Verifies: Query: aggregates and DISTINCT over a const-ref tag collapse to one value

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, query

        Jira: None

        History:
            - 2026-09-16 Created
        """
        self._one(f"SELECT DISTINCT t_loc FROM {DB}.a_ref;", "distinct")
        self._check_count(f"SELECT COUNT(t_loc) FROM {DB}.a_mix WHERE t_loc='beijing';", 3,
                          "COUNT with mixed owned+const-ref")

    def test_query_partition_by_const_ref(self):
        """Query: PARTITION BY on a const-ref tag groups all rows under one value.

        Verifies: Query: PARTITION BY on a const-ref tag groups all rows under one value

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, query

        Jira: None

        History:
            - 2026-09-16 Created
        """
        self._check_values(f"SELECT t_loc, COUNT(*) FROM {DB}.a_mix PARTITION BY t_loc;",
                           [("beijing", "3")], "partition by const-ref")

    # ==================================================================
    # 5. PRESENTATION — DESC / SHOW CREATE / SHOW TAGS / ins_tags
    # ==================================================================

    def test_desc_shows_tag(self):
        """Presentation: DESC lists the const-ref tag with its declared type.

        Verifies: Presentation: DESC lists the const-ref tag with its declared type

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, meta

        Jira: None

        History:
            - 2026-09-16 Created
        """
        rows = self._rows(f"DESC {DB}.a_ref;")
        names = [r[0] for r in rows]
        assert "t_loc" in names, f"DESC missing const-ref tag: {rows}"

    def test_show_create_roundtrip(self):
        """Presentation: SHOW CREATE emits one-level `FROM db.b1.loc` and replays.

        Verifies: Presentation: SHOW CREATE emits one-level `FROM db.b1.loc` and replays

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, meta

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.query(f"SHOW CREATE TABLE {DB}.a_ref;")
        create_sql = str(tdSql.getData(0, 1))
        assert "FROM" in create_sql and "b1" in create_sql, f"unexpected SHOW CREATE: {create_sql}"
        # the table name must occur exactly once: a blind str.replace would rewrite every
        # match, silently pointing the replay at a wrong table if some other identifier
        # contained the substring
        assert create_sql.count("a_ref") == 1, \
            f"expected exactly one a_ref occurrence in SHOW CREATE, got {create_sql.count('a_ref')}: {create_sql}"
        # true replay: execute the emitted DDL under a fresh name — a defective SHOW CREATE
        # (missing db prefix, wrong type, ...) fails here instead of passing a substring check
        try:
            tdSql.execute(create_sql.replace("a_ref", "a_rt"))
            row = self._one(f"SELECT t_loc FROM {DB}.a_rt;", "replayed table")
            assert row[0] == "beijing", f"replayed table tag wrong: {row[0]}"
        finally:
            try:
                tdSql.execute("DROP VTABLE a_rt;")
            except Exception:
                pass  # replay failed before a_rt was created

    def test_show_tags_and_ins_tags(self):
        """Presentation: SHOW TAGS and ins_tags expose the resolved terminal value.

        Verifies: Presentation: SHOW TAGS and ins_tags expose the resolved terminal value

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, meta

        Jira: None

        History:
            - 2026-09-16 Created
        """
        rows = self._rows(f"SHOW TAGS FROM {DB}.a_ref;")
        vals = [r for r in rows if "t_loc" in r]
        assert vals and "beijing" in vals[0], f"SHOW TAGS wrong: {rows}"
        rows = self._rows(f"SELECT tag_name, tag_value FROM information_schema.ins_tags "
                          f"WHERE db_name='{DB}' AND table_name='a_ref';")
        vals = [r for r in rows if r[0] == "t_loc"]
        assert vals and vals[0][1] == "beijing", f"ins_tags wrong: {rows}"

    # ==================================================================
    # 6. DDL ENTRIES — ADD TAG ... FROM and SET TAG =
    # ==================================================================

    def test_alter_add_tag_from_vntb(self):
        """DDL: ALTER TABLE ... ADD TAG ... FROM accepts a vntb source tag.

        Verifies: DDL: ALTER TABLE ... ADD TAG ... FROM accepts a vntb source tag

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, ddl

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"CREATE VTABLE a_add (ts TIMESTAMP, val INT FROM b1.val) TAGS (lit INT = 1);")
        tdSql.execute(f"ALTER TABLE a_add ADD TAG city NCHAR(20) FROM {DB}.b1.loc;")
        try:
            row = self._one(f"SELECT city FROM {DB}.a_add;", "ADD TAG FROM vntb")
            assert row[0] == "beijing", f"a_add.city wrong: {row[0]}"
        finally:
            # cleanup must survive an assertion failure or a_add leaks to later cases
            try:
                tdSql.execute("DROP VTABLE a_add;")
            except Exception as e:
                tdLog.warning(f"drop a_add failed (leftover may affect later cases): {e}")

    def test_set_tag_entry(self):
        """DDL: SET TAG x = db.tb.tag accepts vntb/ntb sources (conversion to ref).

        Verifies: DDL: SET TAG x = db.tb.tag accepts vntb/ntb sources (conversion to ref)

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, ddl

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"CREATE VTABLE a_set (ts TIMESTAMP, val INT FROM b1.val) TAGS (l NCHAR(16) = 'x');")
        tdSql.execute(f"ALTER TABLE a_set SET TAG l = {DB}.b1.loc;")
        try:
            row = self._one(f"SELECT l FROM {DB}.a_set;", "SET TAG to vntb source")
            assert row[0] == "beijing", f"a_set.l wrong: {row[0]}"
        finally:
            # cleanup must survive an assertion failure or a_set leaks to later cases
            try:
                tdSql.execute("DROP VTABLE a_set;")
            except Exception as e:
                tdLog.warning(f"drop a_set failed (leftover may affect later cases): {e}")

    # ==================================================================
    # 7. NEGATIVE
    # ==================================================================

    def test_neg_type_mismatch(self):
        """Negative: declared tag type must match the source tag type.

        Verifies: Negative: declared tag type must match the source tag type

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, negative

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.error(f"CREATE VTABLE e1 (ts TIMESTAMP, val INT FROM b1.val) "
                    f"TAGS (n INT FROM b1.loc);")   # NCHAR source into INT tag

    def test_neg_missing_tag(self):
        """Negative: referencing a tag that does not exist on the source.

        Verifies: Negative: referencing a tag that does not exist on the source

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, negative

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.error(f"CREATE VTABLE e2 (ts TIMESTAMP, val INT FROM b1.val) "
                    f"TAGS (n NCHAR(20) FROM b1.nosuch);")

    def test_neg_data_column(self):
        """Negative: FROM must reference a tag, not a data column (n1.v).

        Verifies: Negative: FROM must reference a tag, not a data column (n1.v)

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, negative

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.error(f"CREATE VTABLE e3 (ts TIMESTAMP, v INT FROM n1.v) "
                    f"TAGS (n INT FROM n1.v);")

    def test_neg_super_table_source(self):
        """Negative: super-table tags remain rejected as tag-ref sources.

        Verifies: Negative: super-table tags remain rejected as tag-ref sources

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, negative

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.error(f"CREATE VTABLE e4 (ts TIMESTAMP, val INT FROM c1.val) "
                    f"TAGS (n NCHAR(20) FROM src_stb.city);")

    def test_neg_circular_via_set_tag(self):
        """Negative: SET TAG re-pointing that would close a cycle is rejected.

        Verifies: Negative: SET TAG re-pointing that would close a cycle is rejected

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, negative

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"CREATE VTABLE cy1 (ts TIMESTAMP, val INT FROM c1.val) TAGS (x NCHAR(20) = 'a');")
        tdSql.execute(f"CREATE VTABLE cy2 (ts TIMESTAMP, val INT FROM c1.val) TAGS (y NCHAR(20) = 'b');")
        try:
            tdSql.execute(f"ALTER TABLE cy1 SET TAG x = {DB}.cy2.y;")   # cy1 → cy2 (fine)
            # cy2 → cy1 would close the cycle cy1 → cy2 → cy1
            tdSql.error(f"ALTER TABLE cy2 SET TAG y = {DB}.cy1.x;",
                        expectedErrno=TSDB_CODE_VTABLE_CIRCULAR_REF)
            row = self._one(f"SELECT x FROM {DB}.cy1;", "chain intact after rejection")
            assert row[0] == "b", f"cy1.l should still follow cy2, got {row[0]}"
        finally:
            # cleanup must run even when the circular-detection assertion regresses,
            # or leftover cy1/cy2 leak into later cases sharing this DB
            for stmt in ("DROP VTABLE cy1;", "DROP VTABLE cy2;"):
                try:
                    tdSql.execute(stmt)
                except Exception as e:
                    tdLog.warning(f"{stmt} failed (leftover may affect later cases): {e}")


    # ==================================================================
    # 8. BOUNDARIES — dropped source
    # ==================================================================

    def test_drop_source_fails_fast(self):
        """Boundary: dropping the source table makes the query fail fast at translation time.

        Verifies: Boundary: dropping the source table makes the query fail fast at translation time

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, boundary

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"CREATE TABLE n2 (ts TIMESTAMP, v INT) TAGS (loc NCHAR(16) = 'x');")
        tdSql.execute(f"INSERT INTO n2 VALUES ({TS0}, 1), ({TS0 + 1000}, 2);")
        # data flows from a surviving source; only the TAG chain breaks with n2
        tdSql.execute(f"CREATE VTABLE a_drop (ts TIMESTAMP, vv INT FROM c1.val) "
                      f"TAGS (t_loc NCHAR(16) FROM n2.loc);")
        row = self._one(f"SELECT t_loc FROM {DB}.a_drop;", "before drop")
        assert row[0] == "x"
        tdSql.execute("DROP TABLE n2;")
        # Current design: the translator pre-fetches tag-ref source tables when planning the
        # query, so a source dropped before the query fails fast at parse time (table not
        # exist) instead of resolving the tag to NULL. Runtime NULL tolerance covers sources
        # that vanish between planning and execution.
        try:
            # pin the expected error: any other failure (syntax, connection) must not
            # masquerade as the fails-fast boundary
            tdSql.error(f"SELECT t_loc FROM {DB}.a_drop;",
                        expectedErrno=TSDB_CODE_PAR_TABLE_NOT_EXIST)
        finally:
            # cleanup must run even when the fails-fast assertion regresses, or the
            # leftover a_drop leaks into later cases sharing this DB
            try:
                tdSql.execute("DROP VTABLE a_drop;")
            except Exception as e:
                tdLog.warning(f"drop a_drop failed (leftover may affect later cases): {e}")

    # ==================================================================
    # 9. VCTB HOP — active ref on a virtual child beats its stale literal
    # ==================================================================
    # metaAlterTagRef deliberately leaves a virtual child's pre-ref literal in
    # ctbEntry.pTags (query-time resolution must override it). A chain hop landing on
    # such a vctb must follow the active tag-ref, never read the stale literal back —
    # for the local meta branch and for the remote table-cfg branch alike. Both tests
    # below plant 'stale*' literals that are WRONG answers, so a regression to
    # value-first resolution fails deterministically instead of passing vacuously.

    def _make_vctb_chain(self, db, vctb, ref_target):
        """vstb(vctb.g literal) -> SET TAG g = ref_target; returns nothing.

        The vctb keeps its stale literal in ctbEntry.pTags after the SET TAG — the
        exact state metaAlterTagRef produces for virtual child tables.
        """
        # idempotent on purpose: a vc_stb left behind by an earlier case's failed
        # mid-setup would otherwise make this CREATE fail with a name clash and cascade
        tdSql.execute(f"DROP STABLE IF EXISTS {db}.vc_stb;")
        tdSql.execute(f"CREATE STABLE {db}.vc_stb (ts TIMESTAMP, val INT) "
                      f"TAGS (g NCHAR(20)) VIRTUAL 1;")
        # the data column tracks the table the REF TARGET's tag lives on (ref_target
        # stripped of its trailing column, e.g. 'db.c1.city' -> 'db.c1'). Callers today
        # pass a single-hop target, so that table coincides with the chain terminal --
        # but this helper does NOT walk the chain: a multi-hop ref_target would bind the
        # data column to the first hop's table, not the terminal
        tdSql.execute(f"CREATE VTABLE {db}.{vctb} (val FROM {ref_target.rsplit('.', 1)[0]}.val) "
                      f"USING {db}.vc_stb TAGS ('stale_city');")
        tdSql.execute(f"ALTER TABLE {db}.{vctb} SET TAG g = {ref_target};")

    def _drop_vctb_chain(self, db, vctb):
        for stmt in (f"DROP VTABLE {db}.{vctb};", f"DROP STABLE {db}.vc_stb;"):
            try:
                tdSql.execute(stmt)
            except Exception as e:
                # visible, not silent: a leftover vc_stb/vctb leaks into later cases
                # sharing the fixed stable name and fails them in confusing ways
                tdLog.warning(f"{stmt} failed (leftover may affect later cases): {e}")  # earlier failure may have removed it already

    def test_vctb_hop_local_chain(self):
        """VCTB hop: a_vc → b_vc(vntb) → vc_mid(vctb, stale literal + active ref) → c1.city.

        Verifies: VCTB hop: a_vc → b_vc(vntb) → vc_mid(vctb, stale literal + active ref) → c1.city

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        self._make_vctb_chain(DB, "vc_mid", f"{DB}.c1.city")
        try:
            tdSql.execute("CREATE VTABLE b_vc (ts TIMESTAMP, val INT FROM c1.val) "
                          "TAGS (loc NCHAR(20) FROM vc_mid.g);")
            tdSql.execute("CREATE VTABLE a_vc (ts TIMESTAMP, vv INT FROM b_vc.val) "
                          "TAGS (t_loc NCHAR(20) FROM b_vc.loc);")
            try:
                row = self._one(f"SELECT t_loc FROM {DB}.a_vc;", "vctb hop: ref beats stale literal")
                assert row[0] == "beijing", \
                    f"must follow vc_mid.g's ref to c1.city, not the stale literal; got {row[0]}"
                # WHERE / aggregate / partition paths over the same hop
                self._check_count(f"SELECT COUNT(*) FROM {DB}.a_vc WHERE t_loc='beijing';", 3,
                                  "WHERE via vctb hop")
                self._check_values(f"SELECT t_loc, COUNT(*) FROM {DB}.a_vc PARTITION BY t_loc;",
                                   [("beijing", "3")], "PARTITION BY via vctb hop")
                # presentation resolves through the same chain walker
                rows = self._rows(f"SELECT tag_name, tag_value FROM information_schema.ins_tags "
                                  f"WHERE db_name='{DB}' AND table_name='a_vc';")
                vals = [r for r in rows if r[0] == "t_loc"]
                assert vals and vals[0][1] == "beijing", f"ins_tags via vctb hop wrong: {rows}"
            finally:
                for stmt in ("DROP VTABLE a_vc;", "DROP VTABLE b_vc;"):
                    try:
                        tdSql.execute(stmt)
                    except Exception as e:
                        tdLog.warning(f"{stmt} failed (leftover may affect later cases): {e}")
        finally:
            self._drop_vctb_chain(DB, "vc_mid")

    def test_vctb_hop_value_follow(self):
        """VCTB hop: terminal SET TAG / mid-chain re-point / ref→literal all follow, no stale read.

        Verifies: VCTB hop: terminal SET TAG / mid-chain re-point / ref→literal all follow, no stale read

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        self._make_vctb_chain(DB, "vc_f", f"{DB}.c1.city")
        try:
            tdSql.execute("CREATE VTABLE b_vf (ts TIMESTAMP, val INT FROM c1.val) "
                          "TAGS (loc NCHAR(20) FROM vc_f.g);")
            tdSql.execute("CREATE VTABLE a_vf (ts TIMESTAMP, vv INT FROM b_vf.val) "
                          "TAGS (t_loc NCHAR(20) FROM b_vf.loc);")
            try:
                # terminal SET TAG follows through the vctb hop
                tdSql.execute("ALTER TABLE c1 SET TAG city='shenzhen';")
                row = self._one(f"SELECT t_loc FROM {DB}.a_vf;", "terminal change through vctb hop")
                assert row[0] == "shenzhen", f"terminal change not followed, got {row[0]}"
                tdSql.execute("ALTER TABLE c1 SET TAG city='beijing';")
                # mid-chain re-point on the vctb itself
                tdSql.execute(f"ALTER TABLE vc_f SET TAG g = {DB}.c2.city;")
                row = self._one(f"SELECT t_loc FROM {DB}.a_vf;", "vctb re-point")
                assert row[0] == "shanghai", f"vctb re-point not followed, got {row[0]}"
                # ref→literal: the ref clears and the CURRENT literal (not the stale one)
                # is the terminal
                tdSql.execute("ALTER TABLE vc_f SET TAG g = 'nanjing';")
                row = self._one(f"SELECT t_loc FROM {DB}.a_vf;", "vctb ref→literal")
                assert row[0] == "nanjing", f"current literal not read, got {row[0]}"
            finally:
                try:
                    tdSql.execute("ALTER TABLE c1 SET TAG city='beijing';")
                except Exception:
                    pass
                for stmt in ("DROP VTABLE a_vf;", "DROP VTABLE b_vf;"):
                    try:
                        tdSql.execute(stmt)
                    except Exception:
                        pass
        finally:
            self._drop_vctb_chain(DB, "vc_f")

    def test_vctb_hop_remote_chain(self):
        """VCTB hop (remote cfg branch): chain crosses into another db's vgroup mid-way.

        Verifies: VCTB hop (remote cfg branch): chain crosses into another db's vgroup mid-way

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref

        Jira: None

        History:
            - 2026-09-16 Created
        """
        DB2 = "td_trvns_r"
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB2};")
        tdSql.execute(f"CREATE DATABASE {DB2} BUFFER 16;")
        try:
            # terminal child table + vctb both live in DB2 (a different vgroup from DB)
            tdSql.execute(f"CREATE STABLE {DB2}.r_stb (ts TIMESTAMP, val INT) TAGS (city NCHAR(20));")
            tdSql.execute(f"CREATE TABLE {DB2}.rc1 USING {DB2}.r_stb TAGS ('xian');")
            for j, v in enumerate([4, 5, 6]):
                tdSql.execute(f"INSERT INTO {DB2}.rc1 VALUES ({TS0 + j * 1000}, {v});")
            self._make_vctb_chain(DB2, "vc_r", f"{DB2}.rc1.city")
            # const-ref entry in DB; hop 2 (vc_r) and hop 3 (rc1) resolve via remote cfg
            tdSql.execute("CREATE VTABLE b_vr (ts TIMESTAMP, val INT FROM c1.val) "
                          f"TAGS (loc NCHAR(20) FROM {DB2}.vc_r.g);")
            tdSql.execute("CREATE VTABLE a_vr (ts TIMESTAMP, vv INT FROM b_vr.val) "
                          "TAGS (t_loc NCHAR(20) FROM b_vr.loc);")
            try:
                row = self._one(f"SELECT t_loc FROM {DB}.a_vr;", "remote vctb hop: ref beats stale")
                assert row[0] == "xian", \
                    f"remote cfg must follow vc_r.g's ref to rc1.city, not the stale literal; got {row[0]}"
                self._check_count(f"SELECT COUNT(*) FROM {DB}.a_vr WHERE t_loc='xian';", 3,
                                  "WHERE via remote vctb hop")
                # terminal SET TAG across the vgroup boundary
                tdSql.execute(f"ALTER TABLE {DB2}.rc1 SET TAG city='yantai';")
                row = self._one(f"SELECT t_loc FROM {DB}.a_vr;", "remote terminal change")
                assert row[0] == "yantai", f"remote terminal change not followed, got {row[0]}"
            finally:
                for stmt in ("DROP VTABLE a_vr;", "DROP VTABLE b_vr;"):
                    try:
                        tdSql.execute(stmt)
                    except Exception:
                        pass
                self._drop_vctb_chain(DB2, "vc_r")
        finally:
            # cleanup must not mask a real assertion failure above, and a leftover DB2
            # would pollute later cases -- log and continue instead of raising
            try:
                tdSql.execute(f"DROP DATABASE IF EXISTS {DB2};")
            except Exception as e:
                tdLog.warning(f"drop DB2 {DB2} failed (leftover may affect later cases): {e}")

    # ==================================================================
    # 10. PERSISTENCE
    # ==================================================================

    def test_persistence_restart(self):
        """Persistence: chain refs and resolved values survive a taosd restart.

        Verifies: Persistence: chain refs and resolved values survive a taosd restart

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, persistence

        Jira: None

        History:
            - 2026-09-16 Created
        """
        try:
            tdDnodes.stop(1)
            tdDnodes.start(1)
        except Exception as e:
            # best-effort bring-back: leaving dnode1 stopped would cascade-fail the
            # class teardown and every later class, masking the real restart failure
            try:
                tdDnodes.start(1)
            except Exception as e2:
                tdLog.warning(f"[trvns-persistence] bring-back start also failed: {e2}")
            raise AssertionError(f"[trvns-persistence] dnode restart failed: {e}") from e
        deadline = time.time() + 30
        last_state = "no response"
        ready = False
        while time.time() < deadline:
            try:
                tdSql.query("SELECT SERVER_STATUS();")
                # server process up does not imply this db's vgroups finished recovery /
                # leader election — only accept readiness once a real read on a chain
                # table succeeds AND actually returns the persisted row: an empty result
                # set would mean recovery is still in progress, and proceeding would
                # misreport "not ready" as "persistence lost" in the asserts below
                # truthiness instead of == 1: the driver may deliver SERVER_STATUS as int or "1"
                if tdSql.queryResult and tdSql.queryResult[0][0]:
                    tdSql.query(f"SELECT t_loc FROM {DB}.a_ref;")
                    if tdSql.queryRows >= 1:
                        ready = True
                        break
                    last_state = "server up, chain table not recovered yet (0 rows)"
                else:
                    last_state = f"server_status={tdSql.queryResult[0][0] if tdSql.queryResult else 'empty'}"
            except Exception as e:
                last_state = f"{type(e).__name__}: {e}"
            time.sleep(0.5)
        if not ready:
            raise AssertionError(
                f"[trvns-persistence] dnode/db not ready 30s after restart, last state: {last_state}")

        row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "after restart")
        assert row[0] == "beijing", f"chain value lost after restart: {row[0]}"
        row = self._one(f"SELECT t_loc, t_num FROM {DB}.a_ntb;", "ntb after restart")
        assert row[0] == "hangzhou" and row[1] == "42"

    def test_cross_db_same_name_shadowing(self):
        """Correctness: a cross-db hop is not hijacked by a same-named table on the local vnode.

        The chain hop targets DB2.src1 explicitly, while the home db also has a same-named
        src1 with the same tag name but a different value. Meta lookups are by bare table
        name, so without the db-ownership guard in the local hop the LOCAL same-named table
        would be read and resolve to the wrong value; the guard must route the hop to the
        remote cfg fetch that resolves by full db-qualified name.

        Verifies: cross-db chain hop resolves by db-qualified name, not local bare name

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag-ref, cross-db

        Jira: None

        History:
            - 2026-09-16 Created
        """
        DB2 = "td_trvns_shadow"
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB2};")
        tdSql.execute(f"CREATE DATABASE {DB2} BUFFER 16;")
        try:
            # the intended cross-db source
            tdSql.execute(f"CREATE STABLE {DB2}.src_stb (ts TIMESTAMP, v INT) TAGS (city NCHAR(20));")
            tdSql.execute(f"CREATE TABLE {DB2}.src1 USING {DB2}.src_stb TAGS ('remote-real');")
            # the local same-named decoy: same table name, same tag name, WRONG value.
            # the HOME src_stb carries TWO tags (city + code) -- both must be supplied
            # or CREATE fails with "Tags number not matched" before any assertion runs
            tdSql.execute(f"CREATE TABLE {DB}.src1 USING {DB}.src_stb TAGS ('local-decoy', 0);")
            tdSql.execute(f"CREATE VTABLE x_sh (ts TIMESTAMP, val INT FROM {DB}.c1.val) "
                          f"TAGS (loc NCHAR(20) FROM {DB2}.src1.city);")
            row = self._one(f"SELECT loc FROM {DB}.x_sh;", "cross-db same-name shadowing")
            assert row[0] == "remote-real", \
                f"cross-db hop hijacked by same-named local table, got: {row[0]}"
            # the SELECT above resolves on the VirtualScan path (no local vnode handle,
            # always remote cfg); the guard's real target is the local-hop path used by
            # SHOW TAGS / ins_tags, which runs on the DB-owning vnode where the decoy
            # lives -- assert there too, or removing the guard would not fail this test
            # position-independent: match the row by tag_name, then take whatever
            # column holds the value -- robust against ins_tags column reordering
            rows = self._rows(f"SELECT * FROM information_schema.ins_tags "
                              f"WHERE db_name='{DB}' AND table_name='x_sh';")
            locs = [r for r in rows if "loc" in (str(c) for c in r)]
            assert locs and "remote-real" in (str(c) for c in locs[0]), \
                f"ins_tags local hop hijacked by decoy: {rows}"

        finally:
            for stmt in (f"DROP VTABLE {DB}.x_sh;", f"DROP TABLE IF EXISTS {DB}.src1;",
                         f"DROP DATABASE IF EXISTS {DB2};"):
                try:
                    tdSql.execute(stmt)
                except Exception:
                    pass

