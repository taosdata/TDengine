# -*- coding: utf-8 -*-

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

import pytest
import taos

from new_test_framework.utils import tdLog, tdSql, tdDnodes

# Error code constants (signed 32-bit representation)
TSDB_CODE_SUCCESS = 0
TSDB_CODE_PAR_TABLE_NOT_EXIST = -2147473917        # 0x80002603
TSDB_CODE_PAR_INVALID_REF_COLUMN = -2147473779     # 0x8000268D
TSDB_CODE_MND_DB_NOT_EXIST = -2147482744           # 0x80000388

DB_NAME = "test_vtable_errmsg"
SRC_DB_NAME = "test_vtable_errmsg_src"


class TestVtableValidateErrmsg:
    def setup_class(cls):
        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"drop database if exists {SRC_DB_NAME};")

    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"drop database if exists {SRC_DB_NAME};")

    def setup_method(self):
        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"drop database if exists {SRC_DB_NAME};")
        tdSql.execute(f"create database {DB_NAME};")
        tdSql.execute(f"use {DB_NAME};")

    @staticmethod
    def _query_err_rows(table_name, db_name=DB_NAME):
        tdSql.query(f"select virtual_col_name, err_code, err_msg "
                    f"from information_schema.ins_virtual_tables_referencing "
                    f"where virtual_db_name = '{db_name}' and virtual_table_name = '{table_name}';")
        return [(r[0], r[1], r[2]) for r in tdSql.queryResult]

    @staticmethod
    def _second_conn():
        # a fresh connection with its own catalog cache; the port follows the
        # deployed cluster (read from its taos.cfg) so the suite works on
        # non-default ports too
        dnode = tdDnodes.dnodes[0]
        port = 6030
        try:
            with open(dnode.cfgPath, "r") as f:
                for line in f:
                    parts = line.split()
                    if parts and parts[0] == "serverPort":
                        port = int(parts[1])
                        break
        except Exception as e:
            tdLog.warning(f"read serverPort from {dnode.cfgPath} failed ({e}), falling back to default {port}")
        return taos.connect(host=dnode.cfgDict.get("fqdn", "localhost"), port=port,
                            user="root", password="taosdata")

    # ------------------------------------------------------------------
    # Part 1: SHOW VTABLE VALIDATE err_msg enrichment
    # ------------------------------------------------------------------

    def test_errmsg_direct_source_table_drop(self):
        """err_msg names the dropped source table and the hop chain.

        1. create a source table and a vtable referencing it
        2. drop the source table
        3. check err_msg names the dropped table and the hop chain

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_direct (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE vntb_direct (ts timestamp, v_c1 int from src_direct.c1);")
        tdSql.execute(f"DROP TABLE src_direct;")

        rows = self._query_err_rows("vntb_direct")
        assert len(rows) == 1
        assert rows[0][0] == "v_c1"
        assert rows[0][1] == TSDB_CODE_PAR_TABLE_NOT_EXIST
        err_msg = rows[0][2]
        assert err_msg and str(err_msg).strip()
        tdLog.info(f"err_msg: {err_msg}")
        assert 'source table' in err_msg
        assert f'"{DB_NAME}.src_direct"' in err_msg
        assert "does not exist" in err_msg
        assert "(chain:" in err_msg
        assert "vntb_direct.v_c1" in err_msg
        assert f"{DB_NAME}.src_direct.c1" in err_msg

    def test_errmsg_direct_source_column_drop(self):
        """err_msg names the dropped source column.

        1. create a source table with two columns and a vtable referencing both
        2. drop one source column
        3. check err_msg names the missing column while the other ref stays ok

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_col (ts timestamp, c1 int, c2 float);")
        tdSql.execute(f"CREATE VTABLE vntb_col (ts timestamp, v_c1 int from src_col.c1, v_c2 float from src_col.c2);")
        tdSql.execute(f"ALTER TABLE src_col DROP COLUMN c1;")

        rows = self._query_err_rows("vntb_col")
        by_col = {r[0]: r for r in rows}
        assert by_col["v_c1"][1] == TSDB_CODE_PAR_INVALID_REF_COLUMN
        assert by_col["v_c2"][1] == TSDB_CODE_SUCCESS
        assert by_col["v_c2"][2] == "" or by_col["v_c2"][2] is None
        err_msg = by_col["v_c1"][2]
        tdLog.info(f"err_msg: {err_msg}")
        assert 'source column' in err_msg
        assert f'"{DB_NAME}.src_col.c1"' in err_msg
        assert 'not found' in err_msg
        assert "(chain:" in err_msg

    def test_errmsg_success_rows_empty_msg(self):
        """Successful refs keep an empty err_msg.

        1. create a healthy vtable with a valid ref
        2. query ins_virtual_tables_referencing
        3. check the success row keeps an empty err_msg

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_ok (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE vntb_ok (ts timestamp, v_c1 int from src_ok.c1);")

        rows = self._query_err_rows("vntb_ok")
        assert len(rows) == 1
        assert rows[0][0] == "v_c1"
        assert rows[0][1] == TSDB_CODE_SUCCESS
        assert rows[0][2] is None or str(rows[0][2]).strip() == ""

    def test_errmsg_three_hop_middle_drop(self):
        """err_msg names the dropped MIDDLE table of a 3-hop chain; the chain lists every hop.

        1. build a 3-hop vtable chain
        2. drop the middle virtual table
        3. check err_msg blames the middle table and the chain lists every hop in order

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_3hop (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE vntb_mid_3hop (ts timestamp, m1 int from src_3hop.c1);")
        tdSql.execute(f"CREATE VTABLE vntb_top_3hop (ts timestamp, t1 int from vntb_mid_3hop.m1);")
        tdSql.execute(f"CREATE VTABLE vntb_leaf_3hop (ts timestamp, l1 int from vntb_top_3hop.t1);")
        tdSql.execute(f"DROP TABLE vntb_mid_3hop;")

        rows = self._query_err_rows("vntb_leaf_3hop")
        assert len(rows) == 1
        col, code, err_msg = rows[0]
        tdLog.info(f"err_msg: {err_msg}")
        assert code == TSDB_CODE_PAR_TABLE_NOT_EXIST
        assert col == "l1"
        # the failure point is the dropped middle table, NOT the direct ref (top)
        assert 'source table' in err_msg
        assert f'"{DB_NAME}.vntb_mid_3hop"' in err_msg
        assert "(chain:" in err_msg
        # the chain must contain all three hops in order
        chain = err_msg.split("(chain:")[1]
        assert f"{DB_NAME}.vntb_leaf_3hop.l1" in chain
        assert f"{DB_NAME}.vntb_top_3hop.t1" in chain
        assert chain.index("vntb_leaf_3hop") < chain.index("vntb_top_3hop")
        assert "vntb_mid_3hop" in chain

    def test_errmsg_cross_db_three_hop_middle_drop(self):
        """Cross-db 3-hop chain: failure point and chain carry db-qualified hops.

        1. build a cross-database 3-hop chain
        2. drop the middle table in the other database
        3. check the failure point and chain carry db-qualified hops

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"create database {SRC_DB_NAME};")
        tdSql.execute(f"CREATE TABLE {SRC_DB_NAME}.src_x (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE {SRC_DB_NAME}.vntb_mid_x (ts timestamp, m1 int from {SRC_DB_NAME}.src_x.c1);")
        tdSql.execute(f"CREATE VTABLE vntb_leaf_x (ts timestamp, l1 int from {SRC_DB_NAME}.vntb_mid_x.m1);")
        tdSql.execute(f"DROP TABLE {SRC_DB_NAME}.vntb_mid_x;")

        rows = self._query_err_rows("vntb_leaf_x")
        assert len(rows) == 1
        _, code, err_msg = rows[0]
        tdLog.info(f"err_msg: {err_msg}")
        assert code == TSDB_CODE_PAR_TABLE_NOT_EXIST
        assert f'"{SRC_DB_NAME}.vntb_mid_x"' in err_msg
        chain = err_msg.split("(chain:")[1]
        assert f"{SRC_DB_NAME}.vntb_mid_x.m1" in chain

    def test_errmsg_source_db_drop(self):
        """err_msg names the dropped source database.

        1. create a vtable referencing a table in another database
        2. drop that database
        3. check err_msg names the dropped database

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"create database {SRC_DB_NAME};")
        tdSql.execute(f"CREATE TABLE {SRC_DB_NAME}.src_db (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE vntb_srcdb (ts timestamp, v_c1 int from {SRC_DB_NAME}.src_db.c1);")
        tdSql.execute(f"drop database {SRC_DB_NAME};")

        rows = self._query_err_rows("vntb_srcdb")
        assert len(rows) == 1
        _, code, err_msg = rows[0]
        tdLog.info(f"err_msg: {err_msg}")
        assert code == TSDB_CODE_MND_DB_NOT_EXIST
        assert 'source database' in err_msg
        assert f'"{SRC_DB_NAME}"' in err_msg
        assert "does not exist" in err_msg

    def test_errmsg_matches_show_vtable_validate(self):
        """SHOW VTABLE VALIDATE output equals the information_schema rows.

        1. break a vtable ref by dropping its source
        2. run SHOW VTABLE VALIDATE and the information_schema query
        3. check both outputs match

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_show (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE vntb_show (ts timestamp, v_c1 int from src_show.c1);")
        tdSql.execute(f"DROP TABLE src_show;")

        tdSql.query(f"SHOW VTABLE VALIDATE FOR vntb_show;")
        show_rows = sorted([(r[3], r[8], r[9]) for r in tdSql.queryResult])
        info_rows = sorted(self._query_err_rows("vntb_show"))
        assert show_rows == info_rows

    # ------------------------------------------------------------------
    # Part 2: query-path errors name the missing source table / column
    # ------------------------------------------------------------------

    def test_query_dropped_source_table(self):
        """SELECT on a vtable whose source table is dropped names the source table.

        1. drop the source table of a vtable
        2. select from the vtable
        3. check the error names the dropped source table

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_q (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_q VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE vntb_q (ts timestamp, v_c1 int from src_q.c1);")
        tdSql.query(f"select v_c1 from vntb_q;")
        tdSql.execute(f"DROP TABLE src_q;")
        tdSql.error(f"select v_c1 from vntb_q;",
                    expectErrInfo=f'source table "{DB_NAME}.src_q" does not exist',
                    fullMatched=False)

    def test_query_dropped_middle_virtual_table(self):
        """SELECT on a 3-hop chain whose middle table is dropped names the middle table.

        1. drop the middle virtual table of a 3-hop chain
        2. select from the leaf vtable
        3. check the error names the middle table

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_qm (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE vntb_qm_mid (ts timestamp, m1 int from src_qm.c1);")
        tdSql.execute(f"CREATE VTABLE vntb_qm_leaf (ts timestamp, l1 int from vntb_qm_mid.m1);")
        tdSql.query(f"select l1 from vntb_qm_leaf;")
        tdSql.execute(f"DROP TABLE vntb_qm_mid;")
        tdSql.error(f"select l1 from vntb_qm_leaf;",
                    expectErrInfo=f'source table "{DB_NAME}.vntb_qm_mid" does not exist',
                    fullMatched=False)

    def test_query_dropped_source_column(self):
        """SELECT on a vtable whose source column is dropped names the source column.

        1. drop a source column of a vtable
        2. select the mapped column
        3. check the error names the source column

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE TABLE src_qc (ts timestamp, c1 int, c2 float);")
        tdSql.execute(f"INSERT INTO src_qc VALUES (now, 1, 2);")
        tdSql.execute(f"CREATE VTABLE vntb_qc (ts timestamp, v_c2 float from src_qc.c2);")
        tdSql.query(f"select v_c2 from vntb_qc;")
        tdSql.execute(f"ALTER TABLE src_qc DROP COLUMN c2;")
        tdSql.error(f"select v_c2 from vntb_qc;",
                    expectErrInfo=f'source column "{DB_NAME}.src_qc.c2"',
                    fullMatched=False)

    def test_query_dropped_source_ctb(self):
        """SELECT on a vtable whose referenced physical child table is dropped names the ctb.

        1. drop the referenced physical child table
        2. select from the vtable
        3. check the error names the child table

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute(f"CREATE STABLE pst_q (ts timestamp, v int) TAGS (t int);")
        tdSql.execute(f"CREATE TABLE pctb_q USING pst_q TAGS (1);")
        tdSql.execute(f"INSERT INTO pctb_q VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE vntb_ctb (ts timestamp, a int from pctb_q.v);")
        tdSql.query(f"select a from vntb_ctb;")
        tdSql.execute(f"DROP TABLE pctb_q;")
        tdSql.error(f"select a from vntb_ctb;",
                    expectErrInfo=f'source table "{DB_NAME}.pctb_q" does not exist',
                    fullMatched=False)

    # ------------------------------------------------------------------
    # Supplementary: tag-ref chains, multi-column isolation, chain
    # truncation, execution-time error-code upgrade, VSTB query path
    # ------------------------------------------------------------------

    def test_errmsg_tag_ref_source_drop(self):
        """tag refs (vchild USING vstb) get the enriched err_msg like column refs.

        1. create a vchild whose column and tag ref the same source
        2. drop the source table
        3. check both the column row and the tag row carry the enriched err_msg

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE STABLE src_tstb (ts timestamp, val int) TAGS (city nchar(20), code int);")
        tdSql.execute("CREATE TABLE src_t0 USING src_tstb TAGS ('bj', 1);")
        tdSql.execute("CREATE TABLE src_t1 USING src_tstb TAGS ('sh', 2);")
        tdSql.execute(
            "CREATE STABLE vstb_tag (ts timestamp, val int) "
            "TAGS (local_tag nchar(20), ref_city nchar(20), ref_code int) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vchild_tag (val FROM src_t0.val) USING vstb_tag TAGS ("
            "'loc0', ref_city FROM src_t0.city, ref_code FROM src_t1.code)")
        tdSql.execute("DROP TABLE src_t0;")

        # tag rows do not carry their name in virtual_col_name; match by type
        tdSql.query(f"select virtual_col_name, type, err_code, err_msg "
                    f"from information_schema.ins_virtual_tables_referencing "
                    f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vchild_tag';")
        all_rows = tdSql.queryResult
        col_fail = [r for r in all_rows if r[1] == 0 and r[2] != TSDB_CODE_SUCCESS]
        tag_fail = [r for r in all_rows if r[1] == 1 and r[2] != TSDB_CODE_SUCCESS]
        tag_ok = [r for r in all_rows if r[1] == 1 and r[2] == TSDB_CODE_SUCCESS]
        # the column ref and the tag ref through the dropped table both fail
        assert len(col_fail) == 1 and col_fail[0][0] == "val"
        assert col_fail[0][2] == TSDB_CODE_PAR_TABLE_NOT_EXIST
        assert len(tag_fail) == 1
        assert tag_fail[0][2] == TSDB_CODE_PAR_TABLE_NOT_EXIST
        # the tag ref through the still-existing table keeps succeeding
        assert len(tag_ok) >= 1
        for r in col_fail + tag_fail:
            err_msg = r[3]
            tdLog.info(f"err_msg: {err_msg}")
            assert 'source table' in err_msg
            assert f'"{DB_NAME}.src_t0"' in err_msg
            assert "(chain:" in err_msg
        # the failing tag hop itself is in the chain
        assert f"{DB_NAME}.src_t0.city" in tag_fail[0][3]
        assert f"{DB_NAME}.src_t1" not in tag_fail[0][3]

    def test_errmsg_multi_col_failures_independent(self):
        """Two columns failing on different source tables keep independent err_msgs.

        1. create a vtable with two columns from two sources
        2. drop both source tables
        3. check each row's err_msg names only its own source

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE TABLE src_m1 (ts timestamp, c1 int);")
        tdSql.execute("CREATE TABLE src_m2 (ts timestamp, c1 int);")
        tdSql.execute(
            "CREATE VTABLE vntb_multi (ts timestamp, a int from src_m1.c1, b int from src_m2.c1);")
        tdSql.execute("DROP TABLE src_m1;")
        tdSql.execute("DROP TABLE src_m2;")

        rows = self._query_err_rows("vntb_multi")
        by_name = {r[0]: r for r in rows}
        assert by_name["a"][1] == TSDB_CODE_PAR_TABLE_NOT_EXIST
        assert by_name["b"][1] == TSDB_CODE_PAR_TABLE_NOT_EXIST
        for col, src in (("a", "src_m1"), ("b", "src_m2")):
            err_msg = by_name[col][2]
            tdLog.info(f"{col} err_msg: {err_msg}")
            assert f'"{DB_NAME}.{src}"' in err_msg
        # per-column failure context must not leak into the other row
        assert "src_m2" not in by_name["a"][2]
        assert "src_m1" not in by_name["b"][2]

    def test_errmsg_deep_chain_tail_truncation(self):
        """A chain longer than the 512-byte buffer drops the OLDEST hops and keeps the ones closest to the failure.

        1. build a 16-hop chain with long table names
        2. drop the root source
        3. check the chain is truncated with '...' and keeps the hops closest to the failure

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        hops = 16

        # long names so ~6 hops fill the buffer and truncation margins are wide
        def name(i):
            return f"vdp{i:02d}" + "z" * 44
        tdSql.execute("CREATE TABLE src_deep (ts timestamp, c1 int);")
        tdSql.execute(f"CREATE VTABLE {name(0)} (ts timestamp, v int from src_deep.c1);")
        for i in range(1, hops):
            tdSql.execute(f"CREATE VTABLE {name(i)} (ts timestamp, v int from {name(i - 1)}.v);")
        tdSql.execute("DROP TABLE src_deep;")

        rows = self._query_err_rows(name(hops - 1))
        assert len(rows) == 1
        _, code, err_msg = rows[0]
        tdLog.info(f"err_msg: {err_msg}")
        assert code == TSDB_CODE_PAR_TABLE_NOT_EXIST
        # assert only format-independent invariants: the failure sentence
        # itself is never truncated, the hops closest to the failure survive,
        # and the truncation is marked with "...". 16 long-name hops (~1.1KB)
        # overflow the current 512-byte chain buffer by fixture design, so the
        # "..." marker is guaranteed; which/how many old hops get dropped
        # depends on the buffer size and name lengths, so nothing is asserted
        # about them.
        assert 'source table' in err_msg
        assert f'"{DB_NAME}.src_deep"' in err_msg
        assert "(chain:" in err_msg
        chain = err_msg.split("(chain:")[1].lstrip()
        assert chain.startswith("...")
        assert f"{DB_NAME}.src_deep.c1" in chain
        assert f"{DB_NAME}.{name(0)}.v" in chain

    def test_query_src_table_drop_from_other_conn(self):
        """A source table dropped from another connection is named in the next query's error.

        1. warm the catalog cache on the main connection
        2. drop the source table from another connection
        3. check the next query from the main connection names the source

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE TABLE src_xc (ts timestamp, c1 int);")
        tdSql.execute("INSERT INTO src_xc VALUES (now, 1);")
        tdSql.execute("CREATE VTABLE vntb_xc (ts timestamp, v_c1 int from src_xc.c1);")
        tdSql.query("select v_c1 from vntb_xc;")  # warm the catalog cache
        tdSql.checkRows(1)

        conn2 = self._second_conn()
        try:
            conn2.execute(f"DROP TABLE {DB_NAME}.src_xc;")
        finally:
            conn2.close()

        tdSql.error("select v_c1 from vntb_xc;",
                    expectErrInfo=f'source table "{DB_NAME}.src_xc" does not exist',
                    fullMatched=False)

    def test_query_src_col_drop_from_other_conn(self):
        """A source column dropped from another connection is named in the next query's error.

        1. warm the catalog cache on the main connection
        2. drop the source column from another connection
        3. check the next query names the missing source column

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE TABLE src_xc2 (ts timestamp, c1 int, c2 float);")
        tdSql.execute("INSERT INTO src_xc2 VALUES (now, 1, 2);")
        tdSql.execute("CREATE VTABLE vntb_xc2 (ts timestamp, v_c2 float from src_xc2.c2);")
        tdSql.query("select v_c2 from vntb_xc2;")
        tdSql.checkRows(1)

        conn2 = self._second_conn()
        try:
            conn2.execute(f"ALTER TABLE {DB_NAME}.src_xc2 DROP COLUMN c2;")
        finally:
            conn2.close()

        tdSql.error("select v_c2 from vntb_xc2;",
                    expectErrInfo=f'virtual table source column "{DB_NAME}.src_xc2.c2"',
                    fullMatched=False)

    def test_query_vstb_child_source_drop(self):
        """Querying the virtual super table after a child's source table is dropped fails with the dedicated 0x6210 code.

        1. drop a child's source table
        2. query the virtual super table
        3. check the dedicated 0x6210 code surfaces instead of the stock table-not-exist

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE STABLE src_qv (ts timestamp, val int) TAGS (city nchar(20));")
        tdSql.execute("CREATE TABLE sq0 USING src_qv TAGS ('bj');")
        tdSql.execute("INSERT INTO sq0 VALUES (now, 1);")
        tdSql.execute(
            "CREATE STABLE vstb_q (ts timestamp, val int) TAGS (t nchar(8)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vq0 (val FROM sq0.val) USING vstb_q TAGS ('x')")
        tdSql.query("select val from vstb_q;")
        tdSql.execute("DROP TABLE sq0;")
        # the super-table scan resolves the child's source at execution time:
        # the dedicated 0x6210 code must surface, not the stock table-not-exist,
        # and the client-visible error must name the missing source table
        tdSql.error("select val from vstb_q;",
                    expectedErrno=0x6210,
                    expectErrInfo=f"Virtual table source table does not exist: {DB_NAME}.sq0",
                    fullMatched=False)

    def test_query_vstb_child_source_col_drop(self):
        """Querying the virtual super table after a child's source column is dropped fails with 0x6211 instead of silent NULLs.

        1. drop a child's source column
        2. query the virtual super table
        3. check 0x6211 is returned instead of silent NULL rows / count 0

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE TABLE src_qvc (ts timestamp, c1 int, c2 float);")
        tdSql.execute("INSERT INTO src_qvc VALUES (now, 1, 2);")
        tdSql.execute(
            "CREATE STABLE vstb_qvc (ts timestamp, val int) TAGS (t nchar(8)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vqvc0 (val FROM src_qvc.c1) USING vstb_qvc TAGS ('x')")
        tdSql.query("select val from vstb_qvc;")
        tdSql.execute("ALTER TABLE src_qvc DROP COLUMN c1;")
        tdSql.error("select val from vstb_qvc;",
                    expectedErrno=0x6211,
                    expectErrInfo=f"Virtual table source column not found: {DB_NAME}.src_qvc.c1",
                    fullMatched=False)

    def test_query_deep_chain_names_deepest_source(self):
        """A deep chain whose deepest source table is dropped blames that source, not the middle table.

        1. drop the deepest source table of a 3-hop chain
        2. select from the leaf vtable
        3. check the error blames the dropped source, not the middle table

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE TABLE src_dp (ts timestamp, c1 int);")
        tdSql.execute("CREATE VTABLE vmid_dp (ts timestamp, m int from src_dp.c1);")
        tdSql.execute("CREATE VTABLE vleaf_dp (ts timestamp, l int from vmid_dp.m);")
        tdSql.query("select l from vleaf_dp;")
        tdSql.execute("DROP TABLE src_dp;")
        tdSql.error("select l from vleaf_dp;",
                    expectErrInfo=f'source table "{DB_NAME}.src_dp" does not exist',
                    fullMatched=False)

    def test_query_tag_ref_source_drop(self):
        """Querying a vchild whose TAG ref source is dropped names the source table.

        1. drop the source table of a vchild tag-ref
        2. query the tag from the vchild
        3. check the error names the source table

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE STABLE src_qt (ts timestamp, val int) TAGS (city nchar(20));")
        tdSql.execute("CREATE TABLE st0 USING src_qt TAGS ('bj');")
        tdSql.execute("INSERT INTO st0 VALUES (now, 1);")
        tdSql.execute(
            "CREATE STABLE vstb_qt (ts timestamp, val int) "
            "TAGS (ref_city nchar(20)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vtq0 (val FROM st0.val) USING vstb_qt TAGS (ref_city FROM st0.city)")
        tdSql.query("select ref_city from vtq0;")
        tdSql.execute("DROP TABLE st0;")
        tdSql.error("select ref_city from vtq0;",
                    expectErrInfo=f'source table "{DB_NAME}.st0" does not exist',
                    fullMatched=False)

    def test_query_vstb_mid_hop_source_drop(self):
        """Querying the vstb after a 2-hop chain's deepest source is dropped returns 0x6210.

        1. build vstb child -> middle virtual table -> physical source chain
        2. drop the deepest physical source (middle virtual table kept)
        3. query the vstb; the dynamic ref resolution walks the 2-hop chain and
           must surface the dedicated source-table code, not a stock table-not-exist

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tdSql.execute("CREATE TABLE src_uv (ts timestamp, c1 int);")
        tdSql.execute("INSERT INTO src_uv VALUES (now, 1);")
        tdSql.execute("CREATE VTABLE vmid_uv (ts timestamp, m int from src_uv.c1);")
        tdSql.execute("CREATE STABLE vstb_uv (ts timestamp, val int) TAGS (t nchar(8)) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE vuv0 (val from vmid_uv.m) USING vstb_uv TAGS ('x')")
        tdSql.query("select val from vstb_uv;")
        tdSql.execute("DROP TABLE src_uv;")
        tdSql.error("select val from vstb_uv;",
                    expectedErrno=0x6210,
                    expectErrInfo=f"Virtual table source table does not exist: {DB_NAME}.src_uv",
                    fullMatched=False)

    def test_errmsg_tail_fallback_truncation(self):
        """Not even a single hop fits: the chain tail survives and the message stays closed.

        1. build a vtable whose failure sentence + one maximum-size hop exceed
           the 512-byte err_msg buffer (long source table + column names)
        2. drop the source table
        3. check the chain fallback keeps the hop tail (closest to the failure),
           marks the cut with "...", and the message still ends with ")"

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        """
        tb = "src_lng" + "g" * 184          # ~190 chars, near TSDB_TABLE_NAME_LEN-1
        col = "c" + "o" * 62                # 63 chars, near TSDB_COL_NAME_LEN-1
        tdSql.execute(f"CREATE TABLE {tb} (ts timestamp, {col} int);")
        tdSql.execute(f"CREATE VTABLE vntb_lng (ts timestamp, v int from {tb}.{col});")
        tdSql.execute(f"DROP TABLE {tb};")

        rows = self._query_err_rows("vntb_lng")
        assert len(rows) == 1
        _, code, err_msg = rows[0]
        tdLog.info(f"err_msg: {err_msg}")
        assert code == TSDB_CODE_PAR_TABLE_NOT_EXIST
        # the failure sentence itself is never truncated
        assert 'source table' in err_msg
        assert f'"{DB_NAME}.{tb}"' in err_msg
        # the chain is cut (no full hop fits) but stays marked and CLOSED
        chain = err_msg.split("(chain:")[1]
        assert "..." in chain
        assert str(err_msg).rstrip().endswith(")")
        # the tail of the deepest hop (the failing column name) survives the cut
        assert col[-4:] in chain
        assert len(str(err_msg)) < 512
