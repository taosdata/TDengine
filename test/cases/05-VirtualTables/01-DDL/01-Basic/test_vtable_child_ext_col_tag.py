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
from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
import pytest

DB = "test_vct_ext"

# Test-first cases for: ALTER VTABLE <vchild> ADD COLUMN ... FROM ... / ADD TAG
# Own (extended) columns/tags live only on the virtual child table, not on the
# virtual super table. Own data columns must carry an origin reference; own
# tags may hold a literal value or a tag reference.
# NOTE: these cases fail until the feature is implemented (by design).
class TestVtableChildExtColTag:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        TestVtableChildExtColTag.prepare_tables()

    def teardown_class(cls):
        # drop the dedicated database so a failed/aborted run does not leak state into the
        # next iteration (prepare_tables only drops it at startup)
        tdSql.execute(f"drop database if exists {DB};")

    # --- util ---
    @staticmethod
    def prepare_tables():
        tdLog.info("prepare origin tables.")
        tdSql.execute(f"drop database if exists {DB};")
        tdSql.execute(f"create database {DB};")
        tdSql.execute(f"use {DB};")

        tdSql.execute("create stable org_stb (ts timestamp, v int, d double) tags (loc int, city nchar(20));")
        for i in range(4):
            tdSql.execute(f"create table org_ctb_{i} using org_stb tags ({i}, 'city_{i}');")

        tdSql.execute("create table org_ntb (ts timestamp, ev bigint, en binary(16));")

        tdLog.info("insert origin data.")
        tdSql.execute("insert into org_ctb_0 values (1700000000000, 10, 0.5) (1700000001000, 11, 1.0);")
        tdSql.execute("insert into org_ctb_1 values (1700000000000, 100, 1.5) (1700000002000, 101, 2.5);")
        tdSql.execute("insert into org_ctb_2 values (1700000000000, 20, 3.5);")
        tdSql.execute("insert into org_ctb_3 values (1700000000000, 30, 4.5) (1700000003000, 31, 5.5);")
        # timestamps interleaved with org_ctb_0/org_ctb_1 to verify ts-merge
        tdSql.execute("insert into org_ntb values (1700000000500, 1000, 'n0') (1700000001500, 1001, 'n1');")

        tdLog.info("prepare virtual tables.")
        tdSql.execute("create stable vstb (ts timestamp, v int, d double) tags (loc int) virtual 1;")
        # separate vstb for the stb-alter scenario so no cleanup is needed
        tdSql.execute("create stable vstb2 (ts timestamp, v int) tags (loc int) virtual 1;")

        tdSql.execute("create vtable vctb0 (v from org_ctb_0.v, d from org_ctb_1.d) using vstb tags (1);")
        tdSql.execute("create vtable vctb1 (v from org_ctb_0.v) using vstb tags (2);")
        tdSql.execute("create vtable vctb2 (v from org_ctb_0.v) using vstb tags (3);")
        tdSql.execute("create vtable vctb3 (v from org_ctb_0.v) using vstb tags (4);")
        tdSql.execute("create vtable vctb4 (v from org_ctb_0.v) using vstb tags (5);")
        tdSql.execute("create vtable vctb5 (v from org_ctb_3.v) using vstb2 tags (6);")
        tdSql.execute("create vtable vctb6 (v from org_ctb_0.v) using vstb tags (7);")
        tdSql.execute("create vtable vctb7 (v from org_ctb_0.v) using vstb tags (8);")
        tdSql.execute("create vtable vctb8 (v from org_ctb_0.v, d from org_ctb_1.d) using vstb tags (9);")

        # dedicated vstb/vchild for the DROP scenarios (independent schema state)
        tdSql.execute("create stable vstb_d (ts timestamp, v int, d double) tags (loc int) virtual 1;")
        tdSql.execute("create vtable vctb_d0 (v from org_ctb_0.v, d from org_ctb_1.d) using vstb_d tags (11);")
        tdSql.execute("create vtable vctb_d1 (v from org_ctb_0.v) using vstb_d tags (12);")

        # dedicated vstb/vchild for the churn + query-coverage scenarios
        tdSql.execute("create stable vstb_q (ts timestamp, v int, d double) tags (loc int) virtual 1;")
        # vctb_q0 pulls from several origins so projections/filters/joins have rich data to test
        tdSql.execute("create vtable vctb_q0 (v from org_ctb_0.v, d from org_ctb_1.d) using vstb_q tags (21);")
        tdSql.execute("create vtable vctb_q1 (v from org_ctb_2.v) using vstb_q tags (22);")

        # dedicated vstb/vchild for non-scalar type coverage (string/bool/timestamp/binary/varbinary/geometry)
        tdSql.execute("create stable org_stb_t (ts timestamp, vb bool, vs varchar(32), vn nchar(32), "
                      "vbin varbinary(32), vgeo geometry(128)) tags (tcity nchar(32), tbin binary(16));")
        tdSql.execute("create table org_ctb_t0 using org_stb_t tags ('tcity0', 'tbinc0');")
        # varbinary takes a bare string literal; geometry takes a 'point(x y)' string literal
        tdSql.execute("insert into org_ctb_t0 values "
                      "(1700000000000, true, 'hello', '世界', 'rawbin0', 'point(1.0 2.0)');")
        tdSql.execute("insert into org_ctb_t0 values "
                      "(1700000001000, false, 'world', '你好', 'rawbin1', 'point(3.0 4.0)');")
        tdSql.execute("create table org_ntb_t (ts timestamp, vb bool, vs varchar(32), vn nchar(32), "
                      "vbin varbinary(32), vgeo geometry(128));")
        tdSql.execute("insert into org_ntb_t values "
                      "(1700000000500, true, 'ntb_s', 'ntb_n', 'nbin', 'point(5.0 6.0)');")

        # vstb_t carries an int cascade column with a ref so own-col projections have rows to surface
        tdSql.execute("create stable vstb_t (ts timestamp, x int) tags (loc int) virtual 1;")
        tdSql.execute("create vtable vctb_t0 (x from org_ctb_0.v) using vstb_t tags (31);")

    def check_col(self, tbname, col_name, col_type, col_ref):
        tdSql.query(f"select col_type, col_source from information_schema.ins_columns "
                    f"where db_name='{DB}' and table_name='{tbname}' and col_name='{col_name}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, col_type)
        tdSql.checkData(0, 1, col_ref)

    def check_col_num(self, tbname, col_num):
        tdSql.query(f"select * from information_schema.ins_columns "
                    f"where db_name='{DB}' and table_name='{tbname}'")
        tdSql.checkRows(col_num)

    def check_tag_num(self, tbname, tag_num):
        tdSql.query(f"select * from information_schema.ins_tags "
                    f"where db_name='{DB}' and table_name='{tbname}'")
        tdSql.checkRows(tag_num)

    def check_tag_value(self, tbname, tag_name, tag_value):
        tdSql.query(f"select tag_value from information_schema.ins_tags "
                    f"where db_name='{DB}' and table_name='{tbname}' and tag_name='{tag_name}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tag_value)

    # --- impl ---
    def do_ext_col_add_query(self):
        tdSql.execute(f"use {DB};")

        # 1. add own column with origin reference
        tdSql.execute("alter vtable vctb0 add column ext_ev bigint from org_ntb.ev;")
        self.check_col_num("vctb0", 4)
        self.check_col("vctb0", "ext_ev", "BIGINT", f"{DB}.org_ntb.ev")

        # own column returns origin data; rows come from the selected columns' origins
        tdSql.query("select ts, ext_ev from vctb0 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1000)
        tdSql.checkData(1, 1, 1001)

        # merged with vstb-aligned column v (org_ctb_0 ts union org_ntb ts)
        tdSql.query("select ts, v, ext_ev from vctb0 order by ts;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, 1000)
        tdSql.checkData(2, 1, 11)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, 1001)

        # select own column only
        tdSql.query("select ext_ev from vctb0 order by ts;")
        tdSql.checkRows(2)

        # select * includes own column
        tdSql.query("select * from vctb0 order by ts;")
        tdSql.checkRows(5)
        assert tdSql.queryCols == 4, f"expect 4 cols, got {tdSql.queryCols}"

        # 2. add second own column from a third origin table
        tdSql.execute("alter vtable vctb0 add column ext_v2 int from org_ctb_2.v;")
        self.check_col_num("vctb0", 5)
        tdSql.query("select ts, ext_v2 from vctb0 order by ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 20)

        # 3. vstb-level query must not see own columns
        tdSql.query("select * from vstb order by ts;")
        assert tdSql.queryCols == 4, f"expect 4 cols (ts,v,d,loc), got {tdSql.queryCols}"
        tdSql.error("select ext_ev from vstb;")

        # 4. describe shows own column (5 columns + 1 tag row)
        tdSql.query("describe vctb0;")
        tdSql.checkRows(6)

        print("do_ext_col_add_query .................. [passed]")

    def do_ext_col_errors(self):
        tdSql.execute(f"use {DB};")

        # 1. add column without reference must be rejected on vchild
        tdSql.error("alter vtable vctb1 add column ext_noref bigint;")

        # 2. duplicate name with vstb column
        tdSql.error("alter vtable vctb1 add column v int from org_ctb_0.v;")

        # 3. duplicate name with existing own column
        tdSql.execute("alter vtable vctb1 add column ext_a int from org_ctb_0.v;")
        tdSql.error("alter vtable vctb1 add column ext_a int from org_ctb_1.v;")

        # 4. duplicate name with vstb tag
        tdSql.error("alter vtable vctb1 add column loc int from org_ctb_0.v;")

        # 5. reference target not exist
        tdSql.error("alter vtable vctb1 add column ext_b int from not_exist_table.v;")
        tdSql.error("alter vtable vctb1 add column ext_b int from org_ctb_0.not_exist_col;")

        # 6. reference column type mismatch
        tdSql.error("alter vtable vctb1 add column ext_c bigint from org_ctb_0.v;")

        # 7. reference to a tag is not allowed for data column
        tdSql.error("alter vtable vctb1 add column ext_d int from org_ctb_0.loc;")

        # 8. circular reference must be rejected
        #    vctb2.ext_b -> vctb1.ext_a -> org_ctb_0.v is a legal chain;
        #    then repointing vctb1.ext_a to vctb2.ext_b would close a cycle
        tdSql.execute("alter vtable vctb2 add column ext_b int from vctb1.ext_a;")
        tdSql.error("alter vtable vctb1 alter column ext_a set vctb2.ext_b;")

        # 9. add column on physical child table stays rejected
        tdSql.error("alter table org_ctb_0 add column ext_x int;")

        # 10. insert into vchild stays rejected (virtual tables are read-only)
        tdSql.error("insert into vctb1 values (1700000009000, 1, 1.0);")

        print("do_ext_col_errors ..................... [passed]")

    def do_ext_tag_literal(self):
        tdSql.execute(f"use {DB};")

        # 1. add own tag (literal form)
        tdSql.execute("alter vtable vctb3 add tag ext_city nchar(16);")
        self.check_tag_num("vctb3", 2)

        # 2. own tag has no value before SET TAG
        tdSql.query("select distinct ext_city from vctb3;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # 3. set literal value
        tdSql.execute("alter vtable vctb3 set tag ext_city = 'beijing';")
        self.check_tag_value("vctb3", "ext_city", "beijing")
        tdSql.query("select distinct ext_city from vctb3;")
        tdSql.checkData(0, 0, "beijing")

        # 4. tag in projection together with vstb tag and tbname
        tdSql.query("select tbname, loc, ext_city, v from vctb3 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, "beijing")

        # 5. tag filter
        tdSql.query("select ts, v from vctb3 where ext_city = 'beijing' order by ts;")
        tdSql.checkRows(2)
        tdSql.query("select ts, v from vctb3 where ext_city = 'shanghai' order by ts;")
        tdSql.checkRows(0)

        # 6. vstb-level query must not see own tag
        tdSql.error("select ext_city from vstb;")

        # 7. duplicate tag name rejected
        tdSql.error("alter vtable vctb3 add tag ext_city nchar(16);")
        tdSql.error("alter vtable vctb3 add tag loc int;")

        # 8. add tag on physical child table stays rejected
        tdSql.error("alter table org_ctb_0 add tag ext_x int;")

        print("do_ext_tag_literal .................... [passed]")

    def do_ext_tag_ref(self):
        tdSql.execute(f"use {DB};")

        # 1. add own tag and attach a tag reference
        tdSql.execute("alter vtable vctb4 add tag ext_ref_city nchar(20);")
        tdSql.execute("alter vtable vctb4 set tag ext_ref_city = org_ctb_0.city;")
        tdSql.query("select distinct ext_ref_city from vctb4;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "city_0")

        # 2. source tag change is reflected through the reference
        tdSql.execute("alter table org_ctb_0 set tag city = 'nanjing';")
        tdSql.query("select distinct ext_ref_city from vctb4;")
        tdSql.checkData(0, 0, "nanjing")
        tdSql.execute("alter table org_ctb_0 set tag city = 'city_0';")

        # 3. set literal value clears the reference
        tdSql.execute("alter vtable vctb4 set tag ext_ref_city = 'static_city';")
        tdSql.query("select distinct ext_ref_city from vctb4;")
        tdSql.checkData(0, 0, "static_city")
        tdSql.execute("alter table org_ctb_0 set tag city = 'nanjing';")
        tdSql.query("select distinct ext_ref_city from vctb4;")
        tdSql.checkData(0, 0, "static_city")
        tdSql.execute("alter table org_ctb_0 set tag city = 'city_0';")

        # 4. literal -> ref again (round trip)
        tdSql.execute("alter vtable vctb4 set tag ext_ref_city = org_ctb_1.city;")
        tdSql.query("select distinct ext_ref_city from vctb4;")
        tdSql.checkData(0, 0, "city_1")

        # 5. error cases
        # 5.1. reference to a data column (not a tag)
        tdSql.error("alter vtable vctb4 set tag ext_ref_city = org_ctb_0.v;")
        # 5.2. type mismatch
        tdSql.execute("alter vtable vctb4 add tag ext_ref_int int;")
        tdSql.error("alter vtable vctb4 set tag ext_ref_int = org_ctb_0.city;")
        # 5.3. reference target not exist
        tdSql.error("alter vtable vctb4 set tag ext_ref_city = not_exist_table.city;")

        print("do_ext_tag_ref ........................ [passed]")

    def do_ext_with_stb_alter(self):
        tdSql.execute(f"use {DB};")

        # vctb5 uses vstb2; first give it an own column and an own tag
        tdSql.execute("alter vtable vctb5 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb5 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb5 set tag ext_city = 'beijing';")

        # 1. vstb add column afterwards: cascaded unref column + own column coexist
        tdSql.execute("alter stable vstb2 add column stb_new_col int;")
        self.check_col_num("vctb5", 4)  # ts, v, stb_new_col, ext_ev
        self.check_col("vctb5", "stb_new_col", "INT", None)
        self.check_col("vctb5", "ext_ev", "BIGINT", f"{DB}.org_ntb.ev")

        # cascaded column has no ref (contributes no origin); rows come from ext_ev's origin
        tdSql.query("select ts, stb_new_col, ext_ev from vctb5 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 1000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, 1001)

        # 2. vstb add tag afterwards: own tag value stays correct
        tdSql.execute("alter stable vstb2 add tag stb_new_tag int;")
        self.check_tag_num("vctb5", 3)  # loc, ext_city, stb_new_tag
        self.check_tag_value("vctb5", "ext_city", "beijing")

        tdSql.execute("alter vtable vctb5 set tag stb_new_tag = 99;")
        tdSql.query("select loc, stb_new_tag, ext_city from vctb5;")
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 99)
        tdSql.checkData(0, 2, "beijing")

        # 3. own column name conflicts with future vstb column are impossible:
        #    vstb add column with a name already used by vctb5's own column must fail
        tdSql.error("alter stable vstb2 add column ext_ev bigint;")

        print("do_ext_with_stb_alter ................ [passed]")

    def do_ext_drop(self):
        tdSql.execute(f"use {DB};")

        # give vctb_d0 an own column + own tag for the DROP scenarios
        tdSql.execute("alter vtable vctb_d0 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb_d0 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb_d0 set tag ext_city = 'beijing';")
        self.check_col_num("vctb_d0", 4)  # ts, v, d, ext_ev
        self.check_tag_num("vctb_d0", 2)  # loc, ext_city

        # --- A. vchild DROP own column / own tag ---
        # 0. dropping a vstb cascaded column via the vchild is rejected (cascaded columns belong
        #    to the vstb; only own columns live in the vchild schema)
        tdSql.error("alter vtable vctb_d0 drop column d;")
        # the failed drop leaves the own schema intact
        self.check_col_num("vctb_d0", 4)
        self.check_col("vctb_d0", "ext_ev", "BIGINT", f"{DB}.org_ntb.ev")

        # 1. drop own column: schema shrinks, the column is no longer selectable
        tdSql.execute("alter vtable vctb_d0 drop column ext_ev;")
        self.check_col_num("vctb_d0", 3)  # ts, v, d
        tdSql.query(f"select col_name from information_schema.ins_columns "
                    f"where db_name='{DB}' and table_name='vctb_d0' and col_name='ext_ev'")
        tdSql.checkRows(0)
        tdSql.error("select ext_ev from vctb_d0;")
        # remaining columns still queryable; rows = ts-union of v's origin (org_ctb_0) and
        # d's origin (org_ctb_1): {000,1000} ∪ {000,2000} = {000,1000,2000}
        tdSql.query("select ts, v, d from vctb_d0 order by ts;")
        tdSql.checkRows(3)

        # 2. drop own tag: tag schema shrinks, the tag is no longer selectable/filterable
        tdSql.execute("alter vtable vctb_d0 drop tag ext_city;")
        self.check_tag_num("vctb_d0", 1)  # loc
        tdSql.query(f"select tag_name from information_schema.ins_tags "
                    f"where db_name='{DB}' and table_name='vctb_d0' and tag_name='ext_city'")
        tdSql.checkRows(0)
        tdSql.error("select distinct ext_city from vctb_d0;")
        tdSql.error("select ts, v from vctb_d0 where ext_city = 'beijing';")

        # 3. drop an own tag that carries a reference: ref entry is cleaned up (no leak)
        tdSql.execute("alter vtable vctb_d0 add tag ext_ref nchar(20);")
        tdSql.execute("alter vtable vctb_d0 set tag ext_ref = org_ctb_0.city;")
        tdSql.query("select distinct ext_ref from vctb_d0;")
        tdSql.checkData(0, 0, "city_0")
        tdSql.execute("alter vtable vctb_d0 drop tag ext_ref;")
        self.check_tag_num("vctb_d0", 1)
        tdSql.error("select distinct ext_ref from vctb_d0;")

        # 4. drop -> re-add same name: colId is freshly allocated (never reuses the dropped gap)
        tdSql.execute("alter vtable vctb_d0 add column ext_ev bigint from org_ntb.ev;")
        self.check_col("vctb_d0", "ext_ev", "BIGINT", f"{DB}.org_ntb.ev")
        tdSql.execute("alter vtable vctb_d0 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb_d0 set tag ext_city = 'beijing';")
        self.check_tag_value("vctb_d0", "ext_city", "beijing")
        # restore the schema state expected by cases B/C/D below
        self.check_col_num("vctb_d0", 4)  # ts, v, d, ext_ev
        self.check_tag_num("vctb_d0", 2)  # loc, ext_city

        # --- B. vstable DROP COLUMN cascades to vchild, own columns survive ---
        # vstb_d drops column d; vctb_d0 must lose d but keep v and ext_ev
        tdSql.execute("alter stable vstb_d drop column d;")
        self.check_col_num("vctb_d0", 3)  # ts, v, ext_ev
        tdSql.query(f"select col_name from information_schema.ins_columns "
                    f"where db_name='{DB}' and table_name='vctb_d0' and col_name='d'")
        tdSql.checkRows(0)
        # own column still queryable and returns origin data
        self.check_col("vctb_d0", "ext_ev", "BIGINT", f"{DB}.org_ntb.ev")
        # rows = ts-union of v's origin (org_ctb_0: 000,1000) and ext_ev's origin (org_ntb: 500,1500)
        tdSql.query("select ts, v, ext_ev from vctb_d0 order by ts;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10)     # org_ctb_0.v @000
        tdSql.checkData(0, 2, None)   # ext_ev absent @000
        tdSql.checkData(1, 1, None)   # v absent @500
        tdSql.checkData(1, 2, 1000)   # org_ntb.ev @500
        tdSql.checkData(2, 1, 11)     # org_ctb_0.v @1000
        tdSql.checkData(3, 2, 1001)   # org_ntb.ev @1500
        # a vchild that never had d (vctb_d1) is unaffected in column count
        self.check_col_num("vctb_d1", 2)  # ts, v

        # --- C. vstable DROP TAG cascades, own tag values survive ---
        # add a second vstb tag so dropping loc does not hit the "last tag" guard
        tdSql.execute("alter stable vstb_d add tag grp int;")
        tdSql.execute("alter vtable vctb_d0 set tag grp = 100;")
        tdSql.execute("alter vtable vctb_d1 set tag grp = 200;")
        self.check_tag_num("vctb_d0", 3)  # loc, ext_city, grp
        # drop the vstb tag loc; own tag ext_city must survive with its value
        tdSql.execute("alter stable vstb_d drop tag loc;")
        self.check_tag_num("vctb_d0", 2)  # ext_city, grp
        self.check_tag_value("vctb_d0", "ext_city", "beijing")
        self.check_tag_value("vctb_d0", "grp", "100")
        self.check_tag_num("vctb_d1", 1)  # grp
        self.check_tag_value("vctb_d1", "grp", "200")
        # own tag still usable in projection and filter
        tdSql.query("select distinct ext_city from vctb_d0;")
        tdSql.checkData(0, 0, "beijing")
        # projecting only v (org_ctb_0) yields org_ctb_0's rows; the own-tag filter applies
        tdSql.query("select ts, v from vctb_d0 where ext_city = 'beijing' order by ts;")
        tdSql.checkRows(2)

        # --- D. vstable DROP the last column / tag guards still apply ---
        # dropping the only remaining vstb data column (v) is rejected by the
        # "cannot drop the last non-ts column" guard, own schema untouched
        tdSql.error("alter stable vstb_d drop column v;")
        self.check_col_num("vctb_d0", 3)
        # dropping the last remaining vstb tag (grp) is rejected by the
        # "cannot drop the last tag" guard, own tag untouched
        tdSql.error("alter stable vstb_d drop tag grp;")
        self.check_tag_num("vctb_d0", 2)

        print("do_ext_drop ......................... [passed]")

    def do_ext_churn(self):
        tdSql.execute(f"use {DB};")
        # vctb_q1 USING vstb_q(ts,v,d): cascaded cols = ts,v,d (d has no ref -> all NULL); + tag loc
        base_cols = 3  # ts, v, d
        base_tags = 1  # loc

        # --- 1. same-name same-type add/drop churn: each re-add must allocate a fresh colId
        #    (DROP leaves a gap that is never reused); schema state is consistent every iteration
        N_SAME = 20
        for i in range(N_SAME):
            tdSql.execute("alter vtable vctb_q1 add column ext_same int from org_ctb_2.v;")
            self.check_col_num("vctb_q1", base_cols + 1)
            self.check_col("vctb_q1", "ext_same", "INT", f"{DB}.org_ctb_2.v")
            tdSql.query("select ext_same from vctb_q1 order by ts;")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 20)
            tdSql.execute("alter vtable vctb_q1 drop column ext_same;")
            self.check_col_num("vctb_q1", base_cols)
            tdSql.error("select ext_same from vctb_q1;")
        self.check_col_num("vctb_q1", base_cols)

        # --- 2. same-name different-type add/drop churn: type flips each iteration, each time
        #    sourced from a matching-type origin column; the name-conflict guard still rejects a
        #    duplicate while the column exists
        #    (int<-org_ctb_2.v, bigint<-org_ntb.ev, double<-org_ctb_2.d, then back)
        flips = [("int", "INT", f"{DB}.org_ctb_2.v"),
                 ("bigint", "BIGINT", f"{DB}.org_ntb.ev"),
                 ("double", "DOUBLE", f"{DB}.org_ctb_2.d"),
                 ("int", "INT", f"{DB}.org_ctb_2.v"),
                 ("bigint", "BIGINT", f"{DB}.org_ntb.ev")]
        for tname, tupper, ref in flips:
            tdSql.execute(f"alter vtable vctb_q1 add column ext_flip {tname} from {ref};")
            self.check_col("vctb_q1", "ext_flip", tupper, ref)
            # while ext_flip exists, re-adding the same name (any type) must be rejected
            tdSql.error(f"alter vtable vctb_q1 add column ext_flip int from org_ctb_2.v;")
            tdSql.execute("alter vtable vctb_q1 drop column ext_flip;")
            self.check_col_num("vctb_q1", base_cols)
        self.check_col_num("vctb_q1", base_cols)

        # --- 3. cross-kind name churn: a name freed by dropping a column can be reused as a tag,
        #    and vice-versa (col/tag share one name namespace)
        for i in range(6):
            kind = i % 2
            if kind == 0:
                tdSql.execute("alter vtable vctb_q1 add column ext_cross int from org_ctb_2.v;")
                self.check_col_num("vctb_q1", base_cols + 1)
                tdSql.execute("alter vtable vctb_q1 drop column ext_cross;")
            else:
                tdSql.execute("alter vtable vctb_q1 add tag ext_cross int;")
                self.check_tag_num("vctb_q1", base_tags + 1)
                tdSql.execute("alter vtable vctb_q1 drop tag ext_cross;")
            # name is free again either way
            self.check_col_num("vctb_q1", base_cols)
            self.check_tag_num("vctb_q1", base_tags)
        self.check_col_num("vctb_q1", base_cols)
        self.check_tag_num("vctb_q1", base_tags)

        # --- 4. multi-member churn: hold several own cols/tags at once, drop them in a scrambled
        #    order, verify the survivors stay queryable and correctly typed each round
        N_ROUNDS = 8
        for r in range(N_ROUNDS):
            # add a fan of own columns and tags
            for j in range(4):
                tdSql.execute(f"alter vtable vctb_q1 add column mc_c{j} int from org_ctb_2.v;")
            for j in range(3):
                tdSql.execute(f"alter vtable vctb_q1 add tag mc_t{j} int;")
            self.check_col_num("vctb_q1", base_cols + 4)
            self.check_tag_num("vctb_q1", base_tags + 3)
            # drop in reverse order to exercise non-tail memmove
            for j in reversed(range(4)):
                tdSql.execute(f"alter vtable vctb_q1 drop column mc_c{j};")
                # remaining own columns still resolve
                for k in range(j):
                    self.check_col("vctb_q1", f"mc_c{k}", "INT", f"{DB}.org_ctb_2.v")
            for j in reversed(range(3)):
                tdSql.execute(f"alter vtable vctb_q1 drop tag mc_t{j};")
            self.check_col_num("vctb_q1", base_cols)
            self.check_tag_num("vctb_q1", base_tags)
        # after all rounds the schema is back to baseline
        self.check_col_num("vctb_q1", base_cols)
        self.check_tag_num("vctb_q1", base_tags)

        # --- 5. tag literal/ref churn on the same name
        for i in range(6):
            tdSql.execute("alter vtable vctb_q1 add tag ext_city nchar(20);")
            if i % 2 == 0:
                tdSql.execute(f"alter vtable vctb_q1 set tag ext_city = 'city_{i}';")
                self.check_tag_value("vctb_q1", "ext_city", f"city_{i}")
            else:
                tdSql.execute("alter vtable vctb_q1 set tag ext_city = org_ctb_0.city;")
                tdSql.query("select distinct ext_city from vctb_q1;")
                tdSql.checkData(0, 0, "city_0")
            tdSql.execute("alter vtable vctb_q1 drop tag ext_city;")
            self.check_tag_num("vctb_q1", base_tags)
            tdSql.error("select distinct ext_city from vctb_q1;")
        self.check_tag_num("vctb_q1", base_tags)

        print("do_ext_churn ........................ [passed]")

    def do_ext_own_query(self):
        tdSql.execute(f"use {DB};")
        # vctb_q0: v from org_ctb_0 (v: 10@000, 11@1000), d from org_ctb_1 (d: 1.5@000, 2.5@2000)
        # add own col ext_ev from org_ntb (ev: 1000@500, 1001@1500) and own tag ext_city
        tdSql.execute("alter vtable vctb_q0 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb_q0 add tag ext_city nchar(20);")
        tdSql.execute("alter vtable vctb_q0 set tag ext_city = 'beijing';")
        self.check_col_num("vctb_q0", 4)  # ts, v, d, ext_ev
        self.check_tag_num("vctb_q0", 2)  # loc, ext_city

        # --- 1. project own column alone, and merged with vstb columns by ts ---
        tdSql.query("select ext_ev from vctb_q0 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1000)
        tdSql.checkData(1, 0, 1001)
        # ts-union of v (000,1000), d (000,2000), ext_ev (500,1500) = {000,500,1000,1500,2000}
        tdSql.query("select ts, v, d, ext_ev from vctb_q0 order by ts;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1700000000000)  # 000
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 1.5)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 0, 1700000000500)  # 500 (ext_ev only)
        tdSql.checkData(1, 3, 1000)
        tdSql.checkData(2, 0, 1700000001000)  # 1000 (v only; ext_ev has no row here)
        tdSql.checkData(2, 1, 11)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(3, 0, 1700000001500)  # 1500 (ext_ev only)
        tdSql.checkData(3, 3, 1001)
        tdSql.checkData(4, 0, 1700000002000)  # 2000 (d only)
        tdSql.checkData(4, 2, 2.5)

        # --- 2. WHERE filter on own column (equality + range) ---
        tdSql.query("select ts, ext_ev from vctb_q0 where ext_ev = 1001 order by ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1001)
        # ext_ev values are 1000@500, 1001@1500; ext_ev > 1000 matches only the 1001 row
        tdSql.query("select ts, ext_ev from vctb_q0 where ext_ev > 1000 order by ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1001)

        # --- 3. own column in arithmetic expression and aggregation ---
        tdSql.query("select sum(ext_ev), max(ext_ev), min(ext_ev), count(ext_ev) from vctb_q0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2001)  # 1000 + 1001
        tdSql.checkData(0, 1, 1001)
        tdSql.checkData(0, 2, 1000)
        tdSql.checkData(0, 3, 2)
        tdSql.query("select ext_ev + 1 from vctb_q0 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1001)
        tdSql.checkData(1, 0, 1002)

        # --- 4. own tag: projection, equality filter, combined with vstb tag and tbname ---
        # tag projection returns one row per data row (tags are attached to each row)
        tdSql.query("select tbname, loc, ext_city from vctb_q0;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "vctb_q0")
        tdSql.checkData(0, 1, 21)
        tdSql.checkData(0, 2, "beijing")
        tdSql.query("select ts, v from vctb_q0 where ext_city = 'beijing' order by ts;")
        # projecting v (org_ctb_0) yields org_ctb_0 rows, filtered by own tag = 2 rows
        tdSql.checkRows(2)
        tdSql.query("select ts, v from vctb_q0 where ext_city = 'shanghai' order by ts;")
        tdSql.checkRows(0)
        # combined vstb tag + own tag filter
        tdSql.query("select ts, v from vctb_q0 where loc = 21 and ext_city = 'beijing' order by ts;")
        tdSql.checkRows(2)

        # --- 5. distinct own tag, group by own tag ---
        tdSql.query("select distinct ext_city from vctb_q0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "beijing")
        tdSql.query("select ext_city, count(*) from vctb_q0 partition by ext_city;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "beijing")

        # --- 6. own column + vstb column mixed in WHERE and projection together ---
        # v exists at ts {000,1000}, ext_ev at {500,1500}: no shared ts -> 0 rows
        tdSql.query("select ts, v, ext_ev from vctb_q0 where v is not null and ext_ev is not null order by ts;")
        tdSql.checkRows(0)
        # but d (000,2000) and v (000,1000) share ts 000
        tdSql.query("select ts, v, d from vctb_q0 where v is not null and d is not null order by ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1700000000000)
        tdSql.checkData(0, 1, 10)

        # --- 7. order by own column, and own column in ORDER BY with mixed sources ---
        tdSql.query("select ts, ext_ev from vctb_q0 order by ext_ev desc;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1001)
        tdSql.checkData(1, 1, 1000)

        # --- 8. select * includes own column after vstb columns; describe layout ---
        tdSql.query("select * from vctb_q0 order by ts;")
        tdSql.checkRows(5)
        assert tdSql.queryCols == 4, f"expect 4 cols (ts,v,d,ext_ev), got {tdSql.queryCols}"

        # --- 9. own tag reference (dynamic): own tag value resolves through the origin tag ---
        tdSql.execute("alter vtable vctb_q0 add tag ext_ref_city nchar(20);")
        tdSql.execute("alter vtable vctb_q0 set tag ext_ref_city = org_ctb_0.city;")
        tdSql.query("select distinct ext_ref_city from vctb_q0;")
        tdSql.checkData(0, 0, "city_0")
        # own literal tag + own ref tag both usable in the same filter
        tdSql.query("select ts, v from vctb_q0 where ext_ref_city = 'city_0' and ext_city = 'beijing' order by ts;")
        tdSql.checkRows(2)

        print("do_ext_own_query .................... [passed]")

    def do_ext_non_scalar(self):
        tdSql.execute(f"use {DB};")
        # vctb_t0: cascade col x from org_ctb_0.v (rows @000,@1000). Own cols below pull from
        # org_ctb_t0 (same ts @000,@1000) so projections line up, plus org_ntb_t (@500) for ts-merge.

        # --- 1. add own columns of every non-scalar type, verify schema in ins_columns ---
        tdSql.execute("alter vtable vctb_t0 add column ob bool from org_ctb_t0.vb;")
        tdSql.execute("alter vtable vctb_t0 add column os varchar(32) from org_ctb_t0.vs;")
        tdSql.execute("alter vtable vctb_t0 add column onch nchar(32) from org_ctb_t0.vn;")
        tdSql.execute("alter vtable vctb_t0 add column obin varbinary(32) from org_ctb_t0.vbin;")
        tdSql.execute("alter vtable vctb_t0 add column ogeo geometry(128) from org_ctb_t0.vgeo;")
        self.check_col("vctb_t0", "ob", "BOOL", f"{DB}.org_ctb_t0.vb")
        self.check_col("vctb_t0", "os", "VARCHAR(32)", f"{DB}.org_ctb_t0.vs")
        self.check_col("vctb_t0", "onch", "NCHAR(32)", f"{DB}.org_ctb_t0.vn")
        self.check_col("vctb_t0", "obin", "VARBINARY", f"{DB}.org_ctb_t0.vbin")
        self.check_col("vctb_t0", "ogeo", "GEOMETRY", f"{DB}.org_ctb_t0.vgeo")

        # --- 2. project each non-scalar own column; rows line up with org_ctb_t0 (@000,@1000) ---
        tdSql.query("select ts, ob, os, onch, ogeo from vctb_t0 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, True)            # @000: vb=true
        tdSql.checkData(0, 2, "hello")
        tdSql.checkData(0, 3, "世界")
        tdSql.checkData(1, 1, False)           # @1000: vb=false
        tdSql.checkData(1, 2, "world")
        tdSql.checkData(1, 3, "你好")

        # --- 3. WHERE filter on string own column (equality) ---
        tdSql.query("select os from vctb_t0 where os = 'world';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "world")
        # nchar own column filter (unicode value)
        tdSql.query("select onch from vctb_t0 where onch = '世界';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "世界")

        # --- 4. WHERE filter on bool own column (true / false) ---
        tdSql.query("select ts, ob from vctb_t0 where ob = true order by ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1700000000000)
        tdSql.query("select ts, ob from vctb_t0 where ob = false order by ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1700000001000)

        # --- 5. varbinary own column round-trips the raw bytes (connector returns bytes) ---
        tdSql.query("select obin from vctb_t0 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, b"rawbin0")
        tdSql.checkData(1, 0, b"rawbin1")

        # --- 6. add an own col from a second origin (org_ntb_t @500) -> ts-merge exposes 3 rows ---
        tdSql.execute("alter vtable vctb_t0 add column ob_n bool from org_ntb_t.vb;")
        tdSql.execute("alter vtable vctb_t0 add column os_n varchar(32) from org_ntb_t.vs;")
        tdSql.query("select ts, os, os_n from vctb_t0 order by ts;")
        tdSql.checkRows(3)  # {000,500,1000}
        tdSql.checkData(0, 1, "hello")
        tdSql.checkData(0, 2, None)        # os_n absent @000
        tdSql.checkData(1, 1, None)        # os absent @500
        tdSql.checkData(1, 2, "ntb_s")
        tdSql.checkData(2, 1, "world")

        # --- 7. own tags of every non-scalar type (literal values) ---
        tdSql.execute("alter vtable vctb_t0 add tag tbool bool;")
        tdSql.execute("alter vtable vctb_t0 add tag tvar varchar(32);")
        tdSql.execute("alter vtable vctb_t0 add tag tnchar nchar(32);")
        tdSql.execute("alter vtable vctb_t0 add tag tbinary binary(16);")
        tdSql.execute("alter vtable vctb_t0 add tag tgeom geometry(128);")
        tdSql.execute("alter vtable vctb_t0 set tag tbool = true;")
        tdSql.execute("alter vtable vctb_t0 set tag tvar = 'red';")
        tdSql.execute("alter vtable vctb_t0 set tag tnchar = '蓝色';")
        tdSql.execute("alter vtable vctb_t0 set tag tbinary = 'binc';")
        tdSql.execute("alter vtable vctb_t0 set tag tgeom = 'point(7 8)';")
        # ins_tags.tag_value renders every tag as a string (bool -> 'true')
        self.check_tag_value("vctb_t0", "tbool", "true")
        self.check_tag_value("vctb_t0", "tvar", "red")
        self.check_tag_value("vctb_t0", "tnchar", "蓝色")
        self.check_tag_value("vctb_t0", "tbinary", "binc")

        # --- 8. own tag filter across non-scalar types ---
        # projecting x (org_ctb_0) yields org_ctb_0's 2 rows; the own-tag filter keeps all of them
        tdSql.query("select ts, x from vctb_t0 where tbool = true order by ts;")
        tdSql.checkRows(2)
        tdSql.query("select ts, x from vctb_t0 where tvar = 'red' order by ts;")
        tdSql.checkRows(2)
        tdSql.query("select ts, x from vctb_t0 where tnchar = '蓝色' order by ts;")
        tdSql.checkRows(2)
        tdSql.query("select ts, x from vctb_t0 where tvar = 'blue' order by ts;")
        tdSql.checkRows(0)

        # --- 9. distinct own tag of string type ---
        tdSql.query("select distinct tvar from vctb_t0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "red")

        print("do_ext_non_scalar ................... [passed]")

    def do_ext_stb_name_conflicts(self):
        tdSql.execute(f"use {DB};")

        # dedicated tables for the name-conflict scenarios
        tdSql.execute("create stable vstb3 (ts timestamp, v int) tags (loc int) virtual 1;")
        tdSql.execute("create vtable vctb9 (v from org_ctb_0.v) using vstb3 tags (10);")
        tdSql.execute("alter vtable vctb9 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb9 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb9 set tag ext_city = 'beijing';")

        # 1. vstb add column/tag with a name used by vctb9's own column/tag must fail
        tdSql.error("alter stable vstb3 add column ext_ev bigint;")
        tdSql.error("alter stable vstb3 add column ext_city int;")
        tdSql.error("alter stable vstb3 add tag ext_city int;")
        tdSql.error("alter stable vstb3 add tag ext_ev int;")

        # 2. vstb rename column/tag into an own name must fail
        tdSql.error("alter stable vstb3 rename column v ext_ev;")
        tdSql.error("alter stable vstb3 rename tag loc ext_city;")

        # 3. the failed stb alters above must leave the own schema intact
        #    (no half-applied stb schema, own column/tag still queryable)
        tdSql.query("select ts, ext_ev from vctb9 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1000)
        tdSql.query("select distinct ext_city from vctb9;")
        tdSql.checkData(0, 0, "beijing")

        # 4. non-conflicting vstb alter still works and keeps the own schema
        tdSql.execute("alter stable vstb3 add column stb_c2 int;")
        self.check_col_num("vctb9", 4)  # ts, v, stb_c2, ext_ev
        self.check_tag_num("vctb9", 2)  # loc, ext_city
        tdSql.query("select ts, stb_c2, ext_ev from vctb9 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 1000)

        # 5. own tag type restrictions (same rules as stb ADD TAG)
        tdSql.error("alter vtable vctb9 add tag ext_json json;")
        tdSql.error("alter vtable vctb9 add tag ext_dec decimal(10,2);")

        # 6. own tag name must not collide with vstb column/tag or own column
        tdSql.error("alter vtable vctb9 add tag v int;")
        tdSql.error("alter vtable vctb9 add tag loc int;")
        tdSql.error("alter vtable vctb9 add tag ext_ev int;")

        # 7. reverse direction (symmetric with case 1): vstb claims a name via
        #    ALTER first, then vchild tries to add an own col/tag of the same
        #    name -> must be rejected. Own col/tag shares the name namespace with
        #    vstb cascaded col/tag regardless of which side claims the name first.
        tdSql.execute("alter stable vstb3 add tag stb_t2 int;")  # vstb3 now has tag stb_t2
        # types are matched to the ref so the ONLY rejection reason is name conflict
        tdSql.error("alter vtable vctb9 add column stb_c2 bigint from org_ntb.ev;")  # stb_c2 is now a vstb col (case 4)
        tdSql.error("alter vtable vctb9 add tag stb_t2 int;")  # stb_t2 is now a vstb tag
        tdSql.error("alter vtable vctb9 add tag stb_c2 int;")  # own tag name vs vstb ALTER-added column
        # final state: own schema intact, vstb-alter-added members present
        self.check_col_num("vctb9", 4)  # ts, v, stb_c2, ext_ev
        self.check_tag_num("vctb9", 3)  # loc, ext_city, stb_t2

        print("do_ext_stb_name_conflicts ............. [passed]")

    def do_ext_stb_name_conflicts_in_txn(self):
        """Same-name vstb alter rejections inside a batch meta txn (txnId != 0 path).

        The batch txn MARK path used to skip the vchild own-name precheck, so a
        conflicting ALTER STABLE entered a RETRY marker txn and only failed later
        at the vnode backstop (retry loop). The precheck now runs before the
        shadow op is recorded and fails the alter loudly.
        """
        tdSql.execute(f"use {DB};")

        tdSql.execute("create stable vstb4 (ts timestamp, v int) tags (loc int) virtual 1;")
        tdSql.execute("create vtable vctb10 (v from org_ctb_0.v) using vstb4 tags (10);")
        tdSql.execute("alter vtable vctb10 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb10 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb10 set tag ext_city = 'beijing';")

        # 1. in-txn ADD COLUMN / ADD TAG / RENAME claiming an own name must be
        #    rejected by the precheck (not deferred to commit)
        tdSql.execute("BEGIN")
        tdSql.error("alter stable vstb4 add column ext_ev bigint;")
        tdSql.error("alter stable vstb4 add tag ext_city int;")
        tdSql.error("alter stable vstb4 rename tag loc ext_city;")
        tdSql.execute("ROLLBACK")

        # 2. a valid in-txn alter (no conflict) still works end to end
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable vstb4 add column stb_c3 int;")
        tdSql.execute("COMMIT")
        self.check_col_num("vctb10", 4)  # ts, v, stb_c3, ext_ev
        self.check_tag_num("vctb10", 2)  # loc, ext_city

        # 3. the rejected marks left no shadow state behind: own members are
        #    intact and a fresh non-txn conflicting alter is still rejected
        #    with the same error
        tdSql.query("select ts, ext_ev from vctb10 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1000)
        tdSql.query("select distinct ext_city from vctb10;")
        tdSql.checkData(0, 0, "beijing")
        tdSql.error("alter stable vstb4 add column ext_ev bigint;")

        print("do_ext_stb_name_conflicts_in_txn ..... [passed]")

    def do_ext_persistence(self):
        tdSql.execute(f"use {DB};")

        tdSql.execute("alter vtable vctb6 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb6 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb6 set tag ext_city = 'beijing';")

        tdLog.info("restart dnode to verify persistence of own schema.")
        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.execute(f"use {DB};")
        self.check_col_num("vctb6", 4)
        self.check_col("vctb6", "ext_ev", "BIGINT", f"{DB}.org_ntb.ev")
        self.check_tag_num("vctb6", 2)
        self.check_tag_value("vctb6", "ext_city", "beijing")

        # rows come from ext_ev's origin (org_ntb ts: 500, 1500)
        tdSql.query("select ts, ext_ev from vctb6 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1000)
        tdSql.checkData(1, 1, 1001)

        print("do_ext_persistence .................... [passed]")

    def do_ext_persistent_conn(self):
        tdSql.execute(f"use {DB};")

        # one long-lived connection caches the vchild meta first
        conn1 = tdCom.newTdSql(database=DB)
        conn1.query("select * from vctb7;")
        conn1.checkRows(2)

        # another connection adds own column/tag
        tdSql.execute("alter vtable vctb7 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb7 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb7 set tag ext_city = 'beijing';")

        # the first connection must see the new schema on next query
        # rows come from ext_ev's origin (org_ntb ts: 500, 1500)
        conn1.query("select ts, ext_ev from vctb7 order by ts;")
        conn1.checkRows(2)
        conn1.checkData(0, 1, 1000)

        conn1.query("select distinct ext_city from vctb7;")
        conn1.checkData(0, 0, "beijing")

        conn1.close()

        print("do_ext_persistent_conn ................ [passed]")

    def do_ext_show_create(self):
        tdSql.execute(f"use {DB};")

        tdSql.execute("alter vtable vctb8 add column ext_ev bigint from org_ntb.ev;")
        tdSql.execute("alter vtable vctb8 add tag ext_city nchar(16);")
        tdSql.execute("alter vtable vctb8 set tag ext_city = 'beijing';")

        # 1. show create output contains own column (with ref) and own tag
        tdSql.query("show create vtable vctb8;")
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"show create vctb8: {create_sql}")
        assert "ext_ev" in create_sql, "own column missing in show create output"
        assert "org_ntb" in create_sql, "own column ref missing in show create output"
        assert "ext_city" in create_sql, "own tag missing in show create output"
        assert "beijing" in create_sql, "own tag value missing in show create output"

        # 2. output must be replayable: recreate an equivalent table from it
        tdSql.execute("drop vtable if exists vctb8_replay;")
        replay_sql = create_sql.replace("`vctb8`", "`vctb8_replay`")
        tdSql.execute(replay_sql)

        self.check_col_num("vctb8_replay", 4)
        self.check_col("vctb8_replay", "ext_ev", "BIGINT", f"{DB}.org_ntb.ev")
        self.check_tag_num("vctb8_replay", 2)
        self.check_tag_value("vctb8_replay", "ext_city", "beijing")

        tdSql.query("select ts, ext_ev from vctb8_replay order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1000)

        tdSql.execute("drop vtable vctb8_replay;")

        print("do_ext_show_create .................... [passed]")

    def do_ext_own_decimal(self):
        tdSql.execute(f"use {DB};")

        # dedicated decimal source + vstb/vchild so this case is self-contained
        tdSql.execute("create table org_ntb_dec (ts timestamp, price decimal(10,2), score decimal(8,2));")
        tdSql.execute("insert into org_ntb_dec values (1700000000000, 12.34, 1.50) (1700000001000, 99.99, 2.75);")
        tdSql.execute("create stable vstb_dec (ts timestamp, v int) tags (loc int) virtual 1;")
        tdSql.execute("create vtable vctb_dec (v from org_ctb_0.v) using vstb_dec tags (41);")

        # ALTER path: add own decimal columns (mix of precisions)
        tdSql.execute("alter vtable vctb_dec add column ext_price decimal(10,2) from org_ntb_dec.price;")
        tdSql.execute("alter vtable vctb_dec add column ext_score decimal(8,2) from org_ntb_dec.score;")

        # own decimal columns query correctly (no 0x268f Invalid ref column type)
        tdSql.query("select ts, ext_price, ext_score from vctb_dec order by ts;")
        tdSql.checkRows(2)
        assert str(tdSql.getData(0, 1)) == "12.34", f"ext_price row0 = {tdSql.getData(0, 1)}"
        assert str(tdSql.getData(0, 2)) == "1.50", f"ext_score row0 = {tdSql.getData(0, 2)}"
        assert str(tdSql.getData(1, 1)) == "99.99", f"ext_price row1 = {tdSql.getData(1, 1)}"
        assert str(tdSql.getData(1, 2)) == "2.75", f"ext_score row1 = {tdSql.getData(1, 2)}"

        # re-query on the same connection hits the catalog cache; the cached own-col typeMod must
        # still be merged (regression: cache-hit path dropped decimal typeMod -> 0x268f on 2nd query)
        tdSql.query("select ts, ext_price, ext_score from vctb_dec order by ts;")
        tdSql.checkRows(2)
        assert str(tdSql.getData(0, 1)) == "12.34", f"ext_price row0 (cache hit) = {tdSql.getData(0, 1)}"

        # SHOW CREATE emits the declared precision/scale (not DECIMAL(0,0)). Column names are
        # backtick-quoted in the output, so match the type clause that follows each own column.
        tdSql.query("show create vtable vctb_dec;")
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"show create vctb_dec: {create_sql}")
        assert "DECIMAL(10,2)" in create_sql, f"decimal precision/scale lost: {create_sql}"
        assert "DECIMAL(8,2)" in create_sql, f"decimal precision/scale lost: {create_sql}"
        assert "DECIMAL(0,0)" not in create_sql, f"own decimal col lost typeMod: {create_sql}"

        # ins_columns reports the full decimal type
        self.check_col("vctb_dec", "ext_price", "DECIMAL(10,2)", f"{DB}.org_ntb_dec.price")
        self.check_col("vctb_dec", "ext_score", "DECIMAL(8,2)", f"{DB}.org_ntb_dec.score")

        # decimal own column survives dnode restart (covers persistence format compat)
        tdLog.info("restart dnode to verify persistence of own decimal column.")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.execute(f"use {DB};")
        self.check_col("vctb_dec", "ext_price", "DECIMAL(10,2)", f"{DB}.org_ntb_dec.price")
        tdSql.query("select ts, ext_price from vctb_dec order by ts;")
        tdSql.checkRows(2)
        assert str(tdSql.getData(0, 1)) == "12.34", f"ext_price after restart row0 = {tdSql.getData(0, 1)}"

        tdSql.execute("drop vtable if exists vctb_dec;")
        tdSql.execute("drop stable if exists vstb_dec;")
        tdSql.execute("drop table if exists org_ntb_dec;")
        print("do_ext_own_decimal .................... [passed]")

    def do_ext_own_decimal_create(self):
        tdSql.execute(f"use {DB};")

        tdSql.execute("create table org_ntb_dec2 (ts timestamp, price decimal(10,2));")
        tdSql.execute("insert into org_ntb_dec2 values (1700000000000, 55.05);")
        tdSql.execute("create stable vstb_dec2 (ts timestamp, v int) tags (loc int) virtual 1;")

        # CREATE path: declare an own decimal column inline at CREATE VTABLE time
        tdSql.execute("create vtable vctb_dec2 (v from org_ctb_0.v, ext_price decimal(10,2) from org_ntb_dec2.price) "
                      "using vstb_dec2 tags (51);")

        # own decimal column declared at create works end-to-end
        tdSql.query("select ts, ext_price from vctb_dec2 order by ts;")
        tdSql.checkRows(1)
        assert str(tdSql.getData(0, 1)) == "55.05", f"ext_price row0 = {tdSql.getData(0, 1)}"

        # SHOW CREATE round-trips the precision/scale (column name is backtick-quoted)
        tdSql.query("show create vtable vctb_dec2;")
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"show create vctb_dec2: {create_sql}")
        assert "DECIMAL(10,2)" in create_sql, f"decimal precision/scale lost: {create_sql}"
        assert "DECIMAL(0,0)" not in create_sql, f"own decimal col lost typeMod: {create_sql}"

        self.check_col("vctb_dec2", "ext_price", "DECIMAL(10,2)", f"{DB}.org_ntb_dec2.price")

        tdSql.execute("drop vtable if exists vctb_dec2;")
        tdSql.execute("drop stable if exists vstb_dec2;")
        tdSql.execute("drop table if exists org_ntb_dec2;")
        print("do_ext_own_decimal_create ............. [passed]")

    def do_ext_interleaved_churn(self):
        tdSql.execute(f"use {DB};")

        # self-contained environment: a dedicated org source, vstb and vchild so the schema state
        # never leaks into sibling cases. vstb starts with ts+v; own cols live at
        # colId >= TSDB_VCHILD_OWN_COL_ID_BASE while vstb cols stay in the normal range, so the
        # whole case doubles as a colId-gap stress test.
        tdSql.execute("create table org_ntb_ic (ts timestamp, ci int, cb bigint, cd double, cdec decimal(10,2));")
        tdSql.execute("insert into org_ntb_ic values (1700000000000, 100, 1000, 1.5, 12.34) "
                      "(1700000001000, 101, 1001, 2.5, 56.78);")
        tdSql.execute("create stable vstb_ic (ts timestamp, v int) tags (loc int) virtual 1;")
        tdSql.execute("create vtable vctb_ic (v from org_ntb_ic.ci) using vstb_ic tags (1);")

        base_cols = 2   # ts, v
        base_tags = 1   # loc

        # helper: full schema snapshot counts, used to assert consistency at every step
        def snap():
            tdSql.query("select count(*) from information_schema.ins_columns "
                        f"where db_name='{DB}' and table_name='vctb_ic';")
            return tdSql.getData(0, 0)

        # --- 1. interleave own-col and vstb-col growth: both ranges must coexist and stay ordered
        #    (vstb cols first, own cols after, regardless of add order) ---
        for i in range(3):
            tdSql.execute(f"alter vtable vctb_ic add column own_c{i} int from org_ntb_ic.ci;")
            tdSql.execute(f"alter stable vstb_ic add column stb_c{i} bigint;")
            expect = base_cols + (i + 1) + (i + 1)   # +1 own, +1 stb each round
            assert snap() == expect, f"after round {i}: cols={snap()} expect={expect}"
            # own col carries its origin ref; cascaded stb col has no ref
            self.check_col("vctb_ic", f"own_c{i}", "INT", f"{DB}.org_ntb_ic.ci")
            self.check_col("vctb_ic", f"stb_c{i}", "BIGINT", None)
        # query mixes own + cascaded columns; own_c contributes rows, stb_c is NULL
        tdSql.query("select ts, own_c0, own_c1, own_c2, stb_c0, stb_c1, stb_c2 from vctb_ic order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(0, 4, None)

        # --- 2. interleave own-tag and vstb-tag growth ---
        for i in range(3):
            tdSql.execute(f"alter vtable vctb_ic add tag own_t{i} int;")
            tdSql.execute(f"alter stable vstb_ic add tag stb_t{i} int;")
            tdSql.execute(f"alter vtable vctb_ic set tag own_t{i} = {1000 + i};")
            tdSql.execute(f"alter vtable vctb_ic set tag stb_t{i} = {2000 + i};")
        self.check_tag_num("vctb_ic", base_tags + 6)   # loc + 3 own + 3 stb
        self.check_tag_value("vctb_ic", "own_t1", "1001")
        self.check_tag_value("vctb_ic", "stb_t2", "2002")
        tdSql.query("select own_t0, own_t1, own_t2, stb_t0, stb_t1, stb_t2 from vctb_ic;")
        tdSql.checkData(0, 0, 1000)
        tdSql.checkData(0, 5, 2002)

        # --- 3. scrambled drop: remove members out of add-order and re-query survivors every step.
        #    Drops hit the middle of both ranges (non-tail memmove) for cols and tags alike.
        #    own_* members live on the vchild (alter vtable), stb_* members on the vstb
        #    (alter stable) and cascade down to the child. ---
        # (kind, name): 'v' = alter vtable, 's' = alter stable
        drop_order_col = [("v", "own_c0"), ("s", "stb_c1"), ("v", "own_c2"), ("s", "stb_c0"),
                          ("v", "own_c1"), ("s", "stb_c2")]
        remaining_cols = base_cols + 6
        for kind, name in drop_order_col:
            tbl = "vtable vctb_ic" if kind == "v" else "stable vstb_ic"
            tdSql.execute(f"alter {tbl} drop column {name};")
            remaining_cols -= 1
            assert snap() == remaining_cols, f"after drop {name}: cols={snap()} expect={remaining_cols}"
            # every surviving own column still resolves to its origin
            tdSql.query("select col_name, col_source from information_schema.ins_columns "
                        f"where db_name='{DB}' and table_name='vctb_ic' and col_source is not null order by col_name;")
            for r in range(tdSql.queryRows):
                assert tdSql.getData(r, 1) == f"{DB}.org_ntb_ic.ci", \
                    f"surviving own col {tdSql.getData(r, 0)} lost ref: {tdSql.getData(r, 1)}"
        assert snap() == base_cols

        drop_order_tag = [("v", "own_t0"), ("s", "stb_t2"), ("v", "own_t2"), ("s", "stb_t0"),
                          ("v", "own_t1"), ("s", "stb_t1")]
        remaining_tags = base_tags + 6
        for kind, name in drop_order_tag:
            tbl = "vtable vctb_ic" if kind == "v" else "stable vstb_ic"
            tdSql.execute(f"alter {tbl} drop tag {name};")
            remaining_tags -= 1
            self.check_tag_num("vctb_ic", remaining_tags)
        self.check_tag_num("vctb_ic", base_tags)

        # --- 4. mixed-type re-add churn on the same name across both ranges: an own name and a
        #    vstb name cycled with type flips; query each time before dropping. Includes a decimal
        #    flip so the typeMod/ownSchemaExt path (COL_HAS_TYPE_MOD) is exercised under churn. ---
        own_flips = [("int", "INT"), ("bigint", "BIGINT"), ("double", "DOUBLE"),
                     ("decimal(10,2)", "DECIMAL(10,2)"), ("int", "INT")]
        src = {"int": "org_ntb_ic.ci", "bigint": "org_ntb_ic.cb",
               "double": "org_ntb_ic.cd", "decimal(10,2)": "org_ntb_ic.cdec"}
        for tname, tupper in own_flips:
            tdSql.execute(f"alter vtable vctb_ic add column own_flip {tname} from {src[tname]};")
            self.check_col("vctb_ic", "own_flip", tupper, f"{DB}.{src[tname]}")
            tdSql.query("select own_flip from vctb_ic order by ts;")
            tdSql.checkRows(2)
            tdSql.execute("alter vtable vctb_ic drop column own_flip;")
        # vstb side: type flip on a cascaded column
        for tname in ["int", "bigint", "double", "int"]:
            tdSql.execute(f"alter stable vstb_ic add column stb_flip {tname};")
            self.check_col("vctb_ic", "stb_flip", tname.upper(), None)
            tdSql.execute("alter stable vstb_ic drop column stb_flip;")
        assert snap() == base_cols

        # --- 5. many rounds of full grow+shrink under a persistent connection: own/stb cols and
        #    tags added together then dropped together, with a verification query mid-round that
        #    must keep returning the baseline result once the schema is restored ---
        N_ROUNDS = 10
        for r in range(N_ROUNDS):
            tdSql.execute("alter vtable vctb_ic add column own_r int from org_ntb_ic.ci;")
            tdSql.execute("alter stable vstb_ic add column stb_r int;")
            tdSql.execute("alter vtable vctb_ic add tag ownt_r int;")
            tdSql.execute("alter stable vstb_ic add tag stbt_r int;")
            # a decimal own col each round forces the COL_HAS_TYPE_MOD/ownSchemaExt path through
            # add/drop under a persistent connection (merge ownExtSize + cache-hit tmpOwnSchemaExt)
            tdSql.execute("alter vtable vctb_ic add column own_dec decimal(10,2) from org_ntb_ic.cdec;")
            assert snap() == base_cols + 3
            self.check_tag_num("vctb_ic", base_tags + 2)
            self.check_col("vctb_ic", "own_dec", "DECIMAL(10,2)", f"{DB}.org_ntb_ic.cdec")
            # mid-round query: own cols resolve (int + decimal), cascaded stb col is NULL
            tdSql.query("select own_r, own_dec, stb_r from vctb_ic order by ts;")
            tdSql.checkData(0, 0, 100)
            tdSql.checkData(0, 1, "12.34")
            tdSql.checkData(0, 2, None)
            # 2nd query same table same connection = cache hit; decimal typeMod must survive merge
            tdSql.query("select own_dec from vctb_ic order by ts;")
            tdSql.checkData(0, 0, "12.34")
            tdSql.checkData(1, 0, "56.78")
            tdSql.execute("alter vtable vctb_ic drop column own_r;")
            tdSql.execute("alter vtable vctb_ic drop column own_dec;")
            tdSql.execute("alter stable vstb_ic drop column stb_r;")
            tdSql.execute("alter vtable vctb_ic drop tag ownt_r;")
            tdSql.execute("alter stable vstb_ic drop tag stbt_r;")
            assert snap() == base_cols
            self.check_tag_num("vctb_ic", base_tags)
        # schema fully restored to baseline; baseline query still correct
        tdSql.query("select ts, v from vctb_ic order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(1, 1, 101)

        tdSql.execute("drop vtable if exists vctb_ic;")
        tdSql.execute("drop stable if exists vstb_ic;")
        tdSql.execute("drop table if exists org_ntb_ic;")
        print("do_ext_interleaved_churn ............... [passed]")

    def do_ext_own_decimal_drop_shift(self):
        tdSql.execute(f"use {DB};")

        # dropping a NON-last own column must shift the ownSchemaExt (typeMod) array in lockstep
        # with the schema array, otherwise a surviving decimal own column inherits the dropped
        # column's typeMod slot and reads back as DECIMAL(0,0) / fails planning with 0x268f.
        # Layout: [own_i INT, own_d1 DECIMAL(10,2), own_d2 DECIMAL(8,3)] so the ext array exists
        # and both decimals sit after the column being dropped.
        tdSql.execute("create table org_ntb_ds (ts timestamp, ci int, cdec decimal(10,2), cdec2 decimal(8,3));")
        tdSql.execute("insert into org_ntb_ds values (1700000000000, 100, 12.34, 1.500) "
                      "(1700000001000, 101, 56.78, 2.250);")
        tdSql.execute("create stable vstb_ds (ts timestamp, v int) tags (loc int) virtual 1;")
        tdSql.execute("create vtable vctb_ds (v from org_ntb_ds.ci) using vstb_ds tags (1);")

        tdSql.execute("alter vtable vctb_ds add column own_i int from org_ntb_ds.ci;")
        tdSql.execute("alter vtable vctb_ds add column own_d1 decimal(10,2) from org_ntb_ds.cdec;")
        tdSql.execute("alter vtable vctb_ds add column own_d2 decimal(8,3) from org_ntb_ds.cdec2;")
        self.check_col("vctb_ds", "own_d1", "DECIMAL(10,2)", f"{DB}.org_ntb_ds.cdec")
        self.check_col("vctb_ds", "own_d2", "DECIMAL(8,3)", f"{DB}.org_ntb_ds.cdec2")

        # 1. drop the int that precedes both decimals: both typeMod entries must shift left intact
        tdSql.execute("alter vtable vctb_ds drop column own_i;")
        self.check_col("vctb_ds", "own_d1", "DECIMAL(10,2)", f"{DB}.org_ntb_ds.cdec")
        self.check_col("vctb_ds", "own_d2", "DECIMAL(8,3)", f"{DB}.org_ntb_ds.cdec2")
        tdSql.query("select own_d1, own_d2 from vctb_ds order by ts;")
        tdSql.checkRows(2)
        assert str(tdSql.getData(0, 0)) == "12.34", f"own_d1 row0 = {tdSql.getData(0, 0)}"
        assert str(tdSql.getData(0, 1)) == "1.500", f"own_d2 row0 = {tdSql.getData(0, 1)}"
        assert str(tdSql.getData(1, 1)) == "2.250", f"own_d2 row1 = {tdSql.getData(1, 1)}"
        # 2nd query on the same connection hits the catalog cache; cached typeMod stays aligned
        tdSql.query("select own_d2, own_d1 from vctb_ds order by ts;")
        assert str(tdSql.getData(0, 0)) == "1.500", f"own_d2 cache-hit row0 = {tdSql.getData(0, 0)}"

        # 2. drop the first decimal too: the survivor keeps its own typeMod, not own_d1's
        tdSql.execute("alter vtable vctb_ds drop column own_d1;")
        self.check_col("vctb_ds", "own_d2", "DECIMAL(8,3)", f"{DB}.org_ntb_ds.cdec2")
        tdSql.query("select own_d2 from vctb_ds order by ts;")
        tdSql.checkRows(2)
        assert str(tdSql.getData(0, 0)) == "1.500", f"own_d2 after d1 drop row0 = {tdSql.getData(0, 0)}"

        # 3. SHOW CREATE keeps the surviving precision/scale after both non-tail drops
        tdSql.query("show create vtable vctb_ds;")
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"show create vctb_ds: {create_sql}")
        assert "DECIMAL(8,3)" in create_sql, f"decimal precision/scale lost after drop: {create_sql}"
        assert "DECIMAL(0,0)" not in create_sql, f"own decimal col lost typeMod after drop: {create_sql}"

        # 4. dnode restart: the shifted ext array must round-trip through the persisted entry
        tdLog.info("restart dnode to verify persisted ext alignment after non-tail drop.")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.execute(f"use {DB};")
        self.check_col("vctb_ds", "own_d2", "DECIMAL(8,3)", f"{DB}.org_ntb_ds.cdec2")
        tdSql.query("select own_d2 from vctb_ds order by ts;")
        tdSql.checkRows(2)
        assert str(tdSql.getData(1, 0)) == "2.250", f"own_d2 after restart row1 = {tdSql.getData(1, 0)}"

        tdSql.execute("drop vtable if exists vctb_ds;")
        tdSql.execute("drop stable if exists vstb_ds;")
        tdSql.execute("drop table if exists org_ntb_ds;")
        print("do_ext_own_decimal_drop_shift .......... [passed]")

    # --- main ---
    def test_vct_ext_col_add_and_query(self):
        """VChildExtColTag: add own column and query

        1. alter vchild add column with origin reference, verify schema in ins_columns
        2. own column returns origin data, merged with other origins by ts
        3. multiple own columns from different origin tables
        4. vstb-level query does not see own columns
        5. describe shows own columns

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_col_add_query()

    def test_vct_ext_col_errors(self):
        """VChildExtColTag: own column error cases

        1. add column without reference is rejected
        2. duplicate names (vstb column, own column, vstb tag) are rejected
        3. bad reference target / type mismatch / tag as ref are rejected
        4. circular reference is rejected
        5. add column on physical child table stays rejected
        6. insert into vchild stays rejected

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_col_errors()

    def test_vct_ext_tag_literal(self):
        """VChildExtColTag: own tag with literal value

        1. add own tag, verify ins_tags
        2. own tag is NULL before SET TAG
        3. set literal value, verify tag query / projection / filter
        4. vstb-level query does not see own tag
        5. duplicate tag names are rejected
        6. add tag on physical child table stays rejected

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_tag_literal()

    def test_vct_ext_tag_ref(self):
        """VChildExtColTag: own tag with tag reference

        1. add own tag and attach tag reference, value resolves dynamically
        2. source tag change is reflected
        3. set literal value clears the reference
        4. literal -> ref round trip
        5. error cases: ref to data column, type mismatch, target not exist

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_tag_ref()

    def test_vct_ext_with_stb_alter(self):
        """VChildExtColTag: vstb alter after own col/tag added

        1. vstb add column cascades unref column, own column unaffected
        2. vstb add tag, own tag value unaffected
        3. vstb add column with name used by own column is rejected

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_with_stb_alter()

    def test_vct_ext_drop(self):
        """VChildExtColTag: drop scenarios for own col/tag and vstb cascade drop

        A. vchild DROP own column / own tag: schema shrinks, dropped members are no longer
           selectable; dropping a vstb cascaded column via the vchild is rejected; drop->re-add
           allocates a fresh colId (never reuses the gap); dropping an own tag with a reference
           cleans up the ref entry
        B. vstable DROP COLUMN cascades to vchild; own columns survive and stay queryable
        C. vstable DROP TAG cascades to vchild; own tag values survive and stay usable
        D. vstable "last column / last tag" guards still apply with own schema present

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 AI Agent Created (test-first for vchild ext col/tag drop)

        """
        self.do_ext_drop()

    def test_vct_ext_churn(self):
        """VChildExtColTag: repeated add/drop churn of own col/tag

        1. same-name same-type add/drop loop: each re-add allocates a fresh colId (gap never reused)
        2. same-name different-type add/drop loop: type flips; duplicate-name guard still fires
        3. cross-kind name churn: a name freed from a column is reusable as a tag (shared namespace)
        4. multi-member churn: hold several own cols/tags, drop in reverse order (non-tail memmove)
        5. tag literal/ref churn on the same name

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 AI Agent Created (churn/stress for vchild ext col/tag add+drop)

        """
        self.do_ext_churn()

    def test_vct_ext_own_query(self):
        """VChildExtColTag: query coverage over own col/tag

        1. project own column alone and merged with vstb columns by ts
        2. WHERE filter on own column (equality + range)
        3. aggregation and arithmetic over own column
        4. own tag projection, filter, combined with vstb tag and tbname
        5. distinct own tag, partition by own tag
        6. own column + vstb column mixed in WHERE/projection
        7. order by own column
        8. select * / describe layout includes own column
        9. own tag reference (dynamic) coexisting with own literal tag

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 AI Agent Created (query coverage for vchild own col/tag)

        """
        self.do_ext_own_query()

    def test_vct_ext_non_scalar(self):
        """VChildExtColTag: non-scalar own col/tag types (bool/varchar/nchar/varbinary/geometry)

        1. add own columns of every non-scalar type with an origin ref; verify ins_columns types
        2. project each non-scalar own column; values line up with the origin by ts
        3. WHERE equality filter on string (varchar/nchar unicode) own columns
        4. WHERE filter on bool own column (true / false)
        5. varbinary own column raw-bytes round trip
        6. own column from a second origin -> ts-merge exposes all rows
        7. own tags of every non-scalar type with literal values (incl geometry)
        8. own tag filter across non-scalar types
        9. distinct own tag of string type

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 AI Agent Created (non-scalar type coverage for vchild own col/tag)

        """
        self.do_ext_non_scalar()

    def test_vct_ext_stb_name_conflicts(self):
        """VChildExtColTag: vstb alter name conflicts with own col/tag

        1. vstb add column/tag with an own name is rejected
        2. vstb rename column/tag into an own name is rejected
        3. failed stb alters leave the own schema intact
        4. own tag type restrictions (json/decimal) and cross-kind name conflicts

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_stb_name_conflicts()

    def test_vct_ext_stb_name_conflicts_in_txn(self):
        """VChildExtColTag: vstb alter name conflicts inside a batch meta txn

        In-txn (txnId != 0) ALTER STABLE ADD COLUMN/ADD TAG/RENAME TAG claiming a
        vchild own member name is rejected by the precheck before the shadow op is
        recorded; no shadow residue is left and valid in-txn alters still work.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-01 AI Agent Created (review: batch meta txn name precheck)

        """
        self.do_ext_stb_name_conflicts_in_txn()

    def test_vct_ext_persistence(self):
        """VChildExtColTag: own schema survives dnode restart

        1. add own column/tag, set tag value
        2. restart dnode
        3. schema, tag value and query results stay correct

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_persistence()

    def test_vct_ext_persistent_conn(self):
        """VChildExtColTag: catalog cache invalidation on long-lived connection

        1. connection A caches vchild meta via a query
        2. connection B adds own column/tag
        3. connection A sees the new schema on the next query

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_persistent_conn()

    def test_vct_ext_show_create(self):
        """VChildExtColTag: show create contains own col/tag and is replayable

        1. show create vtable output contains own column (with ref) and own tag value
        2. the output can be replayed to recreate an equivalent table

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 AI Agent Created (test-first for vchild ext col/tag feature)

        """
        self.do_ext_show_create()

    def test_vct_ext_own_decimal_col(self):
        """VChildExtColTag: own decimal column (ALTER path) keeps precision/scale

        1. alter vtable add column <own> decimal(p,s) from <src>
        2. query returns correct decimal values (no Invalid ref column type)
        3. show create emits DECIMAL(p,s) (not DECIMAL(0,0))
        4. survives dnode restart (persistence format compat)

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-13 AI Agent Created (own-column decimal typeMod support)

        """
        self.do_ext_own_decimal()

    def test_vct_ext_own_decimal_col_create(self):
        """VChildExtColTag: own decimal column declared at CREATE VTABLE keeps precision/scale

        1. create vtable with an inline decimal own column
        2. query returns correct decimal values
        3. show create round-trips DECIMAL(p,s)

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-13 AI Agent Created (own-column decimal typeMod support)

        """
        self.do_ext_own_decimal_create()

    def test_vct_ext_interleaved_churn(self):
        """VChildExtColTag: repeated interleaved add/drop of own col/tag AND vstb col/tag

        Exercises both colId ranges (vstb normal range + own col >= OWN_COL_ID_BASE) and the tag
        namespaces together, under one persistent connection:

        1. interleave own-col + vstb-col growth; verify column ordering (vstb first, own after)
           and that each owns its origin ref / cascade NULL
        2. interleave own-tag + vstb-tag growth; verify values and queries
        3. scrambled out-of-order drop of cols and tags (non-tail memmove); re-query survivors
        4. mixed-type re-add churn on a reused name across both ranges
        5. 10 rounds of full grow+shrink with a mid-round verification query

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-13 AI Agent Created (interleaved own+vstb col/tag churn stress)

        """
        self.do_ext_interleaved_churn()

    def test_vct_ext_own_decimal_drop_shift(self):
        """VChildExtColTag: drop a non-last own column ahead of decimal own columns

        The ownSchemaExt (typeMod) array is 1:1 position-aligned with the own schema array;
        dropping a column in the middle must shift both in lockstep:

        1. [own_i INT, own_d1 DECIMAL(10,2), own_d2 DECIMAL(8,3)]; drop own_i (non-tail, ahead of
           both decimals) and verify both surviving precisions via ins_columns + query + cache-hit
        2. drop own_d1 next; own_d2 must keep DECIMAL(8,3), not inherit own_d1's typeMod slot
        3. SHOW CREATE keeps the surviving precision/scale (no DECIMAL(0,0))
        4. dnode restart round-trips the shifted ext array through the persisted entry

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-14 AI Agent Created (ownSchemaExt shift on non-tail own-column drop)

        """
        self.do_ext_own_decimal_drop_shift()
