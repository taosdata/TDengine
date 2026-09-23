# -*- coding: utf-8 -*-

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

from new_test_framework.utils import tdLog, tdSql, tdStream, StreamCheckItem


DB = "test_bit_changes"
TS = "2024-01-01 00:00:"


class TestBitChanges:
    """bit_changes(expr [, mask [, flag]]) coverage."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def _create_db(self, name=DB):
        tdSql.execute(f"drop database if exists {name}")
        tdSql.execute(f"create database {name} precision 'ms'")
        tdSql.execute(f"use {name}")

    def _scalar(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected)

    def _insert_bits(self):
        tdSql.execute("create table t_bits (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_bits values "
            f"('{TS}00.000',0)('{TS}01.000',1)('{TS}02.000',3)"
            f"('{TS}03.000',2)('{TS}04.000',0)('{TS}05.000',4)"
        )

    # --- impl ---
    def do_basic_and_flags(self):
        self._insert_bits()

        # Low bit: 0,1,1,0,0,0. The first row is never counted.
        self._scalar("select bit_changes(v) from t_bits", 1)
        self._scalar("select bit_changes(v, 1) from t_bits", 1)
        self._scalar("select bit_changes(v, 1, 0) from t_bits", 1)
        self._scalar("select bit_changes(v, 1, 1) from t_bits", 1)
        self._scalar("select bit_changes(v, 1, 2) from t_bits", 2)

        # Low two bits: 0,1,3,2,0,0.
        self._scalar("select bit_changes(v, 3, 0) from t_bits", 2)
        self._scalar("select bit_changes(v, 3, 1) from t_bits", 2)
        self._scalar("select bit_changes(v, 3, 2) from t_bits", 4)
        self._scalar("select bit_changes(v, 0, 2) from t_bits", 0)

    def do_nulls_and_boundaries(self):
        tdSql.execute("create table t_nulls (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_nulls values "
            f"('{TS}00.000',1)('{TS}01.000',NULL)('{TS}02.000',NULL)"
            f"('{TS}03.000',0)('{TS}04.000',1)"
        )
        # NULL is transparent: 1,NULL,NULL,0,1 -> 1->0 and 0->1.
        self._scalar("select bit_changes(v, 1, 2) from t_nulls", 2)

        tdSql.execute("create table t_head_null (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_head_null values "
            f"('{TS}00.000',NULL)('{TS}01.000',1)('{TS}02.000',0)"
        )
        self._scalar("select bit_changes(v, 1, 2) from t_head_null", 1)

        tdSql.execute("create table t_all_null (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_all_null values "
            f"('{TS}00.000',NULL)('{TS}01.000',NULL)"
        )
        self._scalar("select bit_changes(v, 1, 2) from t_all_null", 0)

        tdSql.execute("create table t_one (ts timestamp, v int)")
        tdSql.execute(f"insert into t_one values ('{TS}00.000',1)")
        self._scalar("select bit_changes(v) from t_one", 0)
        tdSql.execute("create table t_empty (ts timestamp, v int)")
        tdSql.query("select bit_changes(v) from t_empty")
        tdSql.checkRows(0)

    def do_type_matrix(self):
        tdSql.execute(
            "create table t_types (ts timestamp, cti tinyint, csi smallint, "
            "ci int, cbi bigint, cuti tinyint unsigned, cusi smallint unsigned, "
            "cui int unsigned, cub bigint unsigned, cb bool)"
        )
        tdSql.execute(
            "insert into t_types values "
            f"('{TS}00.000',0,0,0,0,0,0,0,0,false)"
            f"('{TS}01.000',1,1,1,1,1,1,1,1,true)"
            f"('{TS}02.000',0,0,0,0,0,0,0,0,false)"
        )
        for column in (
            "cti", "csi", "ci", "cbi", "cuti", "cusi", "cui", "cub", "cb"
        ):
            self._scalar(f"select bit_changes({column}, 1, 2) from t_types", 2)

        tdSql.query("select bit_changes(ci) from t_types")
        tdSql.checkRows(1)
        assert tdSql.checkDataType(0, 0, "BIGINT")

        tdSql.execute("create table t_non_integer (ts timestamp, f double, s nchar(8))")
        tdSql.execute(
            "insert into t_non_integer values "
            f"('{TS}00.000',1.0,'a')('{TS}01.000',2.0,'b')"
        )
        tdSql.error("select bit_changes(f) from t_non_integer")
        tdSql.error("select bit_changes(s) from t_non_integer")

    def do_parameters_and_timeline(self):
        tdSql.execute("create table t_params (ts timestamp, v int, c2 int)")
        tdSql.execute(f"insert into t_params values ('{TS}00.000',1,1)")
        tdSql.error("select bit_changes() from t_params")
        tdSql.error("select bit_changes(v, 1, 0, 1) from t_params")
        tdSql.error("select bit_changes(v, -1) from t_params")
        tdSql.error("select bit_changes(v, 9223372036854775808) from t_params")
        tdSql.error("select bit_changes(v, 1, 3) from t_params")
        tdSql.error("select bit_changes(v, c2) from t_params")
        tdSql.error("select bit_changes(v, '1') from t_params")
        tdSql.error("select bit_changes(v, 1.5) from t_params")
        tdSql.error("select bit_changes(v, 1, c2) from t_params")
        tdSql.error("select bit_changes(vgroup_id) from information_schema.ins_vgroups")

        tdSql.execute("create table t_pk (ts timestamp, pk int primary key, v int)")
        tdSql.execute(
            f"insert into t_pk values ('{TS}00.000',1,0)"
            f"('{TS}01.000',2,1)"
        )
        tdSql.query("select bit_changes(v, 1, 2), last(v) from t_pk")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.execute("create table t_dup (ts timestamp, v int) tags (g int)")
        tdSql.execute("create table d1 using t_dup tags(1)")
        tdSql.execute("create table d2 using t_dup tags(2)")
        tdSql.execute(f"insert into d1 values ('{TS}00.000',1)")
        tdSql.execute(f"insert into d2 values ('{TS}00.000',0)")
        tdSql.error("select bit_changes(v) from t_dup")

        tdSql.execute("create table t_order (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_order values "
            f"('{TS}00.000',0)('{TS}01.000',1)('{TS}02.000',0)"
        )
        self._scalar(
            "select bit_changes(v, 1, 2) from "
            "(select ts, v from t_order order by ts desc)", 2
        )

    def do_tables_groups_and_windows(self):
        tdSql.execute("create table st (ts timestamp, v int) tags (g int)")
        tdSql.execute("create table c1 using st tags(1)")
        tdSql.execute("create table c2 using st tags(2)")
        tdSql.execute(
            f"insert into c1 values ('{TS}00.000',0)('{TS}01.000',1)"
            f"('{TS}02.000',1)"
        )
        tdSql.execute(
            f"insert into c2 values ('{TS}00.000',1)('{TS}01.000',1)"
            f"('{TS}02.000',0)"
        )
        self._scalar("select bit_changes(v, 1, 2) from c1", 1)
        tdSql.query("select tbname, bit_changes(v, 1, 2) from st partition by tbname order by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.query("select g, bit_changes(v, 1, 2) from st partition by g order by g")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)

        # Each interval is independent: [0,2) has 0->1, [2,4) has 1->0.
        tdSql.query("select _wstart, bit_changes(v, 1, 2) from c1 interval(2s)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 0)
        tdSql.query("select bit_changes(v, 1, 2) from c1 count_window(2)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 0)

        tdSql.execute("create table t_state (ts timestamp, v int, state_col int)")
        tdSql.execute(
            f"insert into t_state values ('{TS}00.000',0,1)"
            f"('{TS}01.000',1,1)('{TS}02.000',1,2)('{TS}03.000',0,2)"
        )
        tdSql.query("select bit_changes(v, 1, 2) from t_state state_window(state_col)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.query("select bit_changes(v, 1, 2) from t_state session(ts, 5s)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.execute("create table t_pk_window (ts timestamp, pk int primary key, v int)")
        tdSql.execute(
            f"insert into t_pk_window values ('{TS}00.000',1,0)"
            f"('{TS}01.000',1,1)('{TS}02.000',1,0)"
        )
        tdSql.query("select _wstart, bit_changes(v, 1, 2) from t_pk_window count_window(2, 1)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 0)


    def do_composition(self):
        self._scalar("select bit_changes(abs(v), 1, 2) from t_bits", 2)
        self._scalar(
            "select bit_changes(v, 1, 2) from "
            "(select ts, v from t_bits order by ts)", 2
        )
        tdSql.query(
            "select bit_changes(v, 1, 2) from t_bits "
            "union all select bit_changes(v, 1, 2) from t_bits"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.query("select bit_changes(v, 1, 2), count(*), last(v) from t_bits")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 6)
        tdSql.query("select bit_changes(v, 1, 0), bit_changes(v, 1, 1), last(v) from t_bits")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute("create table t_direction (ts timestamp, v int)")
        tdSql.execute(f"insert into t_direction values ('{TS}00.000',0)('{TS}01.000',1)")
        tdSql.query("select bit_changes(v, 1, 0), bit_changes(v, 1, 1), last(v) from t_direction")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 0)
        tdSql.query(
            "select bit_changes(v, 1, 0), bit_changes(v, 1, 1) from "
            "(select ts, v from t_direction order by ts desc)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 0)


    def do_stream_parameter_validation(self):
        tdSql.execute("drop database if exists bc_stream_invalid")
        tdSql.execute("create database bc_stream_invalid vgroups 1")
        tdSql.execute("use bc_stream_invalid")
        tdSql.execute("create table t_double(ts timestamp, v double)")
        tdSql.execute("create table t_nchar(ts timestamp, v nchar(8))")
        tdSql.error(
            "create stream s_double interval(2s) from t_double into out_double as "
            "select _twstart, bit_changes(v) from t_double"
        )
        tdSql.error(
            "create stream s_nchar interval(2s) from t_nchar into out_nchar as "
            "select _twstart, bit_changes(v) from t_nchar"
        )

    # ================================================================== batch entry
    def test_bit_changes_batch(self):
        """Function: bit_changes batch coverage.

        1. Verify default mask, directional flags, and first-row behavior.
        2. Verify NULL skipping, mask zero, empty and single-row boundaries.
        3. Verify signed/unsigned integer and BOOL inputs with BIGINT output.
        4. Verify invalid parameters, system-table rejection, duplicate timestamps,
           and ordered subqueries.
        5. Verify ordinary/super tables, grouping, windows, nested queries, UNION ALL,
           and aggregate composition.

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: TS-6486

        History:
            - 2026-09-20 Codex Added bit_changes batch coverage from TS

        """
        self._create_db()
        self.do_basic_and_flags()
        self.do_nulls_and_boundaries()
        self.do_type_matrix()
        self.do_parameters_and_timeline()
        self.do_tables_groups_and_windows()
        self.do_composition()
        tdLog.success(f"{__file__} batch executed")

    # ================================================================== stream entry
    def test_bit_changes_stream(self):
        """Stream: bit_changes window and partition coverage.

        1. Verify interval directional counting and NULL skipping.
        2. Verify late-row recomputation and supertable partition isolation.

        Catalog:
            - Functions/TimeSeries/Stream

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: TS-6486

        History:
            - 2026-09-20 Codex Added bit_changes stream coverage from TS

        """
        tdStream.ensureSnode()
        self.do_stream_parameter_validation()
        tdStream.checkAll(
            [
                self.StreamInterval(),
                self.StreamIgnoreNull(),
                self.StreamOutOfOrder(),
                self.StreamSuperPartition(),
            ]
        )
        tdLog.success(f"{__file__} stream executed")

    class StreamInterval(StreamCheckItem):
        def __init__(self):
            self.db = "bc_stream_interval"

        def create(self):
            tdSql.execute("create database bc_stream_interval vgroups 1 buffer 8")
            tdSql.execute("use bc_stream_interval")
            tdSql.execute("create table t1(ts timestamp, v int)")
            tdSql.execute(
                "create stream s_bc interval(2s) sliding(2s) from t1 "
                "stream_options(max_delay(3s)) into bc_out as "
                "select _twstart as s, bit_changes(v,1,2) c "
                "from t1 where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(
                "insert into t1 values(1704067200000,0)(1704067201000,1)"
                "(1704067202000,1)(1704067203000,0)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                "select s, c from bc_out order by s",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 1
                and tdSql.getData(1, 1) == 1,
            )

    class StreamIgnoreNull(StreamCheckItem):
        def __init__(self):
            self.db = "bc_stream_null"

        def create(self):
            tdSql.execute("create database bc_stream_null vgroups 1 buffer 8")
            tdSql.execute("use bc_stream_null")
            tdSql.execute("create table t1(ts timestamp, v int)")
            tdSql.execute(
                "create stream s_bc interval(4s) sliding(4s) from t1 "
                "stream_options(max_delay(3s)) into bc_out as "
                "select _twstart as s, bit_changes(v,1,2) c "
                "from t1 where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute(
                "insert into t1 values(1704067200000,1)(1704067201000,NULL)"
                "(1704067202000,0)(1704067203000,1)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from bc_out",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 2,
            )

    class StreamOutOfOrder(StreamCheckItem):
        def __init__(self):
            self.db = "bc_stream_ooo"

        def create(self):
            tdSql.execute("create database bc_stream_ooo vgroups 1 buffer 8")
            tdSql.execute("use bc_stream_ooo")
            tdSql.execute("create table t1(ts timestamp, v int)")
            tdSql.execute(
                "create stream s_bc interval(4s) sliding(4s) from t1 "
                "stream_options(max_delay(3s)) into bc_out as "
                "select _twstart as s, bit_changes(v,1,2) c "
                "from t1 where ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute("insert into t1 values(1704067200000,0)(1704067203000,1)")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from bc_out",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 1,
            )

        def insert2(self):
            tdSql.execute("insert into t1 values(1704067201000,1)(1704067202000,0)")

        def check2(self):
            tdSql.checkResultsByFunc(
                "select c from bc_out",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 3,
            )

    class StreamSuperPartition(StreamCheckItem):
        def __init__(self):
            self.db = "bc_stream_super"

        def create(self):
            tdSql.execute("create database bc_stream_super vgroups 2 buffer 8")
            tdSql.execute("use bc_stream_super")
            tdSql.execute("create table st(ts timestamp, v int) tags(g int)")
            tdSql.execute("create table c1 using st tags(1)")
            tdSql.execute("create table c2 using st tags(2)")
            tdSql.execute(
                "create stream s_bc interval(4s) sliding(4s) from st "
                "partition by tbname stream_options(max_delay(3s)) into bc_out as "
                "select _twstart as s, bit_changes(v,1,2) c from st "
                "where tbname=%%1 and ts >= _twstart and ts < _twend"
            )

        def insert1(self):
            tdSql.execute("insert into c1 values(1704067200000,0)(1704067201000,1)(1704067202000,1)")
            tdSql.execute("insert into c2 values(1704067200000,1)(1704067201000,1)(1704067202000,0)")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from bc_out order by c",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == 1
                and tdSql.getData(1, 0) == 1,
            )
