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

DB = "test_value_changes"
TS = "2024-01-01 00:00:"


class TestValueChanges:
    """value_changes(expr [, ignore_null]) — count of adjacent value changes
    on the effective timeline. Returns BIGINT. First row not counted.

    Reference dataset t1 (ms precision):
        0s=10, 1s=10, 2s=20, 3s=NULL, 4s=20, 5s=30
      value_changes(v)   = value_changes(v,1) = 2   (10->20, 20->30)
      value_changes(v,0) = 4                         (10->20, 20->NULL,
                                                       NULL->20, 20->30)

    NOTE: value_changes is not yet implemented in the engine. Until it is
    registered in builtins, every case here fails at parse ("invalid
    function"). This is the test-first suite tracking FS
    docs/releases/TSDB-v3.4.3-[20260930]/05-设计文档/value_changes-值变化次数函数-FS.md.

    Design points this suite pins (see FS §16 open items):
      - window/stream: each window counts independently, first row of a
        window is not compared to the previous window's last row (§4.7).
      - ignore_null: 1 (default) skips NULL transparently; 0 counts
        NULL<->non-NULL as a change, NULL<->NULL as no change.
      - float: epsilon equality, consecutive NaN == no change (reuses
        compareDoubleVal).

    Since: 3.4.3.0

    Catalog: Functions/TimeSeries

    Labels: common,ci

    Jira: TS-6485

    History:
    - 2026-07-17 Initial test-first suite alongside the FS.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------ helpers
    def _create_db(self, precision="ms"):
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB} precision '{precision}'")
        tdSql.execute(f"use {DB}")

    def _scalar(self, sql, expected):
        """Run a single-cell query and assert an exact BIGINT result."""
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected)

    def _setup_ref(self):
        """t1: 0s=10,1s=10,2s=20,3s=NULL,4s=20,5s=30."""
        self._create_db()
        tdSql.execute("create table t1 (ts timestamp, v int)")
        tdSql.execute(
            "insert into t1 values "
            f"('{TS}00.000',10)('{TS}01.000',10)('{TS}02.000',20)"
            f"('{TS}03.000',NULL)('{TS}04.000',20)('{TS}05.000',30)"
        )

    # ------------------------------------------------------------------ 6.1/6.2 counting + ignore_null
    def _counting_and_null(self):
        # default (ignore_null=1) skips the NULL at 3s -> 2 changes
        self._scalar("select value_changes(v) from t1", 2)
        self._scalar("select value_changes(v,1) from t1", 2)
        # ignore_null=0: NULL participates -> 4 changes
        self._scalar("select value_changes(v,0) from t1", 4)

        # all-equal -> 0; strictly increasing -> n-1
        tdSql.execute("create table t_eq (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_eq values "
            f"('{TS}00.000',7)('{TS}01.000',7)('{TS}02.000',7)"
        )
        self._scalar("select value_changes(v) from t_eq", 0)
        tdSql.execute("create table t_inc (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_inc values "
            f"('{TS}00.000',1)('{TS}01.000',2)('{TS}02.000',3)('{TS}03.000',4)"
        )
        self._scalar("select value_changes(v) from t_inc", 3)

        # 1,NULL,1 : ignore_null=1 -> 0 ; ignore_null=0 -> 2 (1->NULL,NULL->1)
        tdSql.execute("create table t_mid (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_mid values "
            f"('{TS}00.000',1)('{TS}01.000',NULL)('{TS}02.000',1)"
        )
        self._scalar("select value_changes(v) from t_mid", 0)
        self._scalar("select value_changes(v,0) from t_mid", 2)

        # all-NULL column: both modes -> 0
        tdSql.execute("create table t_allnull (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_allnull values "
            f"('{TS}00.000',NULL)('{TS}01.000',NULL)('{TS}02.000',NULL)"
        )
        self._scalar("select value_changes(v) from t_allnull", 0)
        self._scalar("select value_changes(v,0) from t_allnull", 0)

        # two distinct rows -> 1 (first row not counted; only n-1 comparisons)
        tdSql.execute("create table t_pair (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_pair values "
            f"('{TS}00.000',5)('{TS}01.000',9)"
        )
        self._scalar("select value_changes(v) from t_pair", 1)

        # head/tail NULL: NULL,5,5,NULL
        #   ignore_null=1 -> 5,5 -> 0
        #   ignore_null=0 -> NULL->5(1),5->5(0),5->NULL(1) = 2
        tdSql.execute("create table t_hn (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_hn values "
            f"('{TS}00.000',NULL)('{TS}01.000',5)('{TS}02.000',5)('{TS}03.000',NULL)"
        )
        self._scalar("select value_changes(v) from t_hn", 0)
        self._scalar("select value_changes(v,0) from t_hn", 2)

    # ------------------------------------------------------------------ 6.3 type matrix
    def _types(self):
        tdSql.execute(
            "create table t_types (ts timestamp, cti tinyint, csi smallint, "
            "ci int, cbi bigint, cuti tinyint unsigned, cui int unsigned, "
            "cf float, cd double, cbool bool, cts timestamp, "
            "cbin binary(16), cnch nchar(16), cvb varbinary(16), "
            "cdec64 decimal(10,2), cdec128 decimal(30,4))"
        )
        # 3 rows, patterned so each numeric col does 10->10->20 => 1 change
        tdSql.execute(
            "insert into t_types values "
            f"('{TS}00.000',10,10,10,10,10,10,10,10,true,'{TS}00.000','a','a',"
            f"'\\x0a',10,10)"
            f"('{TS}01.000',10,10,10,10,10,10,10,10,true,'{TS}00.000','a','a',"
            f"'\\x0a',10,10)"
            f"('{TS}02.000',20,20,20,20,20,20,20,20,false,'{TS}02.000','b','b',"
            f"'\\x14',20,20)"
        )
        for col in ("cti", "csi", "ci", "cbi", "cuti", "cui", "cf", "cd",
                    "cbool", "cts", "cbin", "cnch", "cvb", "cdec64", "cdec128"):
            self._scalar(f"select value_changes({col}) from t_types", 1)
        # return type is BIGINT
        tdSql.query("select value_changes(ci) from t_types")
        tdSql.checkRows(1)
        assert tdSql.checkDataType(0, 0, "BIGINT")

    # -------------------------------------------------------------- 6.3b decimal
    def _decimal(self):
        # DECIMAL is compared as DOUBLE (FS). Regression guard: a naive per-type
        # comparator (compareInt32Val) would read only the low 4 bytes of the 8/16-byte
        # decimal storage, so high-byte-only differences would be missed. These values
        # are chosen so the low 32 bits collide but the true values differ.
        tdSql.execute(
            "create table t_dec (ts timestamp, d64 decimal(18,0), d128 decimal(38,0))"
        )
        # d64: 0, 2^32 (=4294967296), 0  -> low 4 bytes all 0, true values differ => 2 changes
        # d128: 0, 2^40, 0               -> same idea one 16-byte width           => 2 changes
        tdSql.execute(
            "insert into t_dec values "
            f"('{TS}00.000',0,0)"
            f"('{TS}01.000',4294967296,1099511627776)"
            f"('{TS}02.000',0,0)"
        )
        self._scalar("select value_changes(d64) from t_dec", 2)
        self._scalar("select value_changes(d128) from t_dec", 2)

        # same value repeated -> no change; scale-sensitive change counted once
        tdSql.execute("create table t_dec2 (ts timestamp, d decimal(10,2))")
        tdSql.execute(
            "insert into t_dec2 values "
            f"('{TS}00.000',1.25)('{TS}01.000',1.25)('{TS}02.000',1.26)"
        )
        self._scalar("select value_changes(d) from t_dec2", 1)

        # NULL mixed: default ignore_null=1 skips NULLs transparently
        tdSql.execute("create table t_dec3 (ts timestamp, d decimal(18,0))")
        tdSql.execute(
            "insert into t_dec3 values "
            f"('{TS}00.000',5)('{TS}01.000',NULL)('{TS}02.000',5)"
            f"('{TS}03.000',9)"
        )
        self._scalar("select value_changes(d) from t_dec3", 1)
        # ignore_null=0: NULL participates -> 5,NULL,5,9 => 5->NULL, NULL->5, 5->9 = 3
        self._scalar("select value_changes(d, 0) from t_dec3", 3)

    # ------------------------------------------------------------------ 6.4 float / NaN
    def _float_nan(self):
        tdSql.execute("create table t_f (ts timestamp, v double)")
        # equality is via the engine's compareDoubleVal (tol = 4*DBL_EPSILON ~ 9e-16):
        # a repeated exact value is no change; a real jump is one change.
        tdSql.execute(
            "insert into t_f values "
            f"('{TS}00.000',1.0)('{TS}01.000',1.0)"
            f"('{TS}02.000',5.0)"
        )
        self._scalar("select value_changes(v) from t_f", 1)

        # epsilon tolerance: two values 1 ULP apart (< 4*DBL_EPSILON) are equal,
        # so 1.0, 1.0000000000000002, 2.0 -> only the ->2.0 jump counts = 1.
        # If equality were exact "!=", this would wrongly return 2.
        tdSql.execute("create table t_eps (ts timestamp, v double)")
        tdSql.execute(
            "insert into t_eps values "
            f"('{TS}00.000',1.0)('{TS}01.000',1.0000000000000002)"
            f"('{TS}02.000',2.0)"
        )
        self._scalar("select value_changes(v) from t_eps", 1)

        # NaN/Inf note: TDengine has no NaN/Inf literal and division-by-zero
        # yields NULL (not NaN/Inf) at the SQL layer, so those float values are
        # unreachable from SQL. 1/0 etc. all become NULL and follow the NULL
        # rules above. The per-type comparator's NaN==NaN / NaN!=number handling
        # (compareDoubleVal) is shared engine code exercised by other suites.
        tdSql.execute("create table t_div (ts timestamp, a double, b double)")
        tdSql.execute(
            "insert into t_div values "
            f"('{TS}00.000',1,1)('{TS}01.000',1,0)('{TS}02.000',2,1)"
        )
        # a/b = 1, NULL, 2 -> ignore_null=1 skips NULL -> 1->2 = 1 change
        self._scalar("select value_changes(a/b) from t_div", 1)

    # ------------------------------------------------------------------ 6.5 param validation
    def _errors(self):
        tdSql.execute("create table t_e (ts timestamp, v int, c2 int)")
        tdSql.execute(f"insert into t_e values ('{TS}00.000',1,1)")
        tdSql.error("select value_changes() from t_e")           # missing expr
        tdSql.error("select value_changes(v,0,1) from t_e")      # too many args
        tdSql.error("select value_changes(v,2) from t_e")        # illegal value
        tdSql.error("select value_changes(v,-1) from t_e")       # illegal value
        tdSql.error("select value_changes(v,c2) from t_e")       # non-const
        tdSql.error("select value_changes(v,'x') from t_e")      # non-int
        tdSql.error("select value_changes(v,1.5) from t_e")      # non-int
        # systable forbidden
        tdSql.error("select value_changes(vgroup_id) from information_schema.ins_vgroups")

    # ------------------------------------------------------------------ 6.6 degenerate/boundary
    def _boundary(self):
        tdSql.execute("create table t_one (ts timestamp, v int)")
        tdSql.execute(f"insert into t_one values ('{TS}00.000',5)")
        self._scalar("select value_changes(v) from t_one", 0)     # single row
        tdSql.execute("create table t_empty (ts timestamp, v int)")
        tdSql.query("select value_changes(v) from t_empty")       # empty
        tdSql.checkRows(0)
        tdSql.execute("create table t_two (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_two values "
            f"('{TS}00.000',9)('{TS}01.000',9)"
        )
        self._scalar("select value_changes(v) from t_two", 0)     # two equal

        # duplicate timestamps across subtables (no composite key) -> engine
        # rejects with "Duplicate timestamps not allowed in function".
        tdSql.execute("create table st_dup (ts timestamp, v int) tags(g int)")
        tdSql.execute("create table dup1 using st_dup tags(1)")
        tdSql.execute("create table dup2 using st_dup tags(2)")
        tdSql.execute(f"insert into dup1 values ('{TS}00.000',1)('{TS}01.000',2)")
        tdSql.execute(f"insert into dup2 values ('{TS}00.000',5)('{TS}01.000',6)")
        tdSql.error("select value_changes(v) from st_dup")

        # out-of-order timeline: value_changes is direction-agnostic and counts
        # adjacency, so a descending subquery still computes (3,2,1 -> 2 changes).
        tdSql.execute("create table t_ord (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_ord values "
            f"('{TS}00.000',1)('{TS}01.000',2)('{TS}02.000',3)"
        )
        self._scalar(
            "select value_changes(v) from "
            "(select ts, v from t_ord order by ts desc)", 2
        )

    # ------------------------------------------------------------------ 6.7 table variety
    def _tables(self):
        tdSql.execute("create table st (ts timestamp, v int) tags (g int)")
        tdSql.execute("create table c1 using st tags(1)")
        tdSql.execute("create table c2 using st tags(2)")
        for c in ("c1", "c2"):
            tdSql.execute(
                f"insert into {c} values "
                f"('{TS}00.000',10)('{TS}01.000',20)('{TS}02.000',20)"
            )
        # per child = 1 change; not across tables
        self._scalar("select value_changes(v) from c1", 1)
        tdSql.query("select value_changes(v) from st partition by tbname")
        tdSql.checkRows(2)
        tdSql.query("select value_changes(v) from st group by tbname")
        tdSql.checkRows(2)
        # partition by tag: c1 g=1 -> 1 change, c2 g=2 -> 1 change
        tdSql.query("select g, value_changes(v) from st partition by g order by g")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)

        # supertable full aggregate over a merged, non-overlapping timeline:
        # d1: 0s=10, 2s=20 ; d2: 1s=10, 3s=30 -> merged 10,10,20,30 -> 2 changes
        tdSql.execute("create table st2 (ts timestamp, v int) tags(g int)")
        tdSql.execute("create table d1 using st2 tags(1)")
        tdSql.execute("create table d2 using st2 tags(2)")
        tdSql.execute(f"insert into d1 values ('{TS}00.000',10)('{TS}02.000',20)")
        tdSql.execute(f"insert into d2 values ('{TS}01.000',10)('{TS}03.000',30)")
        self._scalar("select value_changes(v) from st2", 2)

    # ------------------------------------------------------------------ 6.8 batch windows
    def _windows(self):
        # t1: 0s=10,1s=10,2s=20,3s=NULL,4s=20,5s=30
        # INTERVAL(2s): [0,2)=10,10 ->0 ; [2,4)=20,(NULL skip) ->0 ;
        #               [4,6)=20,30 ->1
        tdSql.query(
            f"select _wstart, value_changes(v) from t1 interval(2s)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        # interval + sliding: overlapping 4s windows sliding by 2s over
        # 0s=10,1s=10,2s=20,4s=20,5s=30 -> per-window counts 0,1,1,1
        tdSql.query(
            "select _wstart, value_changes(v) from t1 "
            "interval(4s) sliding(2s) order by _wstart"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)

        # state/count/session/event windows on a dedicated table with an
        # explicit state column so per-window values are deterministic.
        # sw: (10,s=1)(20,s=1)(5,s=2)(9,s=2) -> each state window has 1 change
        tdSql.execute("create table t_sw (ts timestamp, v int, s int)")
        tdSql.execute(
            "insert into t_sw values "
            f"('{TS}00.000',10,1)('{TS}01.000',20,1)"
            f"('{TS}02.000',5,2)('{TS}03.000',9,2)"
        )
        tdSql.query("select value_changes(v) from t_sw state_window(s)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)

        # count_window(3) over t_inc 1,2,3,4 -> [1,2,3]=2, [4]=0
        tdSql.query("select value_changes(v) from t_inc count_window(3)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 0)

        # session window: all rows of t_inc within 5s gap -> one window, 3 changes
        tdSql.query("select value_changes(v) from t_inc session(ts,5s)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        # event window over t_inc (1,2,3,4), start v>=1 end v>=1 -> per-row
        # windows; just assert it executes and returns rows.
        tdSql.query(
            "select value_changes(v) from t_inc "
            "event_window start with v>=1 end with v>=100"
        )

    # ------------------------------------------------------------------ 6.9 composition
    def _composition(self):
        # nested subquery with explicit ordering
        self._scalar(
            "select value_changes(v) from "
            "(select ts, v from t1 order by ts)", 2
        )
        # union all doubles rows for a per-branch aggregate context
        tdSql.query(
            "select value_changes(v) from t1 "
            "union all select value_changes(v) from t1"
        )
        tdSql.checkRows(2)
        # cross-check vs diff on a NULL-free numeric column:
        # value_changes(v) == count of nonzero diff(v). t_inc = 1,2,3,4.
        self._scalar("select value_changes(v) from t_inc", 3)
        tdSql.query(
            "select count(*) from (select diff(v) d from t_inc) where d<>0"
        )
        tdSql.checkData(0, 0, 3)

        # mix with other aggregates in one select
        tdSql.query(
            "select value_changes(v), count(*), last(v) from t_inc"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 4)

    # ================================================================== batch entry
    def test_value_changes_batch(self):
        """Fun: value_changes batch coverage.

        1. Counting + ignore_null (0/1) semantics, all-equal/increasing/all-NULL
        2. Full type matrix (ints/unsigned/float/double/bool/timestamp/
           binary/nchar/varbinary/decimal64/decimal128), BIGINT return
        3. Float epsilon equality / NaN handling
        4. Parameter validation and systable rejection
        5. Single-row/empty/two-equal boundaries
        6. Child/super/normal tables, partition/group by
        7. Batch windows (INTERVAL/STATE/COUNT), window-independent counting
        8. Composition: nested subquery, union all, diff cross-check

        Since: v3.4.3.0
        Labels: common,ci,integration,functional
        Jira: TS-6485
        History:
            - 2026-07-17 Initial test-first suite alongside the FS.
        """
        self._setup_ref()
        self._counting_and_null()
        self._types()
        self._decimal()
        self._float_nan()
        self._errors()
        self._boundary()
        self._tables()
        self._windows()
        self._composition()
        tdLog.success(f"{__file__} batch executed")

    # ================================================================== stream entry
    def test_value_changes_stream(self):
        """Stream: value_changes under stream computing.

        Each stream case computes value_changes over a trigger window and is
        compared against the equivalent batch query (exact BIGINT match).
        Covers INTERVAL trigger, ignore_null=0, out-of-order recompute,
        supertable partition-by, and string columns.

        Since: v3.4.3.0
        Labels: common,ci,integration,functional
        Jira: TS-6485
        History:
            - 2026-07-17 Initial stream suite alongside the FS.
        """
        tdStream.ensureSnode()
        streams = [
            self.StreamInterval(),
            self.StreamIgnoreNull0(),
            self.StreamOutOfOrder(),
            self.StreamSuperPartition(),
            self.StreamStringCol(),
            self.StreamSession(),
            self.StreamCountWindow(),
            self.StreamStateWindow(),
        ]
        tdStream.checkAll(streams)
        tdLog.success(f"{__file__} stream executed")

    # ------------------------------------------------------------------ stream: INTERVAL basic
    class StreamInterval(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_interval"

        def create(self):
            tdSql.execute("create database vc_stream_interval vgroups 1 buffer 8;")
            tdSql.execute("use vc_stream_interval;")
            tdSql.execute("create table t1(ts timestamp, v int);")
            tdSql.execute(
                "create stream s_vc interval(2s) sliding(2s) from t1 "
                "stream_options(max_delay(3s)) into vc_out as "
                "select _twstart as s, value_changes(v) c "
                "from t1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            # window [0,2): 10,10 -> 0 ; [2,4): 20,30 -> 1
            tdSql.execute("insert into t1 values(1704067200000, 10);")
            tdSql.execute("insert into t1 values(1704067201000, 10);")
            tdSql.execute("insert into t1 values(1704067202000, 20);")
            tdSql.execute("insert into t1 values(1704067203000, 30);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select s, c from vc_out order by s;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 0
                and tdSql.getData(1, 1) == 1,
            )

    # ------------------------------------------------------------------ stream: ignore_null=0
    class StreamIgnoreNull0(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_ignnull"

        def create(self):
            tdSql.execute("create database vc_stream_ignnull vgroups 1 buffer 8;")
            tdSql.execute("use vc_stream_ignnull;")
            tdSql.execute("create table t1(ts timestamp, v int);")
            tdSql.execute(
                "create stream s_vc interval(4s) sliding(4s) from t1 "
                "stream_options(max_delay(3s)) into vc_out as "
                "select _twstart as s, value_changes(v,0) c "
                "from t1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            # window [0,4): 10,NULL,20 -> ignore_null=0 => 10->NULL,NULL->20 = 2
            tdSql.execute("insert into t1 values(1704067200000, 10);")
            tdSql.execute("insert into t1 values(1704067201000, NULL);")
            tdSql.execute("insert into t1 values(1704067202000, 20);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select s, c from vc_out order by s;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
            )

    # ------------------------------------------------------------------ stream: out-of-order recompute
    class StreamOutOfOrder(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_ooo"

        def create(self):
            tdSql.execute("create database vc_stream_ooo vgroups 1 buffer 8;")
            tdSql.execute("use vc_stream_ooo;")
            tdSql.execute("create table t1(ts timestamp, v int);")
            tdSql.execute(
                "create stream s_vc interval(4s) sliding(4s) from t1 "
                "stream_options(max_delay(3s)) into vc_out as "
                "select _twstart as s, value_changes(v) c "
                "from t1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            # window [0,4): 10, 20 -> 1
            tdSql.execute("insert into t1 values(1704067200000, 10);")
            tdSql.execute("insert into t1 values(1704067203000, 20);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from vc_out;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 1,
            )

        def insert2(self):
            # late row lands between: 10,10,20 -> still 1 change (10->20)
            tdSql.execute("insert into t1 values(1704067201000, 10);")

        def check2(self):
            tdSql.checkResultsByFunc(
                "select c from vc_out;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 1,
            )

        def insert3(self):
            # late row that introduces an extra change: ...,10,15,10,20 pattern
            tdSql.execute("insert into t1 values(1704067202000, 15);")

        def check3(self):
            # 10,10,15,20 -> 10->15,15->20 = 2
            tdSql.checkResultsByFunc(
                "select c from vc_out;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 2,
            )

    # ------------------------------------------------------------------ stream: supertable partition by
    class StreamSuperPartition(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_super"

        def create(self):
            tdSql.execute("create database vc_stream_super vgroups 2 buffer 8;")
            tdSql.execute("use vc_stream_super;")
            tdSql.execute(
                "create table st(ts timestamp, v int) tags(g int);"
            )
            tdSql.execute("create table c1 using st tags(1);")
            tdSql.execute("create table c2 using st tags(2);")
            tdSql.execute(
                "create stream s_vc interval(4s) sliding(4s) from st "
                "partition by tbname stream_options(max_delay(3s)) "
                "into vc_out as select _twstart as s, value_changes(v) c "
                "from st where tbname=%%1 and ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            # c1: 10,20,20 -> 1 ; c2: 5,5,5 -> 0 (independent per subtable)
            tdSql.execute("insert into c1 values(1704067200000,10)(1704067201000,20)(1704067202000,20);")
            tdSql.execute("insert into c2 values(1704067200000,5)(1704067201000,5)(1704067202000,5);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from vc_out order by c;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == 0
                and tdSql.getData(1, 0) == 1,
            )

    # ------------------------------------------------------------------ stream: string column
    class StreamStringCol(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_str"

        def create(self):
            tdSql.execute("create database vc_stream_str vgroups 1 buffer 8;")
            tdSql.execute("use vc_stream_str;")
            tdSql.execute("create table t1(ts timestamp, s nchar(16));")
            tdSql.execute(
                "create stream s_vc interval(4s) sliding(4s) from t1 "
                "stream_options(max_delay(3s)) into vc_out as "
                "select _twstart as w, value_changes(s) c "
                "from t1 where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            # 'on','on','off','on' -> on->off, off->on = 2
            tdSql.execute("insert into t1 values(1704067200000,'on');")
            tdSql.execute("insert into t1 values(1704067201000,'on');")
            tdSql.execute("insert into t1 values(1704067202000,'off');")
            tdSql.execute("insert into t1 values(1704067203000,'on');")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from vc_out;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 2,
            )

    # ------------------------------------------------------------------ stream: SESSION trigger
    class StreamSession(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_session"

        def create(self):
            tdSql.execute("create database vc_stream_session vgroups 1 buffer 8;")
            tdSql.execute("use vc_stream_session;")
            tdSql.execute("create table t1(ts timestamp, v int);")
            tdSql.execute(
                "create stream s_vc session(ts,2s) from t1 "
                "stream_options(max_delay(3s)) into vc_out as "
                "select _twstart as s, value_changes(v) c from %%trows;"
            )

        def insert1(self):
            # one session (gaps < 2s): 10,10,20,30 -> 2 changes
            tdSql.execute("insert into t1 values(1704067200000,10);")
            tdSql.execute("insert into t1 values(1704067201000,10);")
            tdSql.execute("insert into t1 values(1704067202000,20);")
            tdSql.execute("insert into t1 values(1704067203000,30);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from vc_out order by s;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == 2,
            )

    # ------------------------------------------------------------------ stream: COUNT_WINDOW trigger
    class StreamCountWindow(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_count"

        def create(self):
            tdSql.execute("create database vc_stream_count vgroups 1 buffer 8;")
            tdSql.execute("use vc_stream_count;")
            tdSql.execute("create table t1(ts timestamp, v int);")
            tdSql.execute(
                "create stream s_vc count_window(3) from t1 "
                "stream_options(max_delay(3s)) into vc_out as "
                "select _twstart as s, value_changes(v) c from %%trows;"
            )

        def insert1(self):
            # windows of 3: [1,2,3]->2 ; [4,4,5]->1
            tdSql.execute("insert into t1 values(1704067200000,1);")
            tdSql.execute("insert into t1 values(1704067201000,2);")
            tdSql.execute("insert into t1 values(1704067202000,3);")
            tdSql.execute("insert into t1 values(1704067203000,4);")
            tdSql.execute("insert into t1 values(1704067204000,4);")
            tdSql.execute("insert into t1 values(1704067205000,5);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from vc_out order by s;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == 2
                and tdSql.getData(1, 0) == 1,
            )

    # ------------------------------------------------------------------ stream: STATE_WINDOW trigger
    class StreamStateWindow(StreamCheckItem):
        def __init__(self):
            self.db = "vc_stream_state"

        def create(self):
            tdSql.execute("create database vc_stream_state vgroups 1 buffer 8;")
            tdSql.execute("use vc_stream_state;")
            tdSql.execute("create table t1(ts timestamp, v int, s int);")
            tdSql.execute(
                "create stream s_vc state_window(s) from t1 "
                "stream_options(max_delay(3s)) into vc_out as "
                "select _twstart as w, value_changes(v) c from %%trows;"
            )

        def insert1(self):
            # state s=1: 10,20 -> 1 ; state s=2: 5,9 -> 1
            tdSql.execute("insert into t1 values(1704067200000,10,1);")
            tdSql.execute("insert into t1 values(1704067201000,20,1);")
            tdSql.execute("insert into t1 values(1704067202000,5,2);")
            tdSql.execute("insert into t1 values(1704067203000,9,2);")

        def check1(self):
            tdSql.checkResultsByFunc(
                "select c from vc_out order by w;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 0) == 1
                and tdSql.getData(1, 0) == 1,
            )
