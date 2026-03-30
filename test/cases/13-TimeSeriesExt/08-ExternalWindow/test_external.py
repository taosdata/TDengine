import os

import taos
from taos import new_bind_params
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdCom


class TestExternal:
    def _check_no_sort_rows(self, sql_rows):
        for sql, rows in sql_rows:
            tdSql.query(sql)
            tdSql.checkRows(rows)

    @staticmethod
    def _is_zero_ts(v):
        if v is None:
            return False
        if isinstance(v, int):
            return v == 0
        if hasattr(v, "year") and hasattr(v, "month") and hasattr(v, "day"):
            return v.year == 1970 and v.month == 1 and v.day == 1
        s = str(v)
        return s in ("0", "1970-01-01 08:00:00.000", "1970-01-01 00:00:00.000")

    @staticmethod
    def _is_invalid_fc1(v):
        if v is None:
            return True
        if isinstance(v, (int, float)):
            return v == 0
        s = str(v).strip()
        return s in ("", "0", "0.0")

    def test_External(self):
        """External basic

        1. Test the basic usage of External window
        2. Test some illegal statements

        Catalog:
            - Timeseries:ExternalWindow

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-02-06 xs Ren Created file

        """

        tdLog.debug(f"start to execute {__file__}")
        self.dbName = "test"

        # only used for BUILD_TEST is defined, which is not the case for regular test runs.
        # self.mock_test_external_window_single_block()
        # self.mock_test_external_window_group_blocks()
        
        self.prepare_data()
        self.prepare_for_partition_and_subquery()
        self.basic_query()
        self.partition_and_subquery_regression()
        self.more_branch_coverage()
        self.orderby_and_alias_regression()
        self.window_boundary_regression()
        self.edge_case_regression()
        self.path_regression()
        self.external_window_negative_semantics()
        self.complex_semantics_regression()
        self.cross_mix_and_join_regression()
        self.large_block_and_time_condition_regression()
        self.vtable_external_window_regression()
        self.stmt_external_window_regression()
        self.fill_external_window_negative()
        self.scenario_regression()

    def mock_test_external_window_single_block(self):
        dbName = "external_window_test_single_block"
        self.prepare_mock_data(dbName)
        tdSql.execute(f"use {dbName}")
        tdLog.info(f"=============== start basic query of external window with agg on single block")
        
        sql = "select _wstart, _wend, w.fc1, count(*) from st1_1 external_window((select first(ts) t1, last(ts) t2 from st2) w);"
        tdSql.error(sql)
        
        sql = "select _wstart, _wend, w.fc1, count(*) from st1_1 external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, 50)
        tdSql.checkData(1, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(1, 1, "2020-05-13 11:21:50.000")
        tdSql.checkData(1, 2, 200)
        tdSql.checkData(1, 3, 32)
        
        sql = "select _wstart, _wend, w.fc1, ts from st1_1 external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(82)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, "2020-05-13 10:00:00.000")
        tdSql.checkData(1, 3, "2020-05-13 10:01:00.000")
        tdSql.checkData(49, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(49, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(49, 2, 100)
        tdSql.checkData(49, 3, "2020-05-13 10:49:00.000")
        tdSql.checkData(50, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(50, 1, "2020-05-13 11:21:50.000")
        tdSql.checkData(50, 2, 200)
        tdSql.checkData(50, 3, "2020-05-13 10:50:00.000")
        tdSql.checkData(81, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(81, 1, "2020-05-13 11:21:50.000")
        tdSql.checkData(81, 2, 200)
        tdSql.checkData(81, 3, "2020-05-13 11:21:00.000")
        
        sql = "select _wstart, _wend, w.fc1+1, ts from st1_1 external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(82)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 101)
        tdSql.checkData(50, 2, 201)
        
        sql = "select _wstart, _wend, w.fc1, count(*) from st1_1 partition by dev  external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, 50)
        tdSql.checkData(1, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(1, 1, "2020-05-13 11:21:50.000")
        tdSql.checkData(1, 2, 200)
        tdSql.checkData(1, 3, 32)
        
        sql = "select _wstart, _wend+1, w.fc1 + 2, count(*) from st1_1 partition by dev  external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.001")
        tdSql.checkData(0, 2, 102)
        tdSql.checkData(0, 3, 50)
        tdSql.checkData(1, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(1, 1, "2020-05-13 11:21:50.001")
        tdSql.checkData(1, 2, 202)
        tdSql.checkData(1, 3, 32)
        
        sql = "select _wstart, _wend, w.fc1, count(*), dev from st1_1 partition by dev  external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, 50)
        tdSql.checkData(0, 4, "dev_01")
        tdSql.checkData(1, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(1, 1, "2020-05-13 11:21:50.000")
        tdSql.checkData(1, 2, 200)
        tdSql.checkData(1, 3, 32)
        tdSql.checkData(1, 4, "dev_01")
        
        sql = "select _wstart, _wend, w.fc1, count(*), v2 from st1_1 partition by v2  external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        # 2 windows * 82 groups. timerange has been pushed down, so groups outside the window range are excluded.
        # if there is no data in the window, window will not output. so total groups is 82 instead of 164.
        tdSql.checkRows(82)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, 1)

        sql = "select _wstart, _wend, w.fc1, count(*), v2 from st1_1 partition by v2  external_window((select ts, ts, first(c1) fc1  from st2) w) order by v2 desc;"
        tdSql.query(sql)
        # 2 windows * 82 groups. timerange has been pushed down, so groups outside the window range are excluded.
        # if there is no data in the window, window will not output. so total groups is 82 instead of 164.
        tdSql.checkRows(82)
        tdSql.checkData(0, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(0, 1, "2020-05-13 11:21:50.000")
        tdSql.checkData(0, 2, 200)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 101082)
        
        # subquery boundary regression:
        # timerange pushdown must stay in the current query fragment and must not leak into nested subqueries.
        # if leaked, nested total_rows would become 82 instead of full-table 100.
        sql = (
            "select _wstart, _wend, w.fc1, count(*), "
            "(select count(*) from (select ts from st1_1) t) as total_rows "
            "from st1_1 external_window((select ts, ts, first(c1) fc1 from st2) w);"
        )
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 50)
        tdSql.checkData(1, 3, 32)
        tdSql.checkData(0, 4, 100)
        tdSql.checkData(1, 4, 100)

        # subquery boundary regression with partition downstream:
        # timerange pushdown should still be limited to the current fragment,
        # and must not affect nested subquery result cardinality.
        sql = (
            "select _wstart, _wend, w.fc1, count(*), v2, "
            "(select count(*) from (select ts from st1_1) t) as total_rows "
            "from st1_1 partition by v2 "
            "external_window((select ts, ts, first(c1) fc1 from st2) w) "
            "order by v2 desc;"
        )
        tdSql.query(sql)
        tdSql.checkRows(82)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(81, 3, 1)
        tdSql.checkData(0, 5, 100)
        tdSql.checkData(81, 5, 100)
        
        tdLog.info(f"=============== end basic query of external window with agg on group blocks")
    
    def mock_test_external_window_group_blocks(self):
        dbName = "external_window_test_group_blocks"
        self.prepare_mock_data(dbName)
        tdSql.execute(f"use {dbName}")
        tdLog.info(f"=============== start basic query of external window with agg on group blocks")
        
        sql = "select _wstart, _wend, w.fc1, count(*), dev from st1_1 partition by dev  external_window((select ts, ts, first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, 50)
        tdSql.checkData(0, 4, "dev_01")
        tdSql.checkData(1, 0, "2020-05-13 10:49:00.001")
        tdSql.checkData(1, 1, "2020-05-13 11:21:50.000")
        tdSql.checkData(1, 2, 200)
        tdSql.checkData(1, 3, 32)
        tdSql.checkData(1, 4, "dev_01")
        tdSql.checkData(2, 0, "2020-05-13 11:00:00.000")
        tdSql.checkData(2, 1, "2020-05-13 11:49:00.000")
        tdSql.checkData(2, 2, 200)
        tdSql.checkData(2, 3, 40)
        tdSql.checkData(2, 4, "dev_01")
        
        tdLog.info(f"=============== end basic query of external window with agg on group blocks")
    
    def prepare_mock_data(self, dbName):
        vgroups = 4
        tdLog.info(f"====> create database {dbName} vgroups {vgroups}")
        tdSql.execute(f"drop database if exists {dbName}")
        tdSql.execute(f"create database {dbName} vgroups {vgroups}")
        
        tdSql.execute(f"use {dbName}")

        tdLog.info(f"=============== create super table, child table and insert data")
        tdSql.execute(
            f"create table if not exists st1 (ts timestamp, v1 int, v2 float) tags(dev nchar(50), t1 binary(16))"
        )
        tdSql.execute(
            f"create table if not exists st2 (ts timestamp, c1 int, c2 float) tags(dev nchar(50), t2 binary(16))"
        )
        
        for i in range(1, 21):
            tdSql.execute(f"create table if not exists st1_{i} using st1 tags('dev_0{i}', 'tag1_{i}')")
            tdSql.execute(f"create table if not exists st2_{i} using st2 tags('dev_0{i}', 'tag2_{i}')")

        ts = 1589335200000  # 2020-05-13 10:00:00.000
        
        for tableIndex in range(1, 21):
            for i in range(1, 101):
                tdSql.execute(f"INSERT INTO st1_{tableIndex} VALUES({ts}, {100000 + tableIndex  * 1000 + i}, {100000 + tableIndex  * 1000 + i})")
                tdSql.execute(f"INSERT INTO st2_{tableIndex} VALUES({ts}, {200000 + tableIndex  * 1000 + i}, {200000 + tableIndex  * 1000 + i})")
                ts += 60000  # add 1 minute

    def prepare_external_win_subquery_data(self, dbName, stbName="ext_win_subq"):
        """Build external_window subquery source data.

        Target layout:
        - One super table + 10 child tables.
        - Columns: ts(timestamp), endtime(timestamp), v1(int), v2(nchar).
        - Tags: t1(int), t2(nchar).
        - t1 is unique per child table; t2 is shared by each pair of child tables.
        - 10 rows per child table.
        - For each row: ts < endtime.
        - For consecutive rows: current ts > previous endtime.
        - The first two rows have identical time values across all child tables.
        """
        tdLog.info(f"====> prepare external window subquery data in {dbName}, stb={stbName}")
        tdSql.execute(f"use {dbName}")

        tdSql.execute(f"drop table if exists {stbName}")
        tdSql.execute(
            f"create table if not exists {stbName} "
            f"(ts timestamp, endtime timestamp, v1 int, v2 nchar(64)) "
            f"tags(t1 int, t2 nchar(64))"
        )

        mock_start_ms =  1589212800000
        mock_total_rows = 20 * 100
        mock_end_ms = mock_start_ms + (mock_total_rows - 1) * 60000

        common_row_1 = (mock_start_ms - 3600000, mock_start_ms - 3540000)
        common_row_2 = (mock_end_ms + 3540000, mock_end_ms + 3600000)

        child_count = 10
        row_count = 10

        for table_idx in range(1, child_count + 1):
            ctb = f"{stbName}_{table_idx}"
            tag_t1 = table_idx
            tag_t2 = f"t2_group_{(table_idx - 1) // 2}"
            tdSql.execute(f"create table if not exists {ctb} using {stbName} tags({tag_t1}, '{tag_t2}')")

            rows = []
            prev_end = None
            for row_idx in range(row_count):
                if row_idx == 0:
                    ts = common_row_1[0]
                    endtime = common_row_1[1]
                elif row_idx == 1:
                    ts = common_row_2[0]
                    endtime = common_row_2[1]
                else:
                    base_after_common = common_row_2[1] + table_idx * 3600000
                    ts = base_after_common + (row_idx - 2) * 180000
                    endtime = ts + 60000

                if prev_end is not None and ts <= prev_end:
                    ts = prev_end + 60000
                    endtime = ts + 60000

                v1 = table_idx * 1000 + row_idx
                v2 = f"v2_{table_idx}_{row_idx}"
                rows.append(f"({ts}, {endtime}, {v1}, '{v2}')")
                prev_end = endtime

            tdSql.execute(f"insert into {ctb} values" + "".join(rows))
        
    def prepare_data(self):
        self.prepare_mock_data("test")
        self.prepare_external_win_subquery_data("test", "ext_win_subq")

    def basic_query(self):
        tdLog.info(f"=============== basic query of external window with agg on single block")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "basic_query.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "basic_query.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "basic_query")
        
    def orderby_and_alias_no_sort(self):
        tdLog.info("=============== external window: orderby and alias no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c from ext_src partition by tbname external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)", 2),
            ("select _wstart, _wend, w.mark, cast(ts as bigint) as ts64 from ext_ord_src external_window((select ts, endtime, mark from ext_ord_win_all) w);", 8),
            ("select _wstart, _wend, w.mark, cast(ts as bigint) as ts64 from ext_ord_src external_window((select ts, endtime, mark from ext_ord_win_all) w) limit 5;", 5),
            ("select _wstart, _wend, w.mark, cast(ts as bigint) - cast(_wstart as bigint) as delta from ext_ord_src external_window((select ts, endtime, mark from ext_ord_win_all) w);", 8),
            ("select _wstart, _wend, w.mark, cast(ts as bigint) - cast(_wstart as bigint) as delta from ext_ord_src external_window((select ts, endtime, mark from ext_ord_win_all) w) limit 8;", 8),
            ("select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c from ext_ord_src external_window((select ts, endtime, mark from ext_ord_win_all) w);", 4),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c from ext_ord_src partition by tbname external_window((select ts, endtime, mark from ext_ord_win_all) w);", 5),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c from ext_ord_src partition by t1 external_window((select ts, endtime, mark from ext_ord_win_all) w);", 5),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c from ext_ord_src partition by tbname external_window((select _wstart, _wend, count(*) as wc from ext_ord_win interval(10m)) w);", 5),
            # When there is a PARTITION BY/GROUP BY clause, LIMIT controls the output within each sharding, rather than the output of the total result set. 
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c from ext_ord_src partition by tbname external_window((select _wstart, _wend, count(*) as wc from ext_ord_win interval(10m)) w) limit 4;", 5),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c from ext_ord_src partition by tbname external_window((select _wstart, _wend, count(*) as wc from ext_ord_win interval(10m)) w) limit 2;", 4),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c from ext_ord_src partition by tbname external_window((select _wstart, _wend, count(*) as wc from ext_ord_win interval(10m)) w) limit 1;", 2),
        ])

    def orderby_and_alias_regression(self):
        tdLog.info("=============== external window: orderby and alias regression")
        self.prepare_for_orderby_and_alias()

        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "orderby_and_alias.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "orderby_and_alias.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "orderby_and_alias")
        
        self.orderby_and_alias_no_sort()

    def window_boundary_regression(self):
        tdLog.info("=============== external window: window boundary regression")
        self.prepare_for_window_boundary()

        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "window_boundary.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "window_boundary.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "window_boundary")
        self.window_boundary_no_sort()

    def window_boundary_no_sort(self):
        tdLog.info("=============== external window: window boundary no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c from ext_bnd_src external_window((select ts, endtime, mark from ext_bnd_win) w);", 5),
            ("select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, ts from ext_bnd_src external_window((select ts, endtime, mark from ext_bnd_win) w);", 9),
            ("select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c from ext_bnd_src external_window((select ts, endtime, mark from ext_bnd_win where mark <> 999) w);", 5),
            ("select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c from ext_bnd_src partition by t1 external_window((select ts, endtime, mark from ext_bnd_win_part partition by t1) w);", 4),
            ("select t1, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c from ext_bnd_src partition by t1 external_window((select ts, endtime, mark from ext_bnd_win_part partition by t1) w);", 4),
            ("select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, sum(v) as sv from ext_bnd_src external_window((select ts, endtime, mark from ext_bnd_win) w);", 5),
            ("select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, max(v)-min(v) as span from ext_bnd_src external_window((select ts, endtime, mark from ext_bnd_win) w);", 5),
        ])

        # Grouped external-window subquery may be globally interleaved by window start,
        # but monotonic check only needs to hold inside each trigger group.
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_bnd_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_bnd_win_part partition by t1) w) "
            "order by t1, ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1700200000000)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1700200600000)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1700200120000)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 1700200840000)
        tdSql.checkData(3, 2, 1)

        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 "
            "from ext_bnd_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_bnd_win_part partition by t1) w) "
            "order by t1, ws, ts64"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1700200000000)
        tdSql.checkData(0, 2, 1700200000000)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1700200000000)
        tdSql.checkData(1, 2, 1700200060000)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 1700200600000)
        tdSql.checkData(2, 2, 1700200600000)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 1700200120000)
        tdSql.checkData(3, 2, 1700200120000)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(4, 1, 1700200120000)
        tdSql.checkData(4, 2, 1700200420000)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(5, 1, 1700200840000)
        tdSql.checkData(5, 2, 1700200900000)

    def edge_case_regression(self):
        tdLog.info("=============== external window: edge case regression")
        self.prepare_for_edge_cases()
        tdSql.execute(f"use {self.dbName}")

        t0 = 1700600000000

        # Adjacent windows share boundary points; the boundary point should appear in both windows.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(ts as bigint) as t "
            "from ext_edge_src "
            "external_window((select ts, endtime, mark from ext_edge_win where mark in (8, 6)) w) "
            "order by ws, t"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(1, 0, t0)
        tdSql.checkData(1, 1, t0 + 60000)
        tdSql.checkData(2, 0, t0 + 60000)
        tdSql.checkData(2, 1, t0 + 60000)
        tdSql.checkData(3, 0, t0 + 60000)
        tdSql.checkData(3, 1, t0 + 120000)
        tdSql.checkData(4, 0, t0 + 60000)
        tdSql.checkData(4, 1, t0 + 180000)

        # Zero-length window should match exactly one timestamp when it exists.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, cast(ts as bigint) as t "
            "from ext_edge_src "
            "external_window((select ts, endtime, mark from ext_edge_win where mark = 4) w)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, t0 + 120000)
        tdSql.checkData(0, 1, t0 + 120000)
        tdSql.checkData(0, 2, t0 + 120000)

        # Empty window should not output detail rows.
        tdSql.query(
            "select cast(ts as bigint) as t "
            "from ext_edge_src "
            "external_window((select ts, endtime, mark from ext_edge_win where mark = 3) w)"
        )
        tdSql.checkRows(0)

        # Boundary-sharing windows should keep independent counts.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c "
            "from ext_edge_src "
            "external_window((select ts, endtime, mark from ext_edge_win where mark in (8, 6)) w) "
            "order by ws, we"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, t0 + 60000)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, t0 + 60000)
        tdSql.checkData(1, 1, t0 + 180000)
        tdSql.checkData(1, 2, 3)

        # Duplicate windows should not be deduplicated by planner/executor.
        tdSql.query(
            "select cast(ts as bigint) as t "
            "from ext_edge_src "
            "external_window(("
            "select ts, endtime, mark from ext_edge_win where mark = 8 "
            "union all "
            "select ts, endtime, mark from ext_edge_win where mark = 8"
            ") w)"
        )
        tdSql.checkRows(4)

        # Empty external_window subquery should return no rows.
        tdSql.query(
            "select cast(ts as bigint) as t "
            "from ext_edge_src "
            "external_window((select ts, endtime, mark from ext_edge_win where mark = 9999) w)"
        )
        tdSql.checkRows(0)

    def large_block_and_time_condition_regression(self):
        tdLog.info("=============== external window: large block and time condition regression")
        self.prepare_for_large_block_and_time_condition()
        tdSql.execute(f"use {self.dbName}")

        t0 = 1700700000000
        t1 = 1700800000000

        # Large input reproducer: current behavior only aggregates the first block (4096 rows).
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c "
            "from ext_blk_src "
            "external_window((select ts, endtime, mark from ext_blk_win) w) "
            "order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, t0 + 4499000)
        tdSql.checkData(0, 2, 4500)
        tdSql.checkData(1, 0, t0 + 4500000)
        tdSql.checkData(1, 1, t0 + 8999000)
        tdSql.checkData(1, 2, 4500)

        # Combination 1: outer grouped / subquery not grouped, aggregate path.
        tdSql.query(
            "select tbname, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c "
            "from ext_blk_src_mt partition by tbname "
            "external_window((select ts, endtime, mark from ext_blk_win) w) "
            "order by tbname, ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ext_blk_src_mt_1")
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, t0 + 4499000)
        tdSql.checkData(0, 3, 4500)
        tdSql.checkData(1, 0, "ext_blk_src_mt_1")
        tdSql.checkData(1, 1, t0 + 4500000)
        tdSql.checkData(1, 2, t0 + 8999000)
        tdSql.checkData(1, 3, 4500)
        tdSql.checkData(2, 0, "ext_blk_src_mt_2")
        tdSql.checkData(2, 1, t0)
        tdSql.checkData(2, 2, t0 + 4499000)
        tdSql.checkData(2, 3, 4500)
        tdSql.checkData(3, 0, "ext_blk_src_mt_2")
        tdSql.checkData(3, 1, t0 + 4500000)
        tdSql.checkData(3, 2, t0 + 8999000)
        tdSql.checkData(3, 3, 4500)

        # Combination 2: outer not grouped / subquery not grouped, aggregate path.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_blk_src_mt "
            "external_window((select ts, endtime, mark from ext_blk_win_many) w) "
            "order by ws"
        )
        tdSql.checkRows(5000)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(4999, 0, t0 + 4999000)
        tdSql.checkData(4999, 1, 2)

        # Combination 3: outer grouped / subquery grouped, aggregate path.
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_blk_src_mt partition by t1 "
            "external_window((select ts, endtime, mark from ext_blk_win_many_part partition by t1) w) "
            "order by t1, ws"
        )
        tdSql.checkRows(10000)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(4999, 0, 1)
        tdSql.checkData(4999, 1, t0 + 4999000)
        tdSql.checkData(4999, 2, 1)
        tdSql.checkData(5000, 0, 2)
        tdSql.checkData(5000, 1, t0)
        tdSql.checkData(5000, 2, 1)
        tdSql.checkData(9999, 0, 2)
        tdSql.checkData(9999, 1, t0 + 4999000)
        tdSql.checkData(9999, 2, 1)

        # Combination 1: outer grouped / subquery not grouped, projection path.
        tdSql.query(
            "select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64, w.mark, v "
            "from ext_blk_src_mt partition by tbname "
            "external_window((select ts, endtime, mark from ext_blk_win_many) w) "
            "order by tbname, ws, ts64"
        )
        tdSql.checkRows(10000)
        tdSql.checkData(0, 0, "ext_blk_src_mt_1")
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, t0)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(4999, 0, "ext_blk_src_mt_1")
        tdSql.checkData(4999, 1, t0 + 4999000)
        tdSql.checkData(4999, 2, t0 + 4999000)
        tdSql.checkData(4999, 3, 5000)
        tdSql.checkData(4999, 4, 4999)
        tdSql.checkData(5000, 0, "ext_blk_src_mt_2")
        tdSql.checkData(5000, 1, t0)
        tdSql.checkData(5000, 2, t0)
        tdSql.checkData(5000, 3, 1)
        tdSql.checkData(5000, 4, 10000)
        tdSql.checkData(9999, 0, "ext_blk_src_mt_2")
        tdSql.checkData(9999, 1, t0 + 4999000)
        tdSql.checkData(9999, 2, t0 + 4999000)
        tdSql.checkData(9999, 3, 5000)
        tdSql.checkData(9999, 4, 14999)

        # Combination 2: outer not grouped / subquery not grouped, projection path.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64, w.mark, v "
            "from ext_blk_src "
            "external_window((select ts, endtime, mark from ext_blk_win_many) w) "
            "order by ws, ts64"
        )
        tdSql.checkRows(5000)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(4999, 0, t0 + 4999000)
        tdSql.checkData(4999, 1, t0 + 4999000)
        tdSql.checkData(4999, 2, 5000)
        tdSql.checkData(4999, 3, 4999)

        # Combination 3: outer grouped / subquery grouped, projection path.
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64, w.mark, v "
            "from ext_blk_src_mt partition by t1 "
            "external_window((select ts, endtime, mark from ext_blk_win_many_part partition by t1) w) "
            "order by t1, ws, ts64"
        )
        tdSql.checkRows(10000)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, t0)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(4999, 0, 1)
        tdSql.checkData(4999, 1, t0 + 4999000)
        tdSql.checkData(4999, 2, t0 + 4999000)
        tdSql.checkData(4999, 3, 5000)
        tdSql.checkData(4999, 4, 4999)
        tdSql.checkData(5000, 0, 2)
        tdSql.checkData(5000, 1, t0)
        tdSql.checkData(5000, 2, t0)
        tdSql.checkData(5000, 3, 1)
        tdSql.checkData(5000, 4, 10000)
        tdSql.checkData(9999, 0, 2)
        tdSql.checkData(9999, 1, t0 + 4999000)
        tdSql.checkData(9999, 2, t0 + 4999000)
        tdSql.checkData(9999, 3, 5000)
        tdSql.checkData(9999, 4, 14999)

        # Time-condition semantics are validated on a separate small dataset to avoid coupling
        # with the known large-block limitation above.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_tcond_src "
            f"where ts >= {t1 + 300000} and ts < {t1 + 900000} "
            "external_window((select ts, endtime, mark from ext_tcond_win) w) "
            "order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, t1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, t1 + 600000)
        tdSql.checkData(1, 1, 3)

        # external_window subquery time predicate should filter candidate windows.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_tcond_src "
            f"external_window((select ts, endtime, mark from ext_tcond_win where ts >= {t1 + 600000}) w) "
            "order by ws"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, t1 + 600000)
        tdSql.checkData(0, 1, 5)

        # Combined source-side time predicate + filtered window subquery.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_tcond_src "
            f"where ts >= {t1 + 900000} "
            f"external_window((select ts, endtime, mark from ext_tcond_win where ts >= {t1 + 600000}) w) "
            "order by ws"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, t1 + 600000)
        tdSql.checkData(0, 1, 2)
        
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_tcond_src "
            f"where ts <= {t1 + 900000} "
            f"external_window((select ts, endtime, mark from ext_tcond_win where ts >= {t1 + 600000}) w) "
            "order by ws"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, t1 + 600000)
        tdSql.checkData(0, 1, 3)

    def path_regression(self):
        tdLog.info("=============== external window: path regression")
        self.prepare_for_path_regression()

        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "path_regression.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "path_regression.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "path_regression")
        self.path_regression_no_sort()

    def path_regression_no_sort(self):
        tdLog.info("=============== external window: path regression no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select cast(_wstart as bigint) as ws, count(*) as c from ext_path_src external_window((select _wstart, _wend, count(*) as wc from ext_path_win interval(10m)) w);", 4),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c from ext_path_src partition by t1 external_window((select _wstart, _wend, count(*) as wc from ext_path_win interval(10m)) w);", 4),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c from ext_path_src partition by tbname external_window((select _wstart, _wend, count(*) as wc from ext_path_win interval(10m)) w);", 4),
            ("select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 from ext_path_src where v> 15 partition by tbname external_window((select _wstart, _wend from ext_path_win interval(10m)) w);", 3),
            ("select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 from ext_path_src partition by tbname external_window((select _wstart, _wend from ext_path_win interval(10m)) w);", 7),
            ("select _wstart, _wend, count(*), (select count(*) from (select ts from ext_path_src) t) as total_rows from ext_path_src external_window((select _wstart, _wend, count(*) as wc from ext_path_win interval(10m)) w);", 4),
            ("select tbname, cast(_wstart as bigint) as ws, count(*), (select count(*) from (select ts from ext_path_src) t) as total_rows from ext_path_src partition by tbname external_window((select _wstart, _wend, count(*) as wc from ext_path_win interval(10m)) w);", 4),
        ])

    def prepare_for_partition_and_subquery(self):
        tdLog.info("=============== external window: partition/group combinations (outer and subquery)")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_src")
        tdSql.execute("drop table if exists ext_win")

        tdSql.execute("create table ext_src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table ext_win (ts timestamp, v int) tags(t1 int)")

        tdSql.execute("create table ext_src_1 using ext_src tags(1)")
        tdSql.execute("create table ext_src_2 using ext_src tags(2)")
        tdSql.execute("create table ext_win_1 using ext_win tags(1)")

        t0 = 1700000000000

        tdSql.execute(f"insert into ext_win_1 values({t0}, 1)({t0 + 600000}, 1)")

        tdSql.execute(f"insert into ext_src_1 values({t0 + 60000}, 10)({t0 + 120000}, 11)")
        tdSql.execute(f"insert into ext_src_2 values({t0 + 660000}, 20)")

    def partition_and_subquery_regression(self):
        tdSql.execute(f"use {self.dbName}")
        
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "no_partition_in_subquery.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "no_partition_in_subquery.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "no_partition_in_subquery")

        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "partition_group_and_subquery.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "partition_group_and_subquery.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "partition_group_and_subquery")
        
    def more_branch_coverage(self):
        """
        Add targeted coverage for planner/operator branches that are hard to express
        in .in/.ans golden files (especially negative cases with stable errno checks).
        """
        tdSql.execute(f"use {self.dbName}")

        # 1) Projection-only external_window + outer PARTITION + ORDER BY on base column
        #    that is NOT present in projection list (regression: sort slot key not found).
        sql = (
            "select t1, cast(_wstart as bigint) as ws, cast(ts as bigint) as t "
            "from ext_src partition by t1 "
            "external_window((select _wstart, _wend from ext_win interval(10m)) w) "
            "order by t1, ts"
        )
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1699999800000)
        tdSql.checkData(0, 2, 1700000060000)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1699999800000)
        tdSql.checkData(1, 2, 1700000120000)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1700000400000)
        tdSql.checkData(2, 2, 1700000660000)

        # 2) Projection-only external_window + subquery PARTITION + outer PARTITION
        #    ensure gid matching behaves: only t1=1 has windows, so t1=2 should not output.
        sql = (
            "select t1, cast(_wstart as bigint) as ws, cast(ts as bigint) as t "
            "from ext_src partition by t1 "
            "external_window((select _wstart, _wend from ext_win partition by t1 interval(10m)) w) "
            "order by t1, ts"
        )
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1699999800000)
        tdSql.checkData(0, 2, 1700000060000)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1699999800000)
        tdSql.checkData(1, 2, 1700000120000)

        # 3) Negative: if subquery has PARTITION/GROUP BY, outer query must also have.
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src "
            "external_window((select _wstart, _wend from ext_win partition by t1 interval(10m)) w) "
            "order by ws",
            expectedErrno=0x80002658,
        )

        # 4) Negative: agg + non-group column without PARTITION BY should error
        #    (semantics same as: select t1, count(*) from ext_src;).
        tdSql.error(
            "select t1, count(*) "
            "from ext_src "
            "external_window((select _wstart, _wend from ext_win interval(10m)) w)",
            expectedErrno=0x8000260C,
        )

        # 5) Negative: stable/child-table tbname/tag must be in PARTITION BY keys when aggregating.
        #    Expected: "Not a single-group group function" instead of planner slot key errors.
        tdSql.error(
            "select tbname, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src partition by t1 "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x8000260C,
        )
        tdSql.error(
            "select tbname, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src_1 partition by t1 "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x8000260C,
        )
        tdSql.error(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src_1 partition by tbname "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x8000260C,
        )

        # 6) Sanity: tbname in PARTITION BY is supported.
        tdSql.query(
            "select tbname, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src_1 partition by tbname "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ext_src_1")
        tdSql.checkData(0, 1, 1699999800000)
        tdSql.checkData(0, 2, 2)
        
        tdSql.execute("create table insert_test (ts timestamp, wstart timestamp, t1 int)")
        tdSql.query("select * from insert_test")
        tdSql.checkRows(0)
        sql = (
            "insert into insert_test "
            "select ts as t, _wstart as ws, t1 from ext_src partition by t1 external_window("
            "(select _wstart, _wend from ext_win partition by t1 interval(10m)) w) order by t1, ts"
        )
        tdSql.execute(sql)
        tdSql.query("select * from insert_test")
        tdSql.checkRows(2)
        sql = (
            "insert into insert_test "
            "select cast(ts as bigint) + 1000 as t, _wstart as ws, t1 from ext_src partition by t1 external_window("
            "(select _wstart, _wend from ext_win partition by t1 interval(10m)) w) order by t1, ts"
        )
        tdSql.execute(sql)
        tdSql.query("select * from insert_test")
        tdSql.checkRows(4)

    def prepare_for_orderby_and_alias(self):
        tdLog.info("=============== external window: orderby and alias dataset")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_ord_src")
        tdSql.execute("drop table if exists ext_ord_win")
        tdSql.execute("drop table if exists ext_ord_win_all")

        tdSql.execute("create table ext_ord_src (ts timestamp, v int, v2 int) tags(t1 int, area binary(16))")
        tdSql.execute("create table ext_ord_win (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table ext_ord_win_all (ts timestamp, endtime timestamp, mark int)")

        tdSql.execute("create table ext_ord_src_1 using ext_ord_src tags(1, 'hz')")
        tdSql.execute("create table ext_ord_src_2 using ext_ord_src tags(2, 'sh')")
        tdSql.execute("create table ext_ord_win_1 using ext_ord_win tags(1)")
        tdSql.execute("create table ext_ord_win_2 using ext_ord_win tags(2)")

        t0 = 1700100000000

        tdSql.execute(
            f"insert into ext_ord_win_1 values({t0}, 1)({t0 + 1200000}, 1)"
        )
        tdSql.execute(
            f"insert into ext_ord_win_2 values({t0 + 600000}, 2)({t0 + 1800000}, 2)"
        )

        tdSql.execute(
            f"insert into ext_ord_win_all values"
            f"({t0}, {t0 + 600000}, 101)"
            f"({t0 + 600000}, {t0 + 1200000}, 102)"
            f"({t0 + 1200000}, {t0 + 1800000}, 103)"
            f"({t0 + 1800000}, {t0 + 2400000}, 104)"
        )

        tdSql.execute(
            f"insert into ext_ord_src_1 values"
            f"({t0 + 60000}, 10, 100)"
            f"({t0 + 120000}, 11, 101)"
            f"({t0 + 660000}, 12, 102)"
            f"({t0 + 1320000}, 13, 103)"
        )
        tdSql.execute(
            f"insert into ext_ord_src_2 values"
            f"({t0 + 720000}, 20, 200)"
            f"({t0 + 780000}, 21, 201)"
            f"({t0 + 1860000}, 22, 202)"
            f"({t0 + 1920000}, 23, 203)"
        )

    def prepare_for_window_boundary(self):
        tdLog.info("=============== external window: boundary dataset")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_bnd_src")
        tdSql.execute("drop table if exists ext_bnd_win")
        tdSql.execute("drop table if exists ext_bnd_win_part")

        tdSql.execute("create table ext_bnd_src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table ext_bnd_win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table ext_bnd_win_part (ts timestamp, endtime timestamp, mark int) tags(t1 int)")

        tdSql.execute("create table ext_bnd_src_1 using ext_bnd_src tags(1)")
        tdSql.execute("create table ext_bnd_src_2 using ext_bnd_src tags(2)")
        tdSql.execute("create table ext_bnd_win_part_1 using ext_bnd_win_part tags(1)")
        tdSql.execute("create table ext_bnd_win_part_2 using ext_bnd_win_part tags(2)")

        t0 = 1700200000000

        tdSql.execute(
            f"insert into ext_bnd_src_1 values"
            f"({t0}, 10)"
            f"({t0 + 60000}, 11)"
            f"({t0 + 300000}, 12)"
            f"({t0 + 600000}, 13)"
        )
        tdSql.execute(
            f"insert into ext_bnd_src_2 values"
            f"({t0 + 120000}, 20)"
            f"({t0 + 420000}, 21)"
            f"({t0 + 900000}, 22)"
        )

        tdSql.execute(
            f"insert into ext_bnd_win values"
            f"({t0 - 60000}, {t0}, 100)"
            f"({t0}, {t0 + 60000}, 101)"
            f"({t0 + 60000}, {t0 + 300000}, 102)"
            f"({t0 + 240000}, {t0 + 480000}, 103)"
            f"({t0 + 900000}, {t0 + 960000}, 104)"
            f"({t0 + 1200000}, {t0 + 1260000}, 999)"
        )

        tdSql.execute(
            f"insert into ext_bnd_win_part_1 values"
            f"({t0}, {t0 + 180000}, 201)"
            f"({t0 + 600000}, {t0 + 720000}, 202)"
        )
        tdSql.execute(
            f"insert into ext_bnd_win_part_2 values"
            f"({t0 + 120000}, {t0 + 480000}, 301)"
            f"({t0 + 840000}, {t0 + 960000}, 302)"
        )

    def prepare_for_path_regression(self):
        tdLog.info("=============== external window: path regression dataset")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_path_src")
        tdSql.execute("drop table if exists ext_path_win")

        tdSql.execute("create table ext_path_src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table ext_path_win (ts timestamp, v int) tags(t1 int)")

        tdSql.execute("create table ext_path_src_1 using ext_path_src tags(1)")
        tdSql.execute("create table ext_path_src_2 using ext_path_src tags(2)")
        tdSql.execute("create table ext_path_win_1 using ext_path_win tags(1)")
        tdSql.execute("create table ext_path_win_2 using ext_path_win tags(2)")

        t0 = 1700300000000

        tdSql.execute(
            f"insert into ext_path_win_1 values({t0}, 1)({t0 + 600000}, 1)"
        )
        tdSql.execute(
            f"insert into ext_path_win_2 values({t0 + 1200000}, 2)({t0 + 1800000}, 2)"
        )

        tdSql.execute(
            f"insert into ext_path_src_1 values"
            f"({t0 + 60000}, 10)"
            f"({t0 + 120000}, 11)"
            f"({t0 + 660000}, 12)"
            f"({t0 + 720000}, 13)"
        )
        tdSql.execute(
            f"insert into ext_path_src_2 values"
            f"({t0 + 1260000}, 20)"
            f"({t0 + 1320000}, 21)"
            f"({t0 + 1860000}, 22)"
        )

    def prepare_for_edge_cases(self):
        tdLog.info("=============== external window: edge case dataset")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_edge_src")
        tdSql.execute("drop table if exists ext_edge_win")

        tdSql.execute("create table ext_edge_src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table ext_edge_win (ts timestamp, endtime timestamp, mark int)")

        tdSql.execute("create table ext_edge_src_1 using ext_edge_src tags(1)")

        t0 = 1700600000000

        tdSql.execute(
            f"insert into ext_edge_src_1 values"
            f"({t0}, 10)"
            f"({t0 + 60000}, 11)"
            f"({t0 + 120000}, 12)"
            f"({t0 + 180000}, 13)"
        )

        tdSql.execute(
            f"insert into ext_edge_win values"
            f"({t0}, {t0 + 60000}, 1)"
            f"({t0 + 60000}, {t0 + 120000}, 2)"
            f"({t0 + 240000}, {t0 + 300000}, 3)"
            f"({t0 + 120000}, {t0 + 120000}, 4)"
            f"({t0}, {t0 + 120000}, 5)"
            f"({t0 + 60000}, {t0 + 180000}, 6)"
            f"({t0}, {t0 + 60000}, 7)"
            f"({t0}, {t0 + 60000}, 8)"
        )

    def prepare_for_large_block_and_time_condition(self):
        tdLog.info("=============== external window: large block and time condition dataset")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_blk_src")
        tdSql.execute("drop table if exists ext_blk_win")
        tdSql.execute("drop table if exists ext_blk_src_mt")
        tdSql.execute("drop table if exists ext_blk_src_mt_1")
        tdSql.execute("drop table if exists ext_blk_src_mt_2")
        tdSql.execute("drop table if exists ext_blk_win_many")
        tdSql.execute("drop table if exists ext_blk_win_many_part")
        tdSql.execute("drop table if exists ext_blk_win_many_part_1")
        tdSql.execute("drop table if exists ext_blk_win_many_part_2")
        tdSql.execute("drop table if exists ext_tcond_src")
        tdSql.execute("drop table if exists ext_tcond_win")

        tdSql.execute("create table ext_blk_src (ts timestamp, v int)")
        tdSql.execute("create table ext_blk_win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table ext_blk_src_mt (ts timestamp, v int, v2 int) tags(t1 int)")
        tdSql.execute("create table ext_blk_win_many (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table ext_blk_win_many_part (ts timestamp, endtime timestamp, mark int) tags(t1 int)")
        tdSql.execute("create table ext_tcond_src (ts timestamp, v int)")
        tdSql.execute("create table ext_tcond_win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table ext_blk_src_mt_1 using ext_blk_src_mt tags(1)")
        tdSql.execute("create table ext_blk_src_mt_2 using ext_blk_src_mt tags(2)")
        tdSql.execute("create table ext_blk_win_many_part_1 using ext_blk_win_many_part tags(1)")
        tdSql.execute("create table ext_blk_win_many_part_2 using ext_blk_win_many_part tags(2)")

        t0 = 1700700000000
        t1 = 1700800000000
        total_rows = 9000
        batch_rows = 1000

        for start in range(0, total_rows, batch_rows):
            end = min(start + batch_rows, total_rows)
            vals = []
            for i in range(start, end):
                vals.append(f"({t0 + i * 1000}, {i})")
            tdSql.execute("insert into ext_blk_src values" + "".join(vals))

        for tb_idx, table_name in enumerate(("ext_blk_src_mt_1", "ext_blk_src_mt_2")):
            for start in range(0, total_rows, batch_rows):
                end = min(start + batch_rows, total_rows)
                vals = []
                for i in range(start, end):
                    vals.append(f"({t0 + i * 1000}, {tb_idx * 10000 + i}, {(tb_idx + 1) * 100000 + i})")
                tdSql.execute(f"insert into {table_name} values" + "".join(vals))

        tdSql.execute(
            f"insert into ext_blk_win values"
            f"({t0}, {t0 + 4499000}, 1)"
            f"({t0 + 4500000}, {t0 + 8999000}, 2)"
        )

        large_window_rows = 5000
        for start in range(0, large_window_rows, batch_rows):
            end = min(start + batch_rows, large_window_rows)
            vals = []
            for i in range(start, end):
                ts = t0 + i * 1000
                vals.append(f"({ts}, {ts}, {i + 1})")
            tdSql.execute("insert into ext_blk_win_many values" + "".join(vals))

        for tb_idx, table_name in enumerate(("ext_blk_win_many_part_1", "ext_blk_win_many_part_2")):
            for start in range(0, large_window_rows, batch_rows):
                end = min(start + batch_rows, large_window_rows)
                vals = []
                for i in range(start, end):
                    ts = t0 + i * 1000
                    vals.append(f"({ts}, {ts}, {i + 1})")
                tdSql.execute(f"insert into {table_name} values" + "".join(vals))

        tdSql.execute(
            f"insert into ext_tcond_src values"
            f"({t1 + 0}, 0)"
            f"({t1 + 120000}, 1)"
            f"({t1 + 240000}, 2)"
            f"({t1 + 360000}, 3)"
            f"({t1 + 480000}, 4)"
            f"({t1 + 600000}, 5)"
            f"({t1 + 720000}, 6)"
            f"({t1 + 840000}, 7)"
            f"({t1 + 960000}, 8)"
            f"({t1 + 1080000}, 9)"
            f"({t1 + 1200000}, 10)"
            f"({t1 + 1320000}, 11)"
        )

        tdSql.execute(
            f"insert into ext_tcond_win values"
            f"({t1}, {t1 + 540000}, 1)"
            f"({t1 + 600000}, {t1 + 1140000}, 2)"
        )

    def external_window_negative_semantics(self):
        tdLog.info("=============== external window: negative semantics")
        tdSql.execute(f"use {self.dbName}")

        # err: The first two columns of EXTERNAL_WINDOW subquery must be timestamp
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src "
            "external_window((select _wstart, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x80002658,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src "
            "external_window((select _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x80002658,
        )

        # EXTERNAL_WINDOW subquery cannot have GROUP BY or PARTITION BY clause if the outer query doesn't have
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src "
            "external_window((select _wstart, _wend from ext_win partition by t1 interval(10m)) w)",
            expectedErrno=0x80002658,
            expectErrInfo="EXTERNAL_WINDOW subquery cannot have GROUP BY or PARTITION BY clause if the outer query doesnt have",
            fullMatched=False,
        )

        tdSql.error(
            "select tbname, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src partition by t1 "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x8000260C,
            expectErrInfo="Not a single-group group function",
            fullMatched=False,
        )

        tdSql.error(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src_1 partition by tbname "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x8000260C,
            expectErrInfo="Not a single-group group function",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, bad_alias "
            "from ext_src "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x80002602,
            expectErrInfo="Invalid column name",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, w.not_exist "
            "from ext_src "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x80002602,
            expectErrInfo="Invalid column name",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src external_window((select _wstart, _wend from ext_win interval(10m)) w) "
            "order by bad_col",
            expectedErrno=0x80002602,
            expectErrInfo="Invalid column name",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src external_window((select _wstart, _wend from ext_win interval(10m)) w) "
            "having c > 1",
            expectErrInfo="Invalid column name",
            fullMatched=False,
        )
        
        # Window attribute columns are not allowed in WHERE.
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src "
            "where w.wc > 0 "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w)",
            expectedErrno=0x800026B1,
            expectErrInfo="WHERE clause cannot reference EXTERNAL_WINDOW column",
            fullMatched=False,
        )

    def fill_external_window_negative(self):
        """Test that FILL clause is rejected for external_window."""
        tdLog.info("=============== external window: fill negative regression")
        tdSql.execute(f"use {self.dbName}")

        # FILL NONE — semantic error (grammar accepts fill_mode: none/null/null_f/linear)
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win) w "
            "fill(none))",
            expectedErrno=0x80002657,
            expectErrInfo="Fill not allowed in external window query",
            fullMatched=False,
        )

        # FILL NULL
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win) w "
            "fill(null))",
            expectedErrno=0x80002657,
            expectErrInfo="Fill not allowed in external window query",
            fullMatched=False,
        )

        # FILL LINEAR
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win) w "
            "fill(linear))",
            expectedErrno=0x80002657,
            expectErrInfo="Fill not allowed in external window query",
            fullMatched=False,
        )

        # FILL PREV — syntax error (prev/next/near not in external_window_fill_opt grammar)
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win) w "
            "fill(prev))",
            expectedErrno=0x80002600,
        )

        # FILL NEXT — syntax error
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win) w "
            "fill(next))",
            expectedErrno=0x80002600,
        )

        # FILL NULL with partition by — semantic error
        tdSql.error(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_cx_win) w "
            "fill(null))",
            expectedErrno=0x80002657,
            expectErrInfo="Fill not allowed in external window query",
            fullMatched=False,
        )

        tdLog.info("=============== external window: fill negative regression done")

    def complex_agg_and_filter_no_sort(self):
        tdLog.info("=============== external window: complex agg and filter no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, min(v) as minv, max(v) as maxv, max(v)-min(v) as span from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, avg(v) as avgv, sum(v2) as sv2 from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src where v >= 20 external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win where mark >= 102) w);", 3),
            ("select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src where v >= 12 external_window((select ts, endtime, mark from ext_cx_win where mark <= 103) w);", 3),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w);", 8),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c, max(v)-min(v) as span from ext_cx_src partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w);", 8),
            ("select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w) having count(*) > 1;", 4),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) having sum(v) > 20;", 5),
        ])

    def complex_partition_and_having_no_sort(self):
        tdLog.info("=============== external window: complex partition and having no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w);", 8),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) having count(*) > 1;", 3),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) having sum(v) > 20;", 5),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w);", 8),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c, max(v)-min(v) as span from ext_cx_src partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w) having max(v)-min(v) >= 0;", 8),
            ("select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 from ext_cx_src partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w);", 11),
            ("select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 from ext_cx_src partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w) limit 4;", 8),
            ("select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 from ext_cx_src partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w) limit 8;", 11),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src where v >= 12 partition by t1 external_window((select ts, endtime, mark from ext_cx_win where mark <= 103) w) having sum(v) > 10;", 5),
            ("select tbname, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src where v >= 20 partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) limit 6;", 6),
        ])

    def function_matrix_no_sort(self):
        tdLog.info("=============== external window: function matrix no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select cast(_wstart as bigint) as ws, count(*) as c from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, sum(v) as sv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, min(v) as minv, max(v) as maxv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, avg(v) as avgv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select cast(_wstart as bigint) as ws, first(v) as fv, last(v) as lv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv, min(v) as minv, max(v) as maxv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w);", 8),
            ("select tbname, cast(_wstart as bigint) as ws, avg(v) as avgv, first(v) as fv, last(v) as lv from ext_cx_src partition by tbname external_window((select ts, endtime, mark from ext_cx_win) w);", 8),
            ("select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv, avg(v2) as avgv2 from ext_cx_src where v >= 12 external_window((select ts, endtime, mark from ext_cx_win where mark <= 103) w);", 3),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) having count(*) >= 1;", 8),
            ("select cast(_wstart as bigint) as ws, stddev(v) vv from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w);", 4),
        ])

    def special_function_negative_matrix(self):
        tdLog.info("=============== external window: special function negative matrix")
        tdSql.execute(f"use {self.dbName}")

        tdSql.noError(
            "select cast(_wstart as bigint) as ws, first(v) as fv, last(v) as lv "
            "from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws"
        )

        tdSql.noError(
            "select t1, cast(_wstart as bigint) as ws, first(v) as fv, last(v) as lv "
            "from ext_cx_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by t1, ws"
        )

        tdSql.error(
            "create stream stream1 interval(10m) sliding(10m) from ext_win into stream_out_a "
            "as (select _wstart as ws, first(v) as fv, last(v) as lv from ext_cx_src partition "
            "by t1 external_window((select ts, endtime, mark from ext_cx_win) w) order by t1, ws);",
            fullMatched=False,
            expectErrInfo="External window query can not be used in stream query"
        )

        tdSql.error(
            "create stream stream1 interval(10m) sliding(10m) from ext_win into stream_out_a "
            "as (select cast(_wstart as bigint) as ws, first(v) as fv, last(v) as lv from ext_cx_src partition "
            "by t1 external_window((select ts, endtime, mark from ext_cx_win) w) order by t1, ws)",
            fullMatched=False,
            expectErrInfo="External window query can not be used in stream query"
        )
        
        tdSql.error(
            "CREATE TOPIC IF NOT EXISTS topic_with_external_window as (select cast(_wstart as bigint) "
            "as ws, first(v) as fv, last(v) as lv from ext_cx_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_cx_win) w) order by t1, ws)",
            fullMatched=False,
            expectErrInfo="External window query can not be used in topic query"
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, top(v, 2) "
            "from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, bottom(v, 2) "
            "from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, percentile(v, 50) "
            "from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(distinct v) "
            "from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c, top(v, 2) "
            "from ext_cx_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws",
            fullMatched=False,
        )

        tdSql.error(
            "select cast(_wstart as bigint) as ws, unique(v) "
            "from ext_cx_src external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws",
            fullMatched=False,
        )

    def complex_semantics_regression(self):
        tdLog.info("=============== external window: complex semantics regression")
        self.prepare_for_complex_semantics()

        cases = [
            ("complex_agg_and_filter", "complex_agg_and_filter.in", "complex_agg_and_filter.ans"),
            ("complex_partition_and_having", "complex_partition_and_having.in", "complex_partition_and_having.ans"),
            ("function_matrix", "function_matrix.in", "function_matrix.ans"),
        ]

        for case_name, sql_name, ans_name in cases:
            self.sqlFile = os.path.join(os.path.dirname(__file__), "in", sql_name)
            self.ansFile = os.path.join(os.path.dirname(__file__), "ans", ans_name)
            tdCom.compare_testcase_result(self.sqlFile, self.ansFile, case_name)
        self.complex_agg_and_filter_no_sort()
        self.function_matrix_no_sort()
        self.special_function_negative_matrix()
        self.complex_partition_and_having_no_sort()

    def cross_mix_and_join_regression(self):
        tdLog.info("=============== external window: cross mix and join regression")
        self.prepare_for_complex_semantics()
        self.prepare_for_join_subquery()

        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", "cross_mix_and_join.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", "cross_mix_and_join.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "cross_mix_and_join")
        self.cross_mix_and_join_no_sort()

        # Join over derived subqueries that each contain EXTERNAL_WINDOW is still
        # rejected by planner/executor with 0x80002664. Keep as negative coverage.
        tdSql.error(
            "SELECT "
            "a.ws, "
            "a.c AS c_a, "
            "b.c AS c_b, "
            "a.total_rows AS total_rows_a, "
            "b.total_rows AS total_rows_b "
            "FROM ( "
            "select cast(_wstart as bigint) as ws, count(*) as c, "
            "(select count(*) from (select ts from ext_join_src) t) as total_rows "
            "from ext_join_src "
            "external_window((select jw.ts, jw.endtime "
            "from ext_join_win jw, ext_join_src jd "
            "where jw.ts=jd.ts) w) "
            "order by ws "
            ") a "
            "JOIN ( "
            "select cast(_wstart as bigint) as ws, count(*) as c, "
            "(select count(*) from (select ts from ext_join_src) t) as total_rows "
            "from ext_join_src "
            "external_window((select jw.ts, jw.endtime "
            "from ext_join_win jw left join ext_join_src jd on jw.ts=jd.ts) w) "
            "order by ws "
            ") b "
            "ON a.ws = b.ws "
            "ORDER BY a.ws",
            expectedErrno=0x80002664,
        )

        tdSql.error(
            "SELECT "
            "a.ws, "
            "a.c AS c_a, "
            "b.c AS c_b, "
            "a.total_rows AS total_rows_a, "
            "b.total_rows AS total_rows_b "
            "FROM ( "
            "select cast(_wstart as bigint) as ws, count(*) as c, "
            "(select count(*) from (select ts from ext_join_src) t) as total_rows "
            "from ext_join_src "
            "external_window((select jw.ts, jw.endtime "
            "from ext_join_win jw, ext_join_src jd "
            "where jw.ts=jd.ts) w) "
            "order by ws "
            ") a "
            "LEFT JOIN ( "
            "select cast(_wstart as bigint) as ws, count(*) as c, "
            "(select count(*) from (select ts from ext_join_src) t) as total_rows "
            "from ext_join_src "
            "external_window((select jw.ts, jw.endtime "
            "from ext_join_win jw left join ext_join_src jd on jw.ts=jd.ts) w) "
            "order by ws "
            ") b "
            "ON a.ws = b.ws "
            "ORDER BY a.ws",
            expectedErrno=0x80002664,
        )

    def cross_mix_and_join_no_sort(self):
        tdLog.info("=============== external window: cross mix and join no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src where v >= 11 partition by t1 external_window((select ts, endtime, mark from ext_cx_win where mark <= 104) w) having count(*) >= 1 limit 8;", 8),
            ("select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64 from ext_cx_src where v >= 20 partition by tbname external_window((select ts, endtime, mark from ext_cx_win where mark >= 102) w) limit 8;", 4),
        ])

    def prepare_for_join_subquery(self):
        tdLog.info("=============== external window: join subquery dataset")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_join_src")
        tdSql.execute("drop table if exists ext_join_win")
        tdSql.execute("drop table if exists ext_join_dim")

        tdSql.execute("create table ext_join_src (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table ext_join_win (ts timestamp, endtime timestamp, k int)")
        tdSql.execute("create table ext_join_dim (ts timestamp, k int, tagv int)")

        tdSql.execute("create table ext_join_src_1 using ext_join_src tags(1)")
        tdSql.execute("create table ext_join_src_2 using ext_join_src tags(2)")

        t0 = 1700500000000

        tdSql.execute(
            f"insert into ext_join_dim values"
            f"({t0}, 1, 10)"
            f"({t0 + 300000}, 2, 20)"
            f"({t0 + 600000}, 3, 30)"
        )

        tdSql.execute(
            f"insert into ext_join_win values"
            f"({t0}, {t0 + 300000}, 1)"
            f"({t0 + 300000}, {t0 + 600000}, 2)"
            f"({t0 + 600000}, {t0 + 900000}, 3)"
        )

        tdSql.execute(
            f"insert into ext_join_src_1 values"
            f"({t0 + 60000}, 10)"
            f"({t0 + 120000}, 11)"
            f"({t0 + 360000}, 12)"
            f"({t0 + 720000}, 13)"
        )
        tdSql.execute(
            f"insert into ext_join_src_2 values"
            f"({t0 + 180000}, 20)"
            f"({t0 + 420000}, 21)"
            f"({t0 + 780000}, 22)"
        )

    def prepare_for_complex_semantics(self):
        tdLog.info("=============== external window: complex semantics dataset")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_cx_src")
        tdSql.execute("drop table if exists ext_cx_win")
        tdSql.execute("drop table if exists ext_cx_win_part")

        tdSql.execute("create table ext_cx_src (ts timestamp, v int, v2 int) tags(t1 int)")
        tdSql.execute("create table ext_cx_win (ts timestamp, endtime timestamp, mark int)")
        tdSql.execute("create table ext_cx_win_part (ts timestamp, v int) tags(t1 int)")

        tdSql.execute("create table ext_cx_src_1 using ext_cx_src tags(1)")
        tdSql.execute("create table ext_cx_src_2 using ext_cx_src tags(2)")
        tdSql.execute("create table ext_cx_win_part_1 using ext_cx_win_part tags(1)")
        tdSql.execute("create table ext_cx_win_part_2 using ext_cx_win_part tags(2)")

        t0 = 1700400000000

        tdSql.execute(
            f"insert into ext_cx_win values"
            f"({t0}, {t0 + 300000}, 101)"
            f"({t0 + 300000}, {t0 + 600000}, 102)"
            f"({t0 + 600000}, {t0 + 900000}, 103)"
            f"({t0 + 900000}, {t0 + 1200000}, 104)"
        )

        tdSql.execute(
            f"insert into ext_cx_win_part_1 values"
            f"({t0}, 1)"
            f"({t0 + 600000}, 1)"
        )
        tdSql.execute(
            f"insert into ext_cx_win_part_2 values"
            f"({t0 + 300000}, 2)"
            f"({t0 + 900000}, 2)"
        )

        tdSql.execute(
            f"insert into ext_cx_src_1 values"
            f"({t0 + 60000}, 10, 100)"
            f"({t0 + 120000}, 11, 101)"
            f"({t0 + 360000}, 12, 102)"
            f"({t0 + 420000}, 13, 103)"
            f"({t0 + 660000}, 14, 104)"
            f"({t0 + 960000}, 15, 105)"
        )
        tdSql.execute(
            f"insert into ext_cx_src_2 values"
            f"({t0 + 180000}, 20, 200)"
            f"({t0 + 480000}, 21, 201)"
            f"({t0 + 540000}, 22, 202)"
            f"({t0 + 780000}, 23, 203)"
            f"({t0 + 1020000}, 24, 204)"
        )

    def prepare_for_vtable_external_window(self):
        """Prepare origin tables and virtual tables for external_window + vtable tests."""
        tdLog.info("=============== external window: vtable combination dataset")
        tdSql.execute(f"use {self.dbName}")

        t0 = 1700900000000

        # ---- origin source: super table with two child tables ----
        tdSql.execute("drop table if exists ext_vt_org_src")
        tdSql.execute(
            "create table ext_vt_org_src (ts timestamp, v int, v2 int) tags(t1 int)"
        )
        tdSql.execute("create table ext_vt_org_src_1 using ext_vt_org_src tags(1)")
        tdSql.execute("create table ext_vt_org_src_2 using ext_vt_org_src tags(2)")

        tdSql.execute(
            f"insert into ext_vt_org_src_1 values"
            f"({t0 + 60000}, 10, 100)"
            f"({t0 + 120000}, 11, 101)"
            f"({t0 + 360000}, 12, 102)"
            f"({t0 + 660000}, 13, 103)"
        )
        tdSql.execute(
            f"insert into ext_vt_org_src_2 values"
            f"({t0 + 180000}, 20, 200)"
            f"({t0 + 420000}, 21, 201)"
            f"({t0 + 720000}, 22, 202)"
        )

        # ---- origin window definition (normal table) ----
        tdSql.execute("drop table if exists ext_vt_org_win")
        tdSql.execute(
            "create table ext_vt_org_win (ts timestamp, endtime timestamp, mark int)"
        )
        tdSql.execute(
            f"insert into ext_vt_org_win values"
            f"({t0}, {t0 + 300000}, 101)"
            f"({t0 + 300000}, {t0 + 600000}, 102)"
            f"({t0 + 600000}, {t0 + 900000}, 103)"
        )

        # ---- virtual source: normal vtable referencing child_1 only ----
        tdSql.execute("drop table if exists ext_vt_vsrc_ntb")
        tdSql.execute(
            "create vtable ext_vt_vsrc_ntb ("
            "ts timestamp, "
            "v int from ext_vt_org_src_1.v, "
            "v2 int from ext_vt_org_src_1.v2)"
        )

        # ---- virtual source: virtual super table + child tables ----
        tdSql.execute("drop table if exists ext_vt_vsrc_stb")
        tdSql.execute(
            "create stable ext_vt_vsrc_stb "
            "(ts timestamp, v int, v2 int) tags(t1 int) virtual 1"
        )
        tdSql.execute(
            "create vtable ext_vt_vsrc_stb_1 ("
            "v from ext_vt_org_src_1.v, "
            "v2 from ext_vt_org_src_1.v2) "
            "using ext_vt_vsrc_stb tags(1)"
        )
        tdSql.execute(
            "create vtable ext_vt_vsrc_stb_2 ("
            "v from ext_vt_org_src_2.v, "
            "v2 from ext_vt_org_src_2.v2) "
            "using ext_vt_vsrc_stb tags(2)"
        )

        # ---- virtual window: normal vtable referencing ext_vt_org_win ----
        tdSql.execute("drop table if exists ext_vt_vwin_ntb")
        tdSql.execute(
            "create vtable ext_vt_vwin_ntb ("
            "ts timestamp, "
            "endtime timestamp from ext_vt_org_win.endtime, "
            "mark int from ext_vt_org_win.mark)"
        )

    def vtable_external_window_regression(self):
        """Test external_window combined with virtual tables.

        Three scenarios:
        1. Subquery references a virtual table (window definition from vtable)
        2. Outer query references a virtual table (source data from vtable)
        3. Both subquery and outer query reference virtual tables
        """
        tdLog.info("=============== external window: vtable combination regression")
        self.prepare_for_vtable_external_window()
        tdSql.execute(f"use {self.dbName}")

        t0 = 1700900000000

        # ==================================================================
        # Scenario 1: subquery is virtual table
        #   Window definition comes from vtable, outer query uses origin table.
        # ==================================================================

        # 1a) Agg path: origin super table + vtable window subquery
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, "
            "w.mark, count(*) as c, sum(v) as sv "
            "from ext_vt_org_src "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, t0 + 300000)
        tdSql.checkData(0, 2, 101)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 41)
        tdSql.checkData(1, 0, t0 + 300000)
        tdSql.checkData(1, 1, t0 + 600000)
        tdSql.checkData(1, 2, 102)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 33)
        tdSql.checkData(2, 0, t0 + 600000)
        tdSql.checkData(2, 1, t0 + 900000)
        tdSql.checkData(2, 2, 103)
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 35)

        # 1b) Projection path: origin super table + vtable window subquery
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, cast(ts as bigint) as ts64, v "
            "from ext_vt_org_src "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by ws, ts64"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(0, 2, t0 + 60000)
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(6, 0, t0 + 600000)
        tdSql.checkData(6, 1, 103)
        tdSql.checkData(6, 2, t0 + 720000)
        tdSql.checkData(6, 3, 22)

        # 1c) Partition by t1: origin super table + vtable window subquery
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_org_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by t1, ws"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 21)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, t0)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 20)

        # ==================================================================
        # Scenario 2: outer query is virtual table
        #   Source data comes from vtable, window definition from origin table.
        # ==================================================================

        # 2a) Virtual normal table (references child_1 only) with origin window, agg
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_ntb "
            "external_window((select ts, endtime, mark from ext_vt_org_win) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 21)
        tdSql.checkData(1, 0, t0 + 300000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 12)
        tdSql.checkData(2, 0, t0 + 600000)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 13)

        # 2b) Virtual super table with origin window, agg
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_stb "
            "external_window((select ts, endtime, mark from ext_vt_org_win) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 41)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 33)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 35)

        # 2c) Virtual super table with partition by t1
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_stb partition by t1 "
            "external_window((select ts, endtime, mark from ext_vt_org_win) w) "
            "order by t1, ws"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 21)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, t0)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 20)

        # 2d) Virtual super table, projection path
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64, v "
            "from ext_vt_vsrc_stb "
            "external_window((select ts, endtime, mark from ext_vt_org_win) w) "
            "order by ws, ts64"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, t0 + 60000)
        tdSql.checkData(0, 2, 10)
        tdSql.checkData(6, 0, t0 + 600000)
        tdSql.checkData(6, 1, t0 + 720000)
        tdSql.checkData(6, 2, 22)

        # 2e) Virtual super table with partition by tbname
        tdSql.query(
            "select tbname, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_vt_vsrc_stb partition by tbname "
            "external_window((select ts, endtime, mark from ext_vt_org_win) w) "
            "order by tbname, ws"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "ext_vt_vsrc_stb_1")
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(3, 0, "ext_vt_vsrc_stb_2")
        tdSql.checkData(3, 1, t0)
        tdSql.checkData(3, 2, 1)

        # 2f) Virtual child table directly with origin window, agg
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_stb_1 "
            "external_window((select ts, endtime, mark from ext_vt_org_win) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 21)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 12)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 13)

        # ==================================================================
        # Scenario 3: both subquery and outer query are virtual tables
        # ==================================================================

        # 3a) Virtual normal src + virtual window, agg
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_ntb "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 21)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 12)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 13)

        # 3b) Virtual super src + virtual window, agg with w.mark
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_stb "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 41)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 33)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 35)

        # 3c) Virtual super src + virtual window, partition by t1
        tdSql.query(
            "select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_stb partition by t1 "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by t1, ws"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 21)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, t0)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, 20)

        # 3d) Virtual super src + virtual window, projection path
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.mark, cast(ts as bigint) as ts64, v "
            "from ext_vt_vsrc_stb "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by ws, ts64"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, t0)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(0, 2, t0 + 60000)
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(6, 0, t0 + 600000)
        tdSql.checkData(6, 1, 103)
        tdSql.checkData(6, 2, t0 + 720000)
        tdSql.checkData(6, 3, 22)

        # 3e) Virtual super src + virtual window, partition by tbname with projection
        tdSql.query(
            "select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64, v "
            "from ext_vt_vsrc_stb partition by tbname "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by tbname, ws, ts64"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "ext_vt_vsrc_stb_1")
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, t0 + 60000)
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(4, 0, "ext_vt_vsrc_stb_2")
        tdSql.checkData(4, 1, t0)
        tdSql.checkData(4, 2, t0 + 180000)
        tdSql.checkData(4, 3, 20)

        # 3f) Subquery filter on virtual window
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_vt_vsrc_stb "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb where mark >= 102) w) "
            "order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, t0 + 300000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, t0 + 600000)
        tdSql.checkData(1, 1, 2)

        # 3g) Source-side time filter + both virtual tables
        tdSql.query(
            f"select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            f"from ext_vt_vsrc_stb "
            f"where ts >= {t0 + 300000} "
            f"external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            f"order by ws"
        )
        # ts >= t0+300000 filters source: src_1 keeps t0+360k(12),t0+660k(13); src_2 keeps t0+420k(21),t0+720k(22)
        # Window [t0, t0+300000] has no qualifying rows -> not output
        # Window [t0+300000, t0+600000]: count=2, sum=33
        # Window [t0+600000, t0+900000]: count=2, sum=35
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, t0 + 300000)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 33)
        tdSql.checkData(1, 0, t0 + 600000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 35)

        # 3h) Cross-verify: origin tables and virtual tables produce same results
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_org_src "
            "external_window((select ts, endtime, mark from ext_vt_org_win) w) "
            "order by ws"
        )
        origin_rows = tdSql.queryRows
        origin_data = [tdSql.queryResult[i] for i in range(origin_rows)]

        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_vt_vsrc_stb "
            "external_window((select ts, endtime, mark from ext_vt_vwin_ntb) w) "
            "order by ws"
        )
        tdSql.checkRows(origin_rows)
        for i in range(origin_rows):
            tdSql.checkData(i, 0, origin_data[i][0])
            tdSql.checkData(i, 1, origin_data[i][1])
            tdSql.checkData(i, 2, origin_data[i][2])

    def _stmt_conn(self):
        """Create a taos connection for STMT tests."""
        buildPath = tdCom.getBuildPath()
        config = buildPath + "../sim/dnode1/cfg/"
        return taos.connect(host="localhost", config=config)

    def _assert_stmt_external_window_error(self, conn, sql, params, case_label):
        """Assert that a STMT query with external_window raises an error."""
        stmt = conn.statement(sql)
        try:
            stmt.bind_param(params)
            stmt.execute()
            raise AssertionError(f"{case_label}: expected error but succeeded")
        except Exception as err:
            err_msg = str(err)
            tdLog.info(f"{case_label}: got expected error: {err_msg}")
            if "External window query can not be used in stmt query" not in err_msg:
                raise AssertionError(
                    f"{case_label}: unexpected error message: {err_msg}"
                )

    def stmt_external_window_regression(self):
        """Test that external_window is forbidden in STMT (parameterised) queries."""
        tdLog.info("=============== external window: stmt negative regression")
        tdSql.execute(f"use {self.dbName}")

        conn = self._stmt_conn()
        conn.select_db(self.dbName)

        t0 = 1700400000000  # same base as ext_cx_* tables

        # ---- 1. STMT with bind param in outer WHERE ----
        sql = (
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_cx_src "
            "where ts >= ? "
            "external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws"
        )
        params = new_bind_params(1)
        params[0].timestamp(t0 + 300000)
        self._assert_stmt_external_window_error(conn, sql, params, "stmt case 1")

        # ---- 2. STMT with bind param in subquery WHERE ----
        sql = (
            "select cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win where mark >= ?) w) "
            "order by ws"
        )
        params = new_bind_params(1)
        params[0].int(103)
        self._assert_stmt_external_window_error(conn, sql, params, "stmt case 2")

        # ---- 3. STMT with bind params in both outer and subquery ----
        sql = (
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src "
            "where ts >= ? "
            "external_window((select ts, endtime, mark from ext_cx_win where mark <= ?) w) "
            "order by ws"
        )
        params = new_bind_params(2)
        params[0].timestamp(t0 + 300000)
        params[1].int(103)
        self._assert_stmt_external_window_error(conn, sql, params, "stmt case 3")

        # ---- 4. STMT aggregate with partition by ----
        sql = (
            "select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv "
            "from ext_cx_src partition by t1 "
            "external_window((select ts, endtime, mark from ext_cx_win where mark >= ?) w) "
            "order by t1, ws"
        )
        params = new_bind_params(1)
        params[0].int(101)
        self._assert_stmt_external_window_error(conn, sql, params, "stmt case 4")

        # ---- 5. STMT projection path ----
        sql = (
            "select cast(_wstart as bigint) as ws, w.mark, cast(ts as bigint) as ts64, v "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win where mark <= ?) w) "
            "order by ws, ts64"
        )
        params = new_bind_params(1)
        params[0].int(102)
        self._assert_stmt_external_window_error(conn, sql, params, "stmt case 5")

        # ---- 6. STMT with no bind params (pure external_window, still via stmt path) ----
        sql = (
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_cx_src "
            "external_window((select ts, endtime, mark from ext_cx_win) w) "
            "order by ws"
        )
        stmt = conn.statement(sql)
        try:
            stmt.execute()
            raise AssertionError("stmt case 6: expected error but succeeded")
        except Exception as err:
            err_msg = str(err)
            tdLog.info(f"stmt case 6: got expected error: {err_msg}")

        conn.close()
        tdLog.info("=============== external window: stmt negative regression done")

    # ==================================================================
    # Scenario-based regression tests (5.1–5.4 from requirements doc)
    # Covers: fault-alarm correlation, button-door matching, dynamic
    # ratio calculation, event_window + external_window combination,
    # and heavy use of lag/lead functions.
    # ==================================================================

    def prepare_scenario_data(self):
        """Create all tables and insert data for scenario-based tests."""
        tdLog.info("=============== scenario: preparing data")
        tdSql.execute(f"use {self.dbName}")

        # ---- 5.1 Fault / Alarm tables ----
        tdSql.execute("drop table if exists fault1")
        tdSql.execute("drop table if exists alarm1")
        tdSql.execute("drop table if exists car1")

        tdSql.execute(
            "create table fault1 (ts timestamp, event int, val float) "
            "tags(device_id nchar(32))"
        )
        tdSql.execute(
            "create table alarm1 (ts timestamp, event int, val float) "
            "tags(device_id nchar(32))"
        )
        tdSql.execute(
            "create table car1 (ts timestamp, event int, val float) "
            "tags(device_id nchar(32))"
        )

        tdSql.execute("create table fault1_d1 using fault1 tags('d001')")
        tdSql.execute("create table alarm1_d1 using alarm1 tags('d001')")
        tdSql.execute("create table car1_d1   using car1   tags('d001')")

        # base time: 2024-06-01 00:00:00 UTC+8
        ft0 = 1717171200000

        # fault events (event=1): 3 faults at different times
        # fault1: ft0
        # fault2: ft0 + 120s (2 min later)
        # fault3: ft0 + 600s (10 min later)
        # fault4: ft0 + 1800s (30 min later) — event=2 (recovery)
        tdSql.execute(
            f"insert into fault1_d1 values"
            f"({ft0},            1, 100.0)"
            f"({ft0 + 120000},   1, 200.0)"
            f"({ft0 + 600000},   1, 300.0)"
            f"({ft0 + 1800000},  2, 50.0)"
        )

        # alarm events (event=6): scattered alarms
        # some within 60s of fault events, some not
        tdSql.execute(
            f"insert into alarm1_d1 values"
            f"({ft0 + 10000},    6, 10.0)"   # 10s after fault1 -> in fault1 window
            f"({ft0 + 30000},    6, 20.0)"   # 30s after fault1 -> in fault1 window
            f"({ft0 + 50000},    6, 30.0)"   # 50s after fault1 -> in fault1 window
            f"({ft0 + 70000},    6, 40.0)"   # 70s after fault1 -> NOT in fault1 60s window
            f"({ft0 + 130000},   6, 50.0)"   # 10s after fault2 -> in fault2 window
            f"({ft0 + 170000},   6, 60.0)"   # 50s after fault2 -> in fault2 window
            f"({ft0 + 610000},   6, 70.0)"   # 10s after fault3 -> in fault3 window
            f"({ft0 + 700000},   6, 80.0)"   # 100s after fault3 -> NOT in fault3 60s window
            f"({ft0 + 1810000},  6, 90.0)"   # 10s after fault4 (event=2)
        )

        # car events: between faults
        tdSql.execute(
            f"insert into car1_d1 values"
            f"({ft0 + 5000},     3, 1.0)"
            f"({ft0 + 65000},    3, 2.0)"
            f"({ft0 + 125000},   3, 3.0)"
            f"({ft0 + 300000},   3, 4.0)"
            f"({ft0 + 620000},   3, 5.0)"
            f"({ft0 + 900000},   3, 6.0)"
            f"({ft0 + 1200000},  3, 7.0)"
            f"({ft0 + 1500000},  3, 8.0)"
            f"({ft0 + 1850000},  3, 9.0)"
        )

        # ---- 5.2 Button / Door tables ----
        tdSql.execute("drop table if exists button1")
        tdSql.execute("drop table if exists door1")

        tdSql.execute(
            "create table button1 (ts timestamp, event int, targetFloor int, landingFloor int) "
            "tags(device_id nchar(32))"
        )
        tdSql.execute(
            "create table door1 (ts timestamp, event int, door int) "
            "tags(device_id nchar(32))"
        )

        tdSql.execute("create table button1_d1 using button1 tags('elev01')")
        tdSql.execute("create table door1_d1   using door1   tags('elev01')")

        bt0 = 1717200000000  # 2024-06-01 08:00:00

        # button events:
        # Landing call (event=1): landingFloor matters, targetFloor irrelevant
        # Target call (event=2): targetFloor matters, landingFloor irrelevant
        tdSql.execute(
            f"insert into button1_d1 values"
            f"({bt0},           1, 0, 3)"   # landing call at floor 3
            f"({bt0 + 300000},  2, 5, 0)"   # target call to floor 5
            f"({bt0 + 600000},  1, 0, 7)"   # landing call at floor 7
            f"({bt0 + 900000},  2, 2, 0)"   # target call to floor 2
        )

        # door events:
        tdSql.execute(
            f"insert into door1_d1 values"
            f"({bt0 + 60000},   1, 3)"     # door at floor 3, 60s after landing call floor 3 -> match
            f"({bt0 + 120000},  1, 5)"     # door at floor 5
            f"({bt0 + 180000},  1, 3)"     # door at floor 3
            f"({bt0 + 350000},  1, 5)"     # door at floor 5, 50s after target call floor 5 -> match
            f"({bt0 + 400000},  1, 3)"     # door at floor 3
            f"({bt0 + 500000},  1, 5)"     # door at floor 5
            f"({bt0 + 650000},  1, 7)"     # door at floor 7, 50s after landing call floor 7 -> match
            f"({bt0 + 700000},  1, 2)"     # door at floor 2
            f"({bt0 + 950000},  1, 2)"     # door at floor 2, 50s after target call floor 2 -> match
            f"({bt0 + 1000000}, 1, 7)"     # door at floor 7
        )

        # ---- 5.3 K-line tables (dynamic ratio) ----
        tdSql.execute("drop table if exists k_1m")
        tdSql.execute("drop table if exists k_day")

        tdSql.execute(
            "create table k_1m (ts timestamp, price float) "
            "tags(cmplno nchar(16))"
        )
        tdSql.execute(
            "create table k_day (ts timestamp, a float, b float) "
            "tags(cmplno nchar(16))"
        )

        tdSql.execute("create table k_1m_s1 using k_1m  tags('SH600000')")
        tdSql.execute("create table k_1m_s2 using k_1m  tags('SH600001')")
        tdSql.execute("create table k_day_s1 using k_day tags('SH600000')")
        tdSql.execute("create table k_day_s2 using k_day tags('SH600001')")

        kt0 = 1717257600000  # 2024-06-02 00:00:00

        # Daily K-line: 3 days with a/b values for ratio
        tdSql.execute(
            f"insert into k_day_s1 values"
            f"({kt0},              10.0, 2.0)"
            f"({kt0 + 86400000},   15.0, 3.0)"
            f"({kt0 + 172800000},  24.0, 4.0)"
        )
        tdSql.execute(
            f"insert into k_day_s2 values"
            f"({kt0},              20.0, 4.0)"
            f"({kt0 + 86400000},   30.0, 5.0)"
            f"({kt0 + 172800000},  42.0, 6.0)"
        )

        # 1-minute K-line: data points in each day
        tdSql.execute(
            f"insert into k_1m_s1 values"
            f"({kt0 + 60000},              100.0)"
            f"({kt0 + 120000},             110.0)"
            f"({kt0 + 86400000 + 60000},   120.0)"
            f"({kt0 + 86400000 + 120000},  130.0)"
            f"({kt0 + 172800000 + 60000},  140.0)"
            f"({kt0 + 172800000 + 120000}, 150.0)"
        )
        tdSql.execute(
            f"insert into k_1m_s2 values"
            f"({kt0 + 60000},              200.0)"
            f"({kt0 + 120000},             210.0)"
            f"({kt0 + 86400000 + 60000},   220.0)"
            f"({kt0 + 86400000 + 120000},  230.0)"
            f"({kt0 + 172800000 + 60000},  240.0)"
            f"({kt0 + 172800000 + 120000}, 250.0)"
        )

        # ---- 5.4 Event window voltage table ----
        tdSql.execute("drop table if exists d001")
        tdSql.execute(
            "create table d001 (ts timestamp, voltage float) "
            "tags(device_id nchar(32))"
        )
        tdSql.execute("create table d001_1 using d001 tags('d001')")

        vt0 = 1717300000000

        # Voltage data with dips and recoveries
        tdSql.execute(
            f"insert into d001_1 values"
            f"({vt0},           220.0)"
            f"({vt0 + 10000},   195.0)"
            f"({vt0 + 20000},   185.0)"   # START: voltage <= 190
            f"({vt0 + 30000},   180.0)"
            f"({vt0 + 40000},   175.0)"
            f"({vt0 + 50000},   190.0)"   # still <= 190
            f"({vt0 + 60000},   195.0)"
            f"({vt0 + 70000},   205.0)"   # END: voltage >= 200
            f"({vt0 + 80000},   210.0)"
            f"({vt0 + 90000},   188.0)"   # START: second dip
            f"({vt0 + 100000},  170.0)"
            f"({vt0 + 110000},  200.0)"   # END: voltage >= 200
            f"({vt0 + 120000},  215.0)"
        )

        # ---- Additional alarm data for lag/lead based window tests (5.2.2, 5.2.3) ----
        tdSql.execute("drop table if exists alarm_seq")
        tdSql.execute(
            "create table alarm_seq (ts timestamp, event int, val float) "
            "tags(device_id nchar(32))"
        )
        tdSql.execute("create table alarm_seq_d1 using alarm_seq tags('d001')")

        at0 = 1717250000000
        tdSql.execute(
            f"insert into alarm_seq_d1 values"
            f"({at0},          6, 10.0)"
            f"({at0 + 60000},  6, 20.0)"
            f"({at0 + 180000}, 6, 30.0)"
            f"({at0 + 300000}, 6, 40.0)"
            f"({at0 + 600000}, 6, 50.0)"
        )

        # car events scattered between alarm events
        tdSql.execute("drop table if exists car_seq")
        tdSql.execute(
            "create table car_seq (ts timestamp, event int, val float) "
            "tags(device_id nchar(32))"
        )
        tdSql.execute("create table car_seq_d1 using car_seq tags('d001')")

        tdSql.execute(
            f"insert into car_seq_d1 values"
            f"({at0 + 10000},  3, 1.0)"
            f"({at0 + 30000},  3, 2.0)"
            f"({at0 + 50000},  3, 3.0)"
            f"({at0 + 70000},  3, 4.0)"
            f"({at0 + 100000}, 3, 5.0)"
            f"({at0 + 200000}, 3, 6.0)"
            f"({at0 + 250000}, 3, 7.0)"
            f"({at0 + 400000}, 3, 8.0)"
            f"({at0 + 500000}, 3, 9.0)"
            f"({at0 + 700000}, 3, 10.0)"
            f"({at0 + 800000}, 3, 11.0)"
        )

        # ---- 5.5 Doc example: smart meter (meters + alerts) ----
        # Mirrors the documentation example in 03-query.md "外部窗口" section.
        # meters: ts, current, voltage, phase; tags: groupid(int), location(nchar)
        # alerts: ts, alert_code, alert_value;   tags: groupid(int), location(nchar)
        tdSql.execute("drop table if exists doc_meters")
        tdSql.execute("drop table if exists doc_alerts")

        tdSql.execute(
            "create table doc_meters "
            "(ts timestamp, current float, voltage int, phase float) "
            "tags(groupid int, location nchar(64))"
        )
        tdSql.execute(
            "create table doc_alerts "
            "(ts timestamp, alert_code int, alert_value float) "
            "tags(groupid int, location nchar(64))"
        )

        # Two groups of meters
        tdSql.execute("create table doc_meters_g1_a using doc_meters tags(1, 'California.SanFrancisco')")
        tdSql.execute("create table doc_meters_g2_a using doc_meters tags(2, 'California.LosAngeles')")

        # Two groups of alerts
        tdSql.execute("create table doc_alerts_g1_a using doc_alerts tags(1, 'California.SanFrancisco')")
        tdSql.execute("create table doc_alerts_g2_a using doc_alerts tags(2, 'California.LosAngeles')")

        mt0 = 1717340000000  # base time

        # Group 1 meters: voltage readings, some >= 225 (trigger windows)
        tdSql.execute(
            f"insert into doc_meters_g1_a values"
            f"({mt0},          12.0, 220, 0.31)"
            f"({mt0 + 10000},  12.1, 225, 0.32)"   # voltage >= 225 -> window [+10s, +70s]
            f"({mt0 + 20000},  12.2, 228, 0.33)"   # voltage >= 225 -> window [+20s, +80s]
            f"({mt0 + 30000},  12.3, 218, 0.34)"
            f"({mt0 + 60000},  12.4, 230, 0.35)"   # voltage >= 225 -> window [+60s, +120s]
            f"({mt0 + 120000}, 12.5, 210, 0.36)"
            f"({mt0 + 180000}, 12.6, 226, 0.37)"   # voltage >= 225 -> window [+180s, +240s]
        )

        # Group 2 meters: voltage readings
        tdSql.execute(
            f"insert into doc_meters_g2_a values"
            f"({mt0},          11.0, 222, 0.30)"
            f"({mt0 + 30000},  11.1, 232, 0.31)"   # voltage >= 225 -> window [+30s, +90s]
            f"({mt0 + 90000},  11.2, 215, 0.32)"
            f"({mt0 + 150000}, 11.3, 227, 0.33)"   # voltage >= 225 -> window [+150s, +210s]
        )

        # Group 1 alerts: scattered
        tdSql.execute(
            f"insert into doc_alerts_g1_a values"
            f"({mt0 + 15000},  101, 5.0)"    # in window [+10s, +70s]
            f"({mt0 + 25000},  102, 8.0)"    # in window [+10s, +70s] and [+20s, +80s]
            f"({mt0 + 50000},  103, 12.0)"   # in window [+10s, +70s] and [+20s, +80s]
            f"({mt0 + 75000},  104, 3.0)"    # in window [+20s, +80s]
            f"({mt0 + 100000}, 105, 15.0)"   # in window [+60s, +120s]
            f"({mt0 + 200000}, 106, 7.0)"    # in window [+180s, +240s]
        )

        # Group 2 alerts
        tdSql.execute(
            f"insert into doc_alerts_g2_a values"
            f"({mt0 + 40000},  201, 9.0)"    # in window [+30s, +90s]
            f"({mt0 + 80000},  202, 11.0)"   # in window [+30s, +90s]
            f"({mt0 + 160000}, 203, 6.0)"    # in window [+150s, +210s]
            f"({mt0 + 190000}, 204, 14.0)"   # in window [+150s, +210s]
        )

        # ---- 5.6 Layered aggregation tables for nested external_window ----
        # sensor_data: raw sensor readings
        # event_markers: event start/end markers
        tdSql.execute("drop table if exists layered_sensor")
        tdSql.execute("drop table if exists layered_event")

        tdSql.execute(
            "create table layered_sensor "
            "(ts timestamp, val float) "
            "tags(sid int)"
        )
        tdSql.execute(
            "create table layered_event "
            "(ts timestamp, etype int) "
            "tags(sid int)"
        )

        tdSql.execute("create table layered_sensor_s1 using layered_sensor tags(1)")
        tdSql.execute("create table layered_sensor_s2 using layered_sensor tags(2)")
        tdSql.execute("create table layered_event_s1 using layered_event tags(1)")
        tdSql.execute("create table layered_event_s2 using layered_event tags(2)")

        lt0 = 1717400000000

        # Sensor 1: readings every 10s for 10 min
        sensor_vals_s1 = []
        for i in range(60):
            sensor_vals_s1.append(f"({lt0 + i * 10000}, {10.0 + i * 0.5})")
        tdSql.execute("insert into layered_sensor_s1 values" + "".join(sensor_vals_s1))

        # Sensor 2: readings every 10s for 10 min
        sensor_vals_s2 = []
        for i in range(60):
            sensor_vals_s2.append(f"({lt0 + i * 10000}, {20.0 + i * 0.3})")
        tdSql.execute("insert into layered_sensor_s2 values" + "".join(sensor_vals_s2))

        # Event markers for sensor 1: 3 events
        tdSql.execute(
            f"insert into layered_event_s1 values"
            f"({lt0},          1)"           # event start
            f"({lt0 + 120000}, 2)"           # event end -> event1: [0s, 120s]
            f"({lt0 + 180000}, 1)"           # event start
            f"({lt0 + 300000}, 2)"           # event end -> event2: [180s, 300s]
            f"({lt0 + 360000}, 1)"           # event start
            f"({lt0 + 540000}, 2)"           # event end -> event3: [360s, 540s]
        )

        # Event markers for sensor 2: 2 events (deliberately different from s1)
        tdSql.execute(
            f"insert into layered_event_s2 values"
            f"({lt0 + 60000},  1)"           # event start
            f"({lt0 + 240000}, 2)"           # event end -> event1: [60s, 240s]
            f"({lt0 + 300000}, 1)"           # event start
            f"({lt0 + 480000}, 2)"           # event end -> event2: [300s, 480s]
        )

        tdLog.info("=============== scenario: data preparation done")

    def scenario_fault_alarm_correlation(self):
        """5.1.1 Fault-alarm correlation analysis.

        Goal: Analyze alarm events (event=6) within 60 seconds after each fault event (event=1).
        Uses EXTERNAL_WINDOW with subquery that builds [ts, ts+60s] windows from fault events.
        Uses HAVING COUNT(*) <= 0 to filter windows with no alarms.
        """
        tdLog.info("=============== scenario 5.1.1: fault-alarm correlation")
        tdSql.execute(f"use {self.dbName}")

        ft0 = 1717171200000

        # Basic: fault windows [ts, ts+60s] with alarm counts
        # fault1 at ft0: alarms at +10s, +30s, +50s -> count=3, avg=(10+20+30)/3=20
        # fault2 at ft0+120s: alarms at +10s(=130s), +50s(=170s) -> count=2, avg=(50+60)/2=55
        # fault3 at ft0+600s: alarm at +10s(=610s) -> count=1, avg=70
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.event, w.val, count(*) as c, avg(val) as av "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, ts + 60s, event, val from fault1 where event = 1) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(1, 0, ft0 + 120000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(2, 0, ft0 + 600000)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 3, 1)

        # HAVING count(*) <= 0 — should filter out all windows in this dataset.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.event, w.val, count(*) as c, avg(val) as av "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, ts + 60s, event, val from fault1 where event = 1) w) "
            "having count(*) <= 0 "
            "order by ws"
        )
        tdSql.checkRows(0)

        # Use lead to build windows from current fault to next fault
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, lead(ts, 1) from fault1 where event = 1 order by ts) w) "
            "order by ws"
        )
        # fault1->fault2: [ft0, ft0+120s] alarms: 10s,30s,50s,70s -> 4
        # fault2->fault3: [ft0+120s, ft0+600s] alarms: 130s,170s -> 2
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 0, ft0 + 120000)
        tdSql.checkData(1, 1, 2)

        # Use lag to build backward-looking windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select lag(ts, 1), ts from fault1 where event = 1 order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 0, ft0 + 120000)
        tdSql.checkData(1, 1, 2)

        # lag with default value 0 (epoch) as start
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select lag(ts, 1, 0), ts from fault1 where event = 1 order by ts) w) "
            "order by ws"
        )
        # [0, ft0] -> 0 alarms (all after ft0)
        # [ft0, ft0+120s] -> 4
        # [ft0+120s, ft0+600s] -> 2
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 0, ft0 + 120000)
        tdSql.checkData(1, 1, 2)

        tdLog.info("=============== scenario 5.1.1: done")

    def scenario_fault_alarm_outer_match(self):
        """5.1.2 Fault-alarm outer match (FILL NONE).

        Goal: List all fault events and their correlated alarm events within 60s.
        Faults with no matching alarms should still appear with NULL alarm columns.
        Note: FILL is currently not allowed in external_window so we test error path.
        """
        tdLog.info("=============== scenario 5.1.2: fault-alarm outer match")
        tdSql.execute(f"use {self.dbName}")

        ft0 = 1717171200000

        # Projection: show fault window fields + alarm fields for each match
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.event, w.val, cast(ts as bigint) as ats, val "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, ts + 60s, event, val from fault1 where event = 1) w) "
            "order by ws, ats"
        )
        # fault1 window: 3 alarms
        # fault2 window: 2 alarms
        # fault3 window: 1 alarm
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(3, 0, ft0 + 120000)
        tdSql.checkData(5, 0, ft0 + 600000)

        # FILL error: FILL not allowed with external_window
        tdSql.error(
            "select cast(_wstart as bigint) as ws, w.event, w.val, cast(ts as bigint), val "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, ts + 60s, event, val from fault1 where event = 1) w "
            "fill(none))",
            expectedErrno=0x80002657,
        )

        tdLog.info("=============== scenario 5.1.2: done")

    def scenario_nested_external_window(self):
        """5.1.3 Multi-level nested external_window.

        Goal: Test nested EXTERNAL_WINDOW at multiple levels by passing _wstart/_wend
        through successive nesting layers.
        Level 1: fault → alarm (count alarms in each 60s fault window)
        Level 2: level1 windows → car (count cars in same windows)
        Level 3: level2 windows → alarm (count alarms again in same windows)
        """
        tdLog.info("=============== scenario 5.1.3: nested external_window")
        tdSql.execute(f"use {self.dbName}")

        ft0 = 1717171200000

        # Level 1: fault windows [ts, ts+60s] → count alarm events
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, "
            "count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, ts + 60s from fault1 where event = 1) w) "
            "order by ws"
        )
        # fault1 [ft0, ft0+60s]: 3 alarms
        # fault2 [ft0+120s, ft0+180s]: 2 alarms
        # fault3 [ft0+600s, ft0+660s]: 1 alarm
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 0, ft0 + 120000)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, ft0 + 600000)
        tdSql.checkData(2, 2, 1)

        # Level 2: Pass _wstart, _wend from level 1 → count car events
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car1 "
            "external_window("
            "(select _wstart, _wend, count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, ts + 60s from fault1 where event = 1) w)) w2) "
            "order by ws"
        )
        # Same windows applied to car1:
        # [ft0, ft0+60s]: car at ft0+5s → 1
        # [ft0+120s, ft0+180s]: car at ft0+125s → 1
        # [ft0+600s, ft0+660s]: car at ft0+620s → 1
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, ft0 + 120000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, ft0 + 600000)
        tdSql.checkData(2, 1, 1)

        # Full 3-level nesting: fault→alarm→car→alarm
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select _wstart, _wend, count(*) as c "
            "from car1 "
            "external_window("
            "(select _wstart, _wend, count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, ts + 60s from fault1 where event = 1) w)) w2)) w3) "
            "order by ws"
        )
        # Same windows pass through all 3 levels, final alarm counts:
        # [ft0, ft0+60s]: 3 alarms
        # [ft0+120s, ft0+180s]: 2 alarms
        # [ft0+600s, ft0+660s]: 1 alarm
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, ft0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, ft0 + 120000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, ft0 + 600000)
        tdSql.checkData(2, 1, 1)

        tdLog.info("=============== scenario 5.1.3: done")

    def scenario_button_door_matching(self):
        """5.2.1 Button-door event matching.

        Goal: Find the first door event after each button event where the floor matches.
        Using EXTERNAL_WINDOW with button event defining [ts, ts+10m] windows.
        """
        tdLog.info("=============== scenario 5.2.1: button-door matching")
        tdSql.execute(f"use {self.dbName}")

        bt0 = 1717200000000

        # Button windows [ts, ts+10min] with door event counts
        tdSql.query(
            "select cast(_wstart as bigint) as ws, first(ts) as first_door "
            "from door1 "
            "external_window("
            "(select ts, ts + 10m, event, targetFloor, landingFloor from button1) w) "
            "having count(*) > 0 "
            "order by ws"
        )
        # All 4 buttons have doors in their 10-min windows
        tdSql.checkRows(4)

        # Use lead to build windows between consecutive button presses
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from door1 "
            "external_window("
            "(select ts, lead(ts, 1) from button1 order by ts) w) "
            "order by ws"
        )
        # button1->button2: [bt0, bt0+300s] doors: 60s,120s,180s -> 3
        # button2->button3: [bt0+300s, bt0+600s] doors: 350s,400s,500s -> 3
        # button3->button4: [bt0+600s, bt0+900s] doors: 650s,700s -> 2
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, bt0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, bt0 + 300000)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 0, bt0 + 600000)
        tdSql.checkData(2, 1, 2)

        # Use lag for backward button windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from door1 "
            "external_window("
            "(select lag(ts, 1), ts from button1 order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, bt0)
        tdSql.checkData(0, 1, 3)

        tdLog.info("=============== scenario 5.2.1: done")

    def scenario_lag_lead_consecutive_windows(self):
        """5.2.2 & 5.2.3 Inter-event window statistics using lag/lead functions.

        Goal: Count car events between consecutive alarm events.
        """
        tdLog.info("=============== scenario 5.2.2/5.2.3: lag/lead consecutive alarm windows")
        tdSql.execute(f"use {self.dbName}")

        at0 = 1717250000000

        # Method 1: lead(ts, 1) — windows from current alarm to next alarm
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1) from alarm_seq order by ts) w) "
            "order by ws"
        )
        # alarm1->2: [at0, at0+60s] cars: +10s,+30s,+50s = 3
        # alarm2->3: [at0+60s, at0+180s] cars: +70s,+100s = 2
        # alarm3->4: [at0+180s, at0+300s] cars: +200s,+250s = 2
        # alarm4->5: [at0+300s, at0+600s] cars: +400s,+500s = 2
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, at0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, at0 + 60000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, at0 + 180000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 0, at0 + 300000)
        tdSql.checkData(3, 1, 2)

        # Method 2: lag(ts, 1) — backward windows from previous alarm to current
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select lag(ts, 1), ts from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, at0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, at0 + 60000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, at0 + 180000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 0, at0 + 300000)
        tdSql.checkData(3, 1, 2)

        # Method 3: lead with far-future default so last alarm also generates a window
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1, '2099-12-31 23:59:59.999') from alarm_seq order by ts) w) "
            "order by ws"
        )
        # Same 4 + last alarm -> [at0+600s, far_future]: cars at +700s,+800s = 2
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, at0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(4, 0, at0 + 600000)
        tdSql.checkData(4, 1, 2)

        # Verify total: all window counts add up
        tdSql.query(
            "select sum(c) from ("
            "select count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1, '2099-12-31 23:59:59.999') from alarm_seq order by ts) w))"
        )
        tdSql.checkData(0, 0, 11)  # total 11 car events

        # lead with offset=2: skip-one windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 2) from alarm_seq order by ts) w) "
            "order by ws"
        )
        # alarm1: [at0, at0+180s] = 5 cars
        # alarm2: [at0+60s, at0+300s] = 4 cars
        # alarm3: [at0+180s, at0+600s] = 4 cars
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, at0)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 0, at0 + 60000)
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, at0 + 180000)
        tdSql.checkData(2, 1, 4)

        # lag with offset=2: backward skip-one windows
        # NULL lag values become timestamp 0 (epoch); [0, at0] has 0 cars (not output)
        # [0, at0+60k] matches 3 cars; then same 3 windows as lead(ts,2)
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select lag(ts, 2), ts from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, at0)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(2, 0, at0 + 60000)
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(3, 0, at0 + 180000)
        tdSql.checkData(3, 1, 4)

        # Combine lag and lead: window from previous alarm to next alarm
        # alarm1: lag=NULL→0, lead=at0+60k → [0, at0+60k]: 3 cars
        # alarm2: [at0, at0+180k]: 5 cars
        # alarm3: [at0+60k, at0+300k]: 4 cars
        # alarm4: [at0+180k, at0+600k]: 4 cars
        # alarm5: lag=at0+300k, lead=NULL→0 → invalid (end<start), not output
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select lag(ts, 1), lead(ts, 1) from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, at0)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(2, 0, at0 + 60000)
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(3, 0, at0 + 180000)
        tdSql.checkData(3, 1, 4)

        # lead without explicit ORDER BY (should default to ts order)
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1) from alarm_seq) w) "
            "order by ws"
        )
        tdSql.checkRows(4)

        # Timestamp arithmetic with lead: shrink windows by 5s on each side
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts + 5s, lead(ts, 1) - 5s from alarm_seq order by ts) w) "
            "order by ws"
        )
        # alarm1: [at0+5s, at0+55s]: cars at +10s,+30s,+50s = 3
        # alarm2: [at0+65s, at0+175s]: cars at +70s,+100s = 2
        # alarm3: [at0+185s, at0+295s]: cars at +200s,+250s = 2
        # alarm4: [at0+305s, at0+595s]: cars at +400s,+500s = 2
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, at0 + 5000)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, at0 + 65000)
        tdSql.checkData(1, 1, 2)

        # Invalid window (endtime < starttime) should produce no rows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts + 60s, lag(ts, 1) from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(0)

        tdLog.info("=============== scenario 5.2.2/5.2.3: done")

    def scenario_dynamic_ratio(self):
        """5.3 Dynamic ratio calculation (grouped calculation).

        Goal: Calculate dynamic adjustment ratios from daily K-line data,
        then apply to minute K-line data using EXTERNAL_WINDOW.
        Uses lead(ts, 1) to define windows [day_i, day_{i+1}].
        Note: Cross-table PARTITION BY uses gid-based matching (not tag values),
        so we use child tables directly to avoid mismatch.
        """
        tdLog.info("=============== scenario 5.3: dynamic ratio calculation")
        tdSql.execute(f"use {self.dbName}")

        kt0 = 1717257600000

        # Verify daily ratio calculation with lead
        tdSql.query(
            "select cast(ts as bigint), a/b as rt, "
            "cast(lead(ts, 1) as bigint) as next_ts "
            "from k_day_s1 order by ts"
        )
        tdSql.checkRows(3)

        # Use lead(ts,1) to build day windows for SH600000
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, avg(price) as ap "
            "from k_1m_s1 "
            "external_window("
            "(select ts, lead(ts, 1) from k_day_s1 order by ts) w) "
            "order by ws"
        )
        # day1->[kt0, kt0+86400s]: 2 pts, avg=105; day2->[kt0+86400s, kt0+172800s]: 2 pts, avg=125
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, kt0)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, kt0 + 86400000)
        tdSql.checkData(1, 1, 2)

        # SH600001 with same pattern
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, avg(price) as ap "
            "from k_1m_s2 "
            "external_window("
            "(select ts, lead(ts, 1) from k_day_s2 order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, kt0)
        tdSql.checkData(0, 1, 2)

        # Projection with price in each day window
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64, price "
            "from k_1m_s1 "
            "external_window("
            "(select ts, lead(ts, 1) from k_day_s1 order by ts) w) "
            "order by ws, ts64"
        )
        # 2 days * 2 points/day = 4
        tdSql.checkRows(4)

        # lag(ts,1) to build backward [prev_day, current_day] windows
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from k_1m_s1 "
            "external_window("
            "(select lag(ts, 1), ts from k_day_s1 order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(2)

        # Carry ratio data through window columns (alias 'rt' to avoid reserved keyword 'ratio')
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.rt, count(*) as c "
            "from k_1m_s1 "
            "external_window("
            "(select ts, lead(ts, 1), a/b as rt from k_day_s1 "
            "order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(2)
        # day1 ratio = 10/2 = 5, day2 ratio = 15/3 = 5
        tdSql.checkData(0, 1, 5.0)
        tdSql.checkData(1, 1, 5.0)

        tdLog.info("=============== scenario 5.3: done")

    def scenario_event_window_external(self):
        """5.4 Event window + external window combination.

        Goal: Use EVENT_WINDOW to detect voltage dip episodes, then use those episodes
        as external windows to count alarm events.
        """
        tdLog.info("=============== scenario 5.4: event_window + external_window")
        tdSql.execute(f"use {self.dbName}")

        vt0 = 1717300000000

        # Verify event_window detects voltage dips
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, count(*) as c "
            "from d001 "
            "event_window start with voltage <= 190 end with voltage >= 200 "
            "order by ws"
        )
        # Dip 1: [vt0+20000, vt0+70000] 6 rows
        # Dip 2: [vt0+90000, vt0+110000] 3 rows
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, vt0 + 20000)
        tdSql.checkData(0, 1, vt0 + 70000)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, vt0 + 90000)
        tdSql.checkData(1, 1, vt0 + 110000)
        tdSql.checkData(1, 2, 3)

        # Create alarm data in same time range for cross-window test
        tdSql.execute("drop table if exists alarm_volt")
        tdSql.execute(
            "create table alarm_volt (ts timestamp, event int, val float) "
            "tags(device_id nchar(32))"
        )
        tdSql.execute("create table alarm_volt_d1 using alarm_volt tags('d001')")
        tdSql.execute(
            f"insert into alarm_volt_d1 values"
            f"({vt0 + 25000},  6, 10.0)"
            f"({vt0 + 35000},  6, 20.0)"
            f"({vt0 + 55000},  6, 30.0)"
            f"({vt0 + 75000},  6, 40.0)"
            f"({vt0 + 95000},  6, 50.0)"
            f"({vt0 + 105000}, 6, 60.0)"
        )

        # Event_window as external_window subquery
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, avg(val) as av "
            "from alarm_volt "
            "where event = 6 "
            "external_window("
            "(select _wstart, _wend, count(*) as wc "
            "from d001 "
            "event_window start with voltage <= 190 end with voltage >= 200) w) "
            "order by ws"
        )
        # Dip 1 [vt0+20000, vt0+70000]: alarms +25s,+35s,+55s -> 3
        # Dip 2 [vt0+90000, vt0+110000]: alarms +95s,+105s -> 2
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, vt0 + 20000)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, vt0 + 90000)
        tdSql.checkData(1, 1, 2)

        # With w.wc (event_window count) in projection
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.wc, count(*) as c "
            "from alarm_volt "
            "where event = 6 "
            "external_window("
            "(select _wstart, _wend, count(*) as wc "
            "from d001 "
            "event_window start with voltage <= 190 end with voltage >= 200) w) "
            "order by ws"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, vt0 + 20000)
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 0, vt0 + 90000)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 2)

        tdLog.info("=============== scenario 5.4: done")

    def scenario_lag_lead_advanced(self):
        """Advanced lag/lead usage within external_window subqueries.

        Tests various lag/lead patterns:
        - lag/lead carrying extra data columns into windows
        - lag/lead with default values on integer columns (float defaults unsupported)
        - Multiple lag/lead calls in same subquery (without defaults)
        - PARTITION BY with same super table for both inner and outer
        - Error case: PARTITION BY in subquery without outer partition
        """
        tdLog.info("=============== scenario: advanced lag/lead patterns")
        tdSql.execute(f"use {self.dbName}")

        at0 = 1717250000000

        # 1. lead(val, 1) in subquery — carry extra data into window
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.val, w.next_val, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1), val, lead(val, 1) as next_val from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10.0)
        tdSql.checkData(0, 2, 20.0)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(1, 1, 20.0)
        tdSql.checkData(1, 2, 30.0)

        # 2. lag(val, 1) to carry previous alarm value
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.prev_val, w.val, count(*) as c "
            "from car_seq "
            "external_window("
            "(select lag(ts, 1), ts, lag(val, 1) as prev_val, val from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10.0)
        tdSql.checkData(0, 2, 20.0)
        tdSql.checkData(0, 3, 3)

        # 3. lead with default on integer column (float defaults cause "Invalid parameter data type")
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.ne, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1, '2099-12-31 23:59:59.999'), "
            "lead(event, 1, -1) as ne from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(5)
        tdSql.checkData(3, 1, 6)    # alarm4 event=6, next=alarm5 event=6
        tdSql.checkData(4, 1, -1)   # alarm5 lead default -1
        tdSql.checkData(4, 2, 2)    # 2 cars in last window

        # 3b. Verify lead/lag with float default is an error (implementation limitation)
        tdSql.error(
            "select lead(val, 1, -1.0) from alarm_seq order by ts"
        )

        # 4. PARTITION BY in subquery without matching outer PARTITION BY — error
        tdSql.error(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from alarm1 "
            "where event = 6 "
            "external_window("
            "(select ts, lead(ts, 1) from fault1 "
            "where event = 1 "
            "partition by device_id order by ts) w) "
            "order by ws"
        )

        # 4b. PARTITION BY on same super table (inner and outer) — works
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from alarm_seq "
            "partition by device_id "
            "external_window("
            "(select ts, lead(ts, 1) from alarm_seq "
            "partition by device_id order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)

        # 5. Multiple lag/lead calls in same subquery (without defaults for float columns)
        tdSql.query(
            "select cast(_wstart as bigint) as ws, "
            "w.lag1_val, w.cur_val, w.lead1_val, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1), "
            "lag(val, 1) as lag1_val, "
            "val as cur_val, "
            "lead(val, 1) as lead1_val "
            "from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, None)   # alarm1 lag=NULL
        tdSql.checkData(0, 2, 10.0)
        tdSql.checkData(0, 3, 20.0)
        tdSql.checkData(1, 1, 10.0)
        tdSql.checkData(1, 2, 20.0)
        tdSql.checkData(1, 3, 30.0)

        # 6. lead on integer event column with default
        tdSql.query(
            "select cast(_wstart as bigint) as ws, w.cur_event, w.next_event, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1), event as cur_event, lead(event, 1, -1) as next_event "
            "from alarm_seq order by ts) w) "
            "order by ws"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)

        # 7. lag/lead in outer query of external_window is not allowed,
        #    currently causes taosd crash — skip automated test, filed as known issue.
        tdSql.error(
            "select lag(ts, 1), count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts, lead(ts, 1) from alarm_seq order by ts) w) "
            "order by ws"
        )

        tdLog.info("=============== scenario: advanced lag/lead done")

    def scenario_diff_union_window(self):
        """5.2.3 Alternative: Use diff to build inter-alarm windows.

        The requirement shows using diff(ts) to compute intervals and build windows
        between consecutive events.
        """
        tdLog.info("=============== scenario 5.2.3: diff-based window construction")
        tdSql.execute(f"use {self.dbName}")

        at0 = 1717250000000

        # Verify diff on timestamp column (first row has NULL diff, excluded from output)
        tdSql.query(
            "select cast(ts as bigint), diff(ts) from alarm_seq order by ts"
        )
        tdSql.checkRows(4)

        # diff-based subquery: ts - diff(ts) gives previous ts, ts gives current
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from car_seq "
            "external_window("
            "(select ts - diff(ts), ts from alarm_seq order by ts) w) "
            "order by ws"
        )
        # alarm2: [at0, at0+60s]: 3 cars
        # alarm3: [at0+60s, at0+180s]: 2 cars
        # alarm4: [at0+180s, at0+300s]: 2 cars
        # alarm5: [at0+300s, at0+600s]: 2 cars
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, at0)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, at0 + 60000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, at0 + 180000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 0, at0 + 300000)
        tdSql.checkData(3, 1, 2)

        tdLog.info("=============== scenario 5.2.3: diff-based window done")

    def scenario_doc_smart_meter_example(self):
        """5.5 Documentation example: smart meter voltage anomaly with alerts.

        Mirrors the SQL example from docs/zh/05-basic/03-query.md "外部窗口" section.
        Uses meters (voltage >= 225 as anomaly trigger) to define 60s windows,
        then counts alerts within each window, partitioned by groupid.
        """
        tdLog.info("=============== scenario 5.5: doc smart meter example")
        tdSql.execute(f"use {self.dbName}")

        mt0 = 1717340000000

        # -- Test 1: Basic doc example query (PARTITION BY groupid, HAVING count > 0) --
        # Group 1 voltage >= 225 events: +10s, +20s, +60s, +180s
        # Group 2 voltage >= 225 events: +30s, +150s
        # Each creates a 60s window: [event_ts, event_ts + 60s]
        #
        # Group 1 windows and matching alerts:
        #   [+10s, +70s]:   alerts at +15s(5.0), +25s(8.0), +50s(12.0) -> count=3, max=12.0
        #   [+20s, +80s]:   alerts at +25s(8.0), +50s(12.0), +75s(3.0) -> count=3, max=12.0
        #   [+60s, +120s]:  alerts at +75s(3.0), +100s(15.0) -> count=2, max=15.0
        #   [+180s, +240s]: alerts at +200s(7.0) -> count=1, max=7.0
        # Group 2 windows and matching alerts:
        #   [+30s, +90s]:   alerts at +40s(9.0), +80s(11.0) -> count=2, max=11.0
        #   [+150s, +210s]: alerts at +160s(6.0), +190s(14.0) -> count=2, max=14.0
        tdSql.query(
            "select w.groupid, w.location, "
            "cast(_wstart as bigint) as ws, "
            "count(a.*) as alert_count, "
            "max(a.alert_value) as max_alert_value "
            "from doc_alerts a "
            "partition by a.groupid "
            "external_window("
            "(select ts, ts + 60s, groupid, location "
            " from doc_meters "
            " where voltage >= 225 "
            " partition by groupid) w) "
            "having count(a.*) > 0 "
            "order by w.groupid, ws"
        )
        # Group 1: 4 windows, all have alerts
        # Group 2: 2 windows, all have alerts
        tdSql.checkRows(6)
        # Group 1 window 1: [mt0+10000, mt0+70000]
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, mt0 + 10000)
        tdSql.checkData(0, 3, 3)
        # Group 1 window 2: [mt0+20000, mt0+80000]
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 2, mt0 + 20000)
        tdSql.checkData(1, 3, 3)
        # Group 1 window 3: [mt0+60000, mt0+120000]
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 2, mt0 + 60000)
        tdSql.checkData(2, 3, 2)
        # Group 1 window 4: [mt0+180000, mt0+240000]
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 2, mt0 + 180000)
        tdSql.checkData(3, 3, 1)
        # Group 2 window 1: [mt0+30000, mt0+90000]
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(4, 2, mt0 + 30000)
        tdSql.checkData(4, 3, 2)
        # Group 2 window 2: [mt0+150000, mt0+210000]
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(5, 2, mt0 + 150000)
        tdSql.checkData(5, 3, 2)

        # -- Test 2: Without HAVING (includes all windows, even empty ones) --
        tdSql.query(
            "select w.groupid, "
            "cast(_wstart as bigint) as ws, "
            "count(a.*) as alert_count "
            "from doc_alerts a "
            "partition by a.groupid "
            "external_window("
            "(select ts, ts + 60s, groupid, location "
            " from doc_meters "
            " where voltage >= 225 "
            " partition by groupid) w) "
            "order by w.groupid, ws"
        )
        tdSql.checkRows(6)

        # -- Test 3: AVG aggregation on alert_value --
        tdSql.query(
            "select w.groupid, "
            "cast(_wstart as bigint) as ws, "
            "avg(a.alert_value) as avg_val "
            "from doc_alerts a "
            "partition by a.groupid "
            "external_window("
            "(select ts, ts + 60s, groupid, location "
            " from doc_meters "
            " where voltage >= 225 "
            " partition by groupid) w) "
            "having count(a.*) > 0 "
            "order by w.groupid, ws"
        )
        tdSql.checkRows(6)

        # -- Test 4: Subquery has PARTITION BY but outer query has none -> syntax error --
        # Doc 约束与限制: "若外部窗口（内部子查询）使用了分组，则外部查询必须同时使用 PARTITION BY；否则语法报错"
        # The subquery below uses partition by groupid; the outer query has no partition by -> error.
        tdSql.error(
            "select cast(_wstart as bigint) as ws, "
            "count(a.*) as alert_count "
            "from doc_alerts a "
            "external_window("
            "(select ts, ts + 60s, groupid, location "
            " from doc_meters "
            " where voltage >= 225 "
            " partition by groupid order by ts) w)"
        )

        # -- Test 5: No partition by in subquery -> all data share one global window set --
        # Doc 分组和对齐 bullet 3: "当子查询未使用 PARTITION BY 时，内部子查询只生成一组共享窗口。"
        # Both subquery and outer query have no PARTITION BY here, so all alerts from all groups
        # are counted together against the single shared window set derived from all voltage events.
        tdSql.query(
            "select cast(_wstart as bigint) as ws, "
            "count(a.*) as alert_count "
            "from doc_alerts a "
            "external_window("
            "(select ts, ts + 60s, groupid, location "
            " from doc_meters "
            " where voltage >= 225 order by ts) w) "
            "order by ws"
        )
        # Windows from both groups interleaved by time:
        # +10s, +20s, +30s, +60s, +150s, +180s  (all voltage>=225 events sorted)
        # All alerts (both groups) match against these global windows
        tdSql.checkRows(6)

        # -- Test 6: Window attribute column in ORDER BY --
        tdSql.query(
            "select w.groupid, w.location, "
            "cast(_wstart as bigint) as ws, "
            "count(a.*) as alert_count "
            "from doc_alerts a "
            "partition by a.groupid "
            "external_window("
            "(select ts, ts + 60s, groupid, location "
            " from doc_meters "
            " where voltage >= 225 "
            " partition by groupid) w) "
            "having count(a.*) > 0 "
            "order by w.groupid desc, ws"
        )
        tdSql.checkRows(6)
        # Group 2 first
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(2, 0, 1)

        # -- Test 7: ORDER BY in partitioned subquery breaks partition alignment (doc limitation) --
        # Doc "分组和对齐" bullet 5 (限制与注意事项，未来版本可能变化):
        #   "当内外查询都使用 PARTITION BY，且窗口子查询中再使用 ORDER BY 时，
        #    排序可能打乱各分组窗口流的原有组织方式；外部查询可能作用于合并后的窗口流，
        #    表现为内部分组语义失效（等同未分组），不再按内外分组一一对齐。"
        #
        # Compare with Test 1 (no ORDER BY in subquery) which yields 6 rows:
        #   Group 1 sees only its 4 windows → 4 rows
        #   Group 2 sees only its 2 windows → 2 rows
        #
        # With ORDER BY ts added to the subquery, all 6 windows are merged into one sorted stream.
        # Each outer partition (a.groupid=1 and a.groupid=2) now sees ALL 6 merged windows:
        #   Merged windows (sorted by ts): [+10s], [+20s], [+30s], [+60s], [+150s], [+180s]
        #   Group 1 alerts (+15s, +25s, +50s, +75s, +100s, +200s) match all 6 windows → 6 rows
        #   Group 2 alerts (+40s, +80s, +160s, +190s) match all 6 windows → 6 rows
        # Total: 12 rows (doubled from 6), demonstrating the alignment failure.
        tdSql.query(
            "select w.groupid, "
            "cast(_wstart as bigint) as ws, "
            "count(a.*) as alert_count "
            "from doc_alerts a "
            "partition by a.groupid "
            "external_window("
            "(select ts, ts + 60s, groupid, location "
            " from doc_meters "
            " where voltage >= 225 "
            " partition by groupid order by ts) w) "
            "having count(a.*) > 0 "
            "order by ws"
        )
        # 12 rows: each of the 6 merged windows is matched by both outer partitions
        # (vs 6 rows in Test 1 where each group only sees its own windows)
        tdSql.checkRows(12)

        tdLog.info("=============== scenario 5.5: doc smart meter example done")

    def scenario_nested_layered_aggregation(self):
        """5.6 Nested external_window for layered aggregation.

        Demonstrates the doc description:
        "先用第一层外部窗口按事件划定时间范围并聚合出中间指标，
         再用第二层外部窗口在新的时间范围内对这些中间指标做二次聚合。"

        Level 1: Use event markers (etype=1 start, etype=2 end) to build windows,
                 aggregate sensor readings within each event window (avg, count).
        Level 2: Use the level-1 result windows to build larger time ranges,
                 aggregate the raw sensor data again at a coarser granularity.
        """
        tdLog.info("=============== scenario 5.6: nested layered aggregation")
        tdSql.execute(f"use {self.dbName}")

        lt0 = 1717400000000

        # -- Level 1: event windows from event_markers -> aggregate sensor data --
        # This query uses etype=1 start markers + lead(ts, 1), so windows are
        # start-to-next-start, not explicit start/end pairs.
        # Sensor 1 start markers at: lt0, lt0+180s, lt0+360s -> windows [0,180s], [180s,360s]
        # Sensor 1 readings every 10s: val = 10.0 + i*0.5 for i in 0..59
        #   [0, 180s]: i=0..18 -> 19 readings
        #   [180s, 360s]: i=18..36 -> 19 readings
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, "
            "count(*) as c, avg(val) as av "
            "from layered_sensor_s1 "
            "external_window("
            "(select ts, lead(ts, 1) from layered_event_s1 "
            " where etype = 1 order by ts) w) "
            "order by ws"
        )
        # event1: [lt0, lt0+180000] (from start1 to start2)
        # event2: [lt0+180000, lt0+360000] (from start2 to start3)
        # lead(ts, 1) gives next start event timestamp
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, lt0)
        tdSql.checkData(1, 0, lt0 + 180000)

        # Alternative: use paired start/end markers to define windows
        # Build windows from [start_ts, end_ts] pairs using lead on interleaved events
        tdSql.query(
            "select cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, "
            "count(*) as c "
            "from layered_sensor_s1 "
            "external_window("
            "(select ts, lead(ts, 1) from layered_event_s1 "
            " where etype in (1, 2) order by ts) w) "
            "order by ws"
        )
        # Pairs: [lt0, lt0+120s], [lt0+120s, lt0+180s], [lt0+180s, lt0+300s],
        #        [lt0+300s, lt0+360s], [lt0+360s, lt0+540s]
        tdSql.checkRows(5)

        # -- Level 2: Nested - use level-1 aggregate windows to drive level-2 --
        # Inner: start-to-next-start windows from etype=1 markers on sensor 1
        # Middle: aggregate sensor 1 data in those windows -> produces _wstart, _wend, count, avg
        # Outer: use those same windows to aggregate sensor 2 data
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c, avg(val) as av "
            "from layered_sensor_s2 "
            "external_window("
            "(select _wstart, _wend, count(*) as inner_cnt "
            " from layered_sensor_s1 "
            " external_window("
            " (select ts, lead(ts, 1) from layered_event_s1 "
            "  where etype = 1 order by ts) w1"
            ") order by _wstart) w2) "
            "order by ws"
        )
        # Level-1 produces 2 windows (start-to-start): [lt0, lt0+180s], [lt0+180s, lt0+360s]
        # Level-2 aggregates sensor_s2 in those windows
        # sensor_s2: val = 20.0 + i*0.3 for i in 0..59, readings every 10s
        #   [lt0, lt0+180s]: i=0..18 -> 19 readings (closed-closed boundary)
        #   [lt0+180s, lt0+360s]: i=18..36 -> 19 readings
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, lt0)
        tdSql.checkData(0, 1, 19)
        tdSql.checkData(1, 0, lt0 + 180000)
        tdSql.checkData(1, 1, 19)

        # -- Level 2 with PARTITION BY: nested with both levels partitioned --
        # Inner events partitioned by sid, outer sensor data partitioned by sid
        tdSql.query(
            "select sid, cast(_wstart as bigint) as ws, count(*) as c "
            "from layered_sensor "
            "partition by sid "
            "external_window("
            "(select _wstart, _wend, sid, count(*) as inner_cnt "
            " from layered_sensor "
            " partition by sid "
            " external_window("
            " (select ts, lead(ts, 1) from layered_event "
            "  where etype = 1 partition by sid) w1"
            ")) w2) "
            "order by sid, ws"
        )
        # Expected partition-alignment behavior for this nested case:
        # both sid=1 and sid=2 windows are preserved and matched by sid.
        # sid=1 windows: [lt0, +180k], [lt0+180k, +360k]
        # sid=2 window:  [lt0+60k, +300k]
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, lt0)
        tdSql.checkData(0, 2, 19)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, lt0 + 180000)
        tdSql.checkData(1, 2, 19)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, lt0 + 60000)
        tdSql.checkData(2, 2, 25)

        # -- 3-level nesting: event->sensor1->sensor2->count --
        tdSql.query(
            "select cast(_wstart as bigint) as ws, count(*) as c "
            "from layered_sensor_s2 "
            "external_window("
            "(select _wstart, _wend, count(*) as c2 "
            " from layered_sensor_s2 "
            " external_window("
            " (select _wstart, _wend, count(*) as c1 "
            "  from layered_sensor_s1 "
            "  external_window("
            "  (select ts, lead(ts, 1) from layered_event_s1 "
            "   where etype = 1 order by ts) w1"
            " )) w2"
            ")) w3) "
            "order by ws"
        )
        # Windows pass through all 3 levels unchanged
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, lt0)
        tdSql.checkData(0, 1, 19)
        tdSql.checkData(1, 0, lt0 + 180000)
        tdSql.checkData(1, 1, 19)

        tdLog.info("=============== scenario 5.6: nested layered aggregation done")

    def scenario_regression(self):
        """Main entry point for all scenario-based tests."""
        tdLog.info("=============== scenario regression: start")
        self.prepare_scenario_data()
        self.scenario_fault_alarm_correlation()
        self.scenario_fault_alarm_outer_match()
        self.scenario_nested_external_window()
        self.scenario_button_door_matching()
        self.scenario_lag_lead_consecutive_windows()
        self.scenario_dynamic_ratio()
        self.scenario_event_window_external()
        self.scenario_lag_lead_advanced()
        self.scenario_diff_union_window()
        self.scenario_doc_smart_meter_example()
        self.scenario_nested_layered_aggregation()
        tdLog.info("=============== scenario regression: done")
