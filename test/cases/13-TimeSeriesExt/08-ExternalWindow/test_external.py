import os

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

        # to do: used group by in subquey
        # to do: vitual table with external window
        # to do: FILL 本期未实现，测试加上
        # to do: test in stmt scenario

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
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) having sum(v) > 20;", 8),
        ])

    def complex_partition_and_having_no_sort(self):
        tdLog.info("=============== external window: complex partition and having no sort")
        tdSql.execute(f"use {self.dbName}")
        self._check_no_sort_rows([
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w);", 8),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) having count(*) > 1;", 8),
            ("select t1, cast(_wstart as bigint) as ws, count(*) as c, sum(v) as sv from ext_cx_src partition by t1 external_window((select ts, endtime, mark from ext_cx_win) w) having sum(v) > 20;", 8),
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
                   
        # SELECT
        # a.ws,
        # a.c AS c_a,
        # b.c AS c_b,
        # a.total_rows AS total_rows_a,
        # b.total_rows AS total_rows_b
        # FROM (
        # select cast(_wstart as bigint) as ws, count(*) as c,
        #         (select count(*) from (select ts from ext_join_src) t) as total_rows
        # from ext_join_src
        # external_window((select jw.ts, jw.endtime
        #                 from ext_join_win jw, ext_join_src jd
        #                 where jw.ts=jd.ts) w)
        # order by ws
        # ) a
        # JOIN (
        # select cast(_wstart as bigint) as ws, count(*) as c,
        #         (select count(*) from (select ts from ext_join_src) t) as total_rows
        # from ext_join_src
        # external_window((select jw.ts, jw.endtime
        #                 from ext_join_win jw left join ext_join_src jd on jw.ts=jd.ts) w)
        # order by ws
        # ) b
        # ON a.ws = b.ws
        # ORDER BY a.ws;

        # SELECT
        # a.ws,
        # a.c AS c_a,
        # b.c AS c_b,
        # a.total_rows AS total_rows_a,
        # b.total_rows AS total_rows_b
        # FROM (
        # select cast(_wstart as bigint) as ws, count(*) as c,
        #         (select count(*) from (select ts from ext_join_src) t) as total_rows
        # from ext_join_src
        # external_window((select jw.ts, jw.endtime
        #                 from ext_join_win jw, ext_join_src jd
        #                 where jw.ts=jd.ts) w)
        # order by ws
        # ) a
        # LEFT JOIN (
        # select cast(_wstart as bigint) as ws, count(*) as c,
        #         (select count(*) from (select ts from ext_join_src) t) as total_rows
        # from ext_join_src
        # external_window((select jw.ts, jw.endtime
        #                 from ext_join_win jw left join ext_join_src jd on jw.ts=jd.ts) w)
        # order by ws
        # ) b
        # ON a.ws = b.ws
        # ORDER BY a.ws;

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
