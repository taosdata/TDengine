import os

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdCom


class TestExternal:

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
