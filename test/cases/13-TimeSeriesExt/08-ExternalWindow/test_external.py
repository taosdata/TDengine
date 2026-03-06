from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


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

        self.mock_test_external_window_single_block()
        self.mock_test_external_window_group_blocks()
        # self.basic_query()
        # partition by + external window regression is tracked separately.
        # keep basic_query as focused validation entry for external placeholder assignment.
        # self.partition_by_group_regression()

    def mock_test_external_window_single_block(self):
        dbName = "external_window_test_single_block"
        self.prepareData(dbName)
        tdSql.execute(f"use {dbName}")
        tdLog.info(f"=============== basic query of external window with agg")
        
        sql = "select _wstart, _wend, w.fc1, count(*) from st1_1 external_window((select first(c1) fc1  from st2) w);"
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
        
        sql = "select _wstart, _wend, w.fc1, ts from st1_1 external_window((select first(c1) fc1  from st2) w);"
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
        
        sql = "select _wstart, _wend, w.fc1+1, ts from st1_1 external_window((select first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        tdSql.checkRows(82)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 101)
        tdSql.checkData(50, 2, 201)
        
        sql = "select _wstart, _wend, w.fc1, count(*) from st1_1 partition by dev  external_window((select first(c1) fc1  from st2) w);"
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
        
        sql = "select _wstart, _wend, w.fc1, count(*), dev from st1_1 partition by dev  external_window((select first(c1) fc1  from st2) w);"
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
        
        sql = "select _wstart, _wend, w.fc1, count(*), v2 from st1_1 partition by v2  external_window((select first(c1) fc1  from st2) w);"
        tdSql.query(sql)
        # 2 windows * 82 groups. timerange has been pushed down, so groups outside the window range are excluded.
        tdSql.checkRows(164)
        tdSql.checkData(0, 0, "2020-05-13 10:00:00.000")
        tdSql.checkData(0, 1, "2020-05-13 10:49:00.000")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, 1)

        sql = "select _wstart, _wend, w.fc1, count(*), v2 from st1_1 partition by v2  external_window((select first(c1) fc1  from st2) w) order by v2 desc;"
        tdSql.query(sql)
        # 2 windows * 82 groups. timerange has been pushed down, so groups outside the window range are excluded.
        tdSql.checkRows(164)
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
            "from st1_1 external_window((select first(c1) fc1 from st2) w);"
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
            "external_window((select first(c1) fc1 from st2) w) "
            "order by v2 desc;"
        )
        tdSql.query(sql)
        tdSql.checkRows(164)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(81, 3, 1)
        tdSql.checkData(82, 3, 0)
        tdSql.checkData(163, 3, 0)
        tdSql.checkData(0, 5, 100)
        tdSql.checkData(81, 5, 100)
        tdSql.checkData(82, 5, 100)
        tdSql.checkData(163, 5, 100)
    
    def mock_test_external_window_group_blocks(self):
        dbName = "external_window_test_group_blocks"
        self.prepareData(dbName)
        tdSql.execute(f"use {dbName}")
        tdLog.info(f"=============== basic query of external window with agg on group blocks")
        
        sql = "select _wstart, _wend, w.fc1, count(*), dev from st1_1 partition by dev  external_window((select first(c1) fc1  from st2) w);"
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
        
        # select _wstart, _wend, w.fc1, count(*), v2 from st1_1 partition by dev  external_window((select first(c1) fc1  from st2) w);
        
        # select _wstart, _wend, w.fc1, count(*), v2 from st1_1 partition by v2  external_window((select first(c1) fc1  from st2) w);
        
        # sql = "select _wstart, _wend, w.fc1, count(*) from st1 partition by dev external_window((select first(c1) fc1  from st2) w);"
        # tdSql.query(sql)
        # tdSql.checkRows(8)
        # for i in range(8):
        #     tdSql.checkData(i, 0, "2020-05-13 10:00:00.000")
        #     tdSql.checkData(i, 1, "2020-05-13 11:21:50.000")
        #     tdSql.checkData(i, 2, 100)
        #     tdSql.checkData(i, 3, 100)
    
    def prepareData(self, dbName):
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
        
    def basic_query(self):
        tdLog.info(f"=============== basic query of external window")
        sql1 = "select _wstart, _wend, w.fc1, ts from st1_1 external_window((select first(c1) fc1  from st2) w);"
        tdSql.query(sql1)

        rows = tdSql.getRows()
        if rows <= 0:
            tdLog.exit(f"external window query got no rows: {sql1}")

        for i in range(min(rows, 5)):
            ws = tdSql.getData(i, 0)
            we = tdSql.getData(i, 1)
            if ws is None or self._is_zero_ts(ws):
                tdLog.exit(f"_wstart is invalid at row {i}, value: {ws}, sql: {sql1}")
            if we is None or self._is_zero_ts(we):
                tdLog.exit(f"_wend is invalid at row {i}, value: {we}, sql: {sql1}")
            fc1 = tdSql.getData(i, 2)
            if self._is_invalid_fc1(fc1):
                tdLog.exit(f"w.fc1 is invalid at row {i}, value: {fc1}, sql: {sql1}")

        sql2 = "select _wstart, _wend, w.fc1, count(*) from st1_1 external_window((select first(c1) fc1  from st2) w);"
        tdSql.query(sql2)

        rows = tdSql.getRows()
        if rows <= 0:
            tdLog.exit(f"external window agg query got no rows: {sql2}")

        for i in range(rows):
            ws = tdSql.getData(i, 0)
            we = tdSql.getData(i, 1)
            if ws is None or self._is_zero_ts(ws):
                tdLog.exit(f"_wstart is invalid at row {i}, value: {ws}, sql: {sql2}")
            if we is None or self._is_zero_ts(we):
                tdLog.exit(f"_wend is invalid at row {i}, value: {we}, sql: {sql2}")
            fc1 = tdSql.getData(i, 2)
            if self._is_invalid_fc1(fc1):
                tdLog.exit(f"w.fc1 is invalid at row {i}, value: {fc1}, sql: {sql2}")
            if tdSql.getData(i, 3) is None:
                tdLog.exit(f"count(*) is None at row {i}, sql: {sql2}")
        
        # todo xs fix external window column placeholder issue
        # select _wstart, _wend, w.fc1 + 1, ts from st1_1 external_window((select first(c1) fc1  from st2) w);
        
        # select _wstart, _wend, w.fc1, count(*) from st1_1 external_window((select first(c1) fc1  from st2) w); 
        # select _wstart, _wend, w.fc1, count(*) from st1_1 external_window((select ts, ts+1, first(c1) fc1 from st2) w);
        # select _wstart, _wend, w.fc1, ts from st1_1 external_window((select ts, ts+1, first(c1) fc1 from st2) w);
        # select _wstart, _wend, _wduration, ts from st1_1 external_window((select ts, ts+1, first(c1) c1 from st2) w);
        # select _wstart, _wend, ts from st1_1 external_window((select ts, ts+1, first(c1) c1 from st2) w);
        # select _wstart, _wend, ts, cast(ts as bigint)- cast(_wstart as bigint) from st1_1 external_window((select ts, ts+1, first(c1) c1 from st2) w);
        # tdSql.execute("select count(*) from st1_1 external_window((select ts, ts+10, first(c1) c1 from st2) w);")
        # tdSql.execute("select _wstart, count(*) from st1_1 external_window((select ts, ts+10, first(c1) c1 from st2) w);")
        # select _wstart, * from st1_1 external_window((select ts, ts+10, first(c1) c1 from st2) w);
        # select _wstart, count(*) from st1 external_window((select ts, ts+10, first(c1) c1 from st2) w);
        # select _wstart, w.c1, count(*) from st1 external_window((select ts, ts+10, first(c1) c1 from st2) w);
        # select _wstart, count(*) from st1 external_window((select ts, ts+10, first(c1) from st2) w);
        # select _wstart, count(*) from st1 external_window((select ts, ts+10 from st2) w);
        # select _wstart, count(*) from st1 external_window((select ts, ts+10 from st2 interval(2m)) w);
        # select _wstart, count(*) from st1 external_window((select _wstart, _wend from st2 interval(2m)) w);
        
        # todo xsren: 从超级表查询
        
        # 投影查询 + patition by , 自带 ts
        # select _wstart, _wend, w.fc1 as fc1, v2 from st1_1 partition by v2 external_window((select first(c1) fc1  from st2) w);
        # todo 投影查询 + patition by , 不带 ts，有问题，需要修复，通过给 partition 算子增加 ts 列解决
        # select _wstart, _wend, w.fc1 as fc1, v2, ts from st1_1 partition by v2 external_window((select first(c1) fc1  from st2) w);
        # explain verbose true select count(*) from st1_1 external_window((select ts, ts+10, first(c1) c1 from st2) w) \G;

    def partition_by_group_regression(self):
        tdLog.info("=============== regression: partition by + external window group calculation")

        tdSql.execute(f"use {self.dbName}")

        tdSql.execute("drop table if exists ext_src")
        tdSql.execute("drop table if exists ext_win")

        tdSql.execute("create table ext_src (ts timestamp, v int) tags(g int)")
        tdSql.execute("create table ext_win (ts timestamp, v int) tags(g int)")

        tdSql.execute("create table ext_src_1 using ext_src tags(1)")
        tdSql.execute("create table ext_src_2 using ext_src tags(2)")
        tdSql.execute("create table ext_win_1 using ext_win tags(1)")

        t0 = 1700000000000

        tdSql.execute(f"insert into ext_win_1 values({t0}, 1)({t0 + 600000}, 1)")

        tdSql.execute(f"insert into ext_src_1 values({t0 + 60000}, 10)({t0 + 120000}, 11)")
        tdSql.execute(f"insert into ext_src_2 values({t0 + 660000}, 20)")

        sql = (
            "select tbname, cast(_wstart as bigint) as ws, count(*) as c "
            "from ext_src partition by tbname "
            "external_window((select _wstart, _wend, count(*) as wc from ext_win interval(10m)) w) "
            "order by tbname, ws"
        )

        tdSql.query(sql)
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "ext_src_1")
        tdSql.checkData(0, 1, t0)
        tdSql.checkData(0, 2, 2)

        tdSql.checkData(1, 0, "ext_src_1")
        tdSql.checkData(1, 1, t0 + 600000)
        tdSql.checkData(1, 2, 0)

        tdSql.checkData(2, 0, "ext_src_2")
        tdSql.checkData(2, 1, t0)
        tdSql.checkData(2, 2, 0)

        tdSql.checkData(3, 0, "ext_src_2")
        tdSql.checkData(3, 1, t0 + 600000)
        tdSql.checkData(3, 2, 1)