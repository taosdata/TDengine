from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestExternal:

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
        vgroups = 4
        self.dbName = "d0"

        tdLog.info(f"====> create database {self.dbName} vgroups {vgroups}")
        tdSql.execute(f"drop database if exists {self.dbName}")
        tdSql.execute(f"create database {self.dbName} vgroups {vgroups}")
        self.prepareData()

        self.basic_query()
        self.partition_by_group_regression()

    def prepareData(self):
        tdSql.execute(f"use {self.dbName}")

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
        # select _wstart, _wend, w.fc1, ts from st1_1 external_window((select first(c1) fc1  from st2) w);
        
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
        return
    
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