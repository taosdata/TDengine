from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestExternal:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

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

        vgroups = 4
        self.dbName = "d0"

        tdLog.info(f"====> create database {self.dbName} vgroups {vgroups}")
        tdSql.execute(f"drop database if exists {self.dbName}")
        tdSql.execute(f"create database {self.dbName} vgroups {vgroups}")
        self.prepareData()

        self.basic_query()

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
        
        select _wstart, _wend, w.fc1, count(*) from st1_1 external_window((select first(c1) fc1  from st2) w); 
        
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
        return
    
        # explain verbose true select count(*) from st1_1 external_window((select ts, ts+10, first(c1) c1 from st2) w) \G;