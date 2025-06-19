import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseConcat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_concat(self):
        """Stream concat

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndCol0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag2.sim

        """

        self.udTableAndCol0()
        self.udTableAndTag0()
        self.udTableAndTag1()
        self.udTableAndTag2()

    def udTableAndCol0(self):
        tdLog.info(f"udTableAndCol0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdLog.info(f"===== table name")

        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.error(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt1(a, b, c, d) as select  _wstart, count(*) c1, max(a) from st interval(10s);"
        )
        tdSql.error(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt2(a, b) as select  _wstart, count(*) c1, max(a) from st interval(10s);"
        )

        tdSql.execute(
            f"create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt3(a, b) as select  count(*) c1, max(a) from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.query(f"desc streamt3;")
        tdSql.checkRows(4)

        tdSql.execute(
            f"create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt4(a, b, c) as select  _wstart, count(*) c1, max(a) from st interval(10s);"
        )
        tdSql.execute(
            f"create stream streams5 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt5(a, b, c) as select  _wstart, count(*) c1, max(a) from st partition by tbname interval(10s);"
        )
        tdSql.execute(
            f"create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt6(a, b, c) tags(tbn varchar(60)) as select  _wstart, count(*) c1, max(a) from st partition by tbname tbn interval(10s);"
        )

        tdSql.execute(
            f"create stream streams7 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt7(a, b primary key, c) tags(tbn varchar(60)) as select  _wstart, count(*) c1, max(a) from st partition by tbname tbn interval(10s);"
        )
        tdSql.error(
            f"create stream streams8 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt8(a, b, c primary key) as select  _wstart, count(*) c1, max(a) from st interval(10s);"
        )

        tdSql.execute(
            f"create stream streams9 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt9(a primary key, b) as select  count(*) c1, max(a) from st interval(10s);"
        )

        tdSql.error(
            f"create stream streams10 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt10(a, b primary key, c) as select  count(*) c1, max(a), max(b) from st interval(10s);"
        )
        tdSql.error(
            f"create stream streams11 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt11(a, b , a) as select  count(*) c1, max(a), max(b) from st interval(10s);"
        )
        tdSql.error(
            f"create stream streams12 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt12(a, b , c) tags(c varchar(60)) as select  count(*) c1, max(a), max(b) from st partition by tbname c interval(10s);"
        )

        tdSql.error(
            f"create stream streams13 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt13(a, b , c) tags(tc varchar(60)) as select  count(*) c1, max(a) c1, max(b) from st partition by tbname tc interval(10s);"
        )

        tdSql.error(
            f"create stream streams14 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt14 tags(tc varchar(60)) as select  count(*) tc, max(a) c1, max(b) from st partition by tbname tc interval(10s);"
        )

        tdSql.error(
            f"create stream streams15 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt15 tags(tc varchar(100) primary key) as select _wstart, count(*) c1, max(a) from st partition by tbname tc interval(10s);"
        )

        tdSql.execute(f"create database test1  vgroups 4;")
        tdSql.execute(f"use test1;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int primary key, b int, c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.error(
            f"create stream streams16 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt16 as select _wstart, count(*) c1, max(a) from st partition by tbname tc state_window(b);"
        )
        tdSql.error(
            f"create stream streams17 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt17 as select _wstart, count(*) c1, max(a) from st partition by tbname tc event_window start with a = 0 end with a = 9;"
        )
        tdSql.error(
            f"create stream streams18 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 watermark 10s into streamt18 as select _wstart, count(*) c1, max(a) from st partition by tbname tc count_window(2);"
        )

        tdLog.info(f"===== step2")
        tdLog.info(f"===== scalar")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1 (ts timestamp, a int, b int);")

        tdSql.execute(
            f"create table rst(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
        )
        tdSql.execute(f'create table rct1 using rst tags("aa");')

        tdSql.execute(
            f"create table rst6(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
        )
        tdSql.execute(
            f"create table rst7(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
        )

        tdSql.execute(
            f"create stream streams19 trigger at_once ignore expired 0 ignore update 0  into streamt19 as select  ts,a, b from t1;"
        )

        tdSql.execute(
            f"create stream streams20 trigger at_once ignore expired 0 ignore update 0  into streamt20(ts, a primary key, b)  as select  ts,a, b from t1;"
        )
        tdSql.execute(
            f"create stream streams21 trigger at_once ignore expired 0 ignore update 0  into rst  as select  ts,a, b from t1;"
        )

        tdSql.error(
            f"create stream streams22 trigger at_once ignore expired 0 ignore update 0  into streamt22 as select  ts,1, b from rct1;"
        )

        tdSql.execute(
            f"create stream streams23 trigger at_once ignore expired 0 ignore update 0  into streamt23 as select  ts, a, b from rct1;"
        )

        tdSql.execute(
            f"create stream streams24 trigger at_once ignore expired 0 ignore update 0  into streamt24(ts, a primary key, b) as select  ts, a, b from rct1;"
        )
        tdSql.execute(
            f"create stream streams25 trigger at_once ignore expired 0 ignore update 0  into rst6 as select  ts, a, b from rct1;"
        )

        tdSql.error(
            f"create stream streams26 trigger at_once ignore expired 0 ignore update 0  into rst7 as select  ts, 1,b from rct1;"
        )

        tdSql.error(
            f"create stream streams27 trigger at_once ignore expired 0 ignore update 0  into streamt27(ts, a primary key, b) as select  ts, 1,b from rct1;"
        )

        tdLog.info(f"======over")

    def udTableAndTag0(self):
        tdLog.info(f"udTableAndTag0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdLog.info(f"===== table name")

        tdSql.execute(f"create database result vgroups 1;")
        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result.streamt SUBTABLE(concat("aaa-", tbname)) as select  _wstart, count(*) c1 from st partition by tbname interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,1,2,3);")

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result" order by 1;',
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from result.streamt;", lambda: tdSql.getRows() == 2
        )

        tdLog.info(f"===== step3")
        tdLog.info(f"===== tag name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result2 vgroups 1;")
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result2.streamt2 TAGS(cc varchar(100)) as select  _wstart, count(*) c1 from st partition by concat("tag-", tbname) as cc interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,1,2,3);")

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result2" and stable_name = "streamt2" order by 1;',
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "cc"
            and tdSql.getData(1, 0) == "cc",
        )

        tdSql.checkResultsByFunc(
            f"select cc from result2.streamt2 order by 1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "tag-t1"
            and tdSql.getData(1, 0) == "tag-t2",
        )

        tdSql.checkResultsByFunc(
            f"select * from result2.streamt2;",
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(f"===== step4")
        tdLog.info(f"===== tag name + table name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result3 vgroups 1;")
        tdSql.execute(f"create database test3  vgroups 4;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result3.streamt3 TAGS(dd varchar(100)) SUBTABLE(concat("tbn-", tbname)) as select  _wstart, count(*) c1 from st partition by concat("tag-", tbname) as dd, tbname interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,1,2,3);")

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result3" and stable_name = "streamt3" order by 1;',
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "dd"
            and tdSql.getData(1, 0) == "dd",
        )

        tdSql.checkResultsByFunc(
            f"select dd from result3.streamt3 order by 1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "tag-t1"
            and tdSql.getData(1, 0) == "tag-t2",
        )

        tdSql.checkResultsByFunc(
            f"select * from result3.streamt3;",
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result3" order by 1;',
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(f"===== step5")
        tdLog.info(f"===== tag name + table name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result4 vgroups 1;")
        tdSql.execute(f"create database test4  vgroups 4;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")

        tdSql.execute(
            f'create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result4.streamt4 TAGS(dd varchar(100)) SUBTABLE(concat("tbn-", tbname)) as select  _wstart, count(*) c1 from st partition by concat("tag-", tbname) as dd, tbname interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000,1,1,1) t2 values(1648791213000,2,2,2) t3 values(1648791213000,3,3,3);"
        )

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result4" order by 1;',
            lambda: tdSql.getRows() == 3,
        )

        tdSql.checkResultsByFunc(
            f"select * from result4.streamt4 order by 3;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == "tag-t1"
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == "tag-t2"
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == "tag-t3",
        )

        tdLog.info(f"===== step6")
        tdLog.info(f"===== transform tag value")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"drop database if exists test5;")

        tdSql.execute(f"drop database if exists result1;")
        tdSql.execute(f"drop database if exists result2;")
        tdSql.execute(f"drop database if exists result3;")
        tdSql.execute(f"drop database if exists result4;")
        tdSql.execute(f"drop database if exists result5;")

        tdSql.execute(f"create database result6 vgroups 1;")

        tdSql.execute(f"create database test6  vgroups 4;")
        tdSql.execute(f"use test6;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta varchar(20), tb int, tc int);"
        )
        tdSql.execute(f'create table t1 using st tags("1",1,1);')
        tdSql.execute(f'create table t2 using st tags("2",2,2);')
        tdSql.execute(f'create table t3 using st tags("3",3,3);')

        tdSql.execute(
            f'create stream streams6 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result6.streamt6 TAGS(dd int) as select  _wstart, count(*) c1 from st partition by concat(ta, "0") as dd, tbname interval(10s);'
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000,1,1,1) t2 values(1648791213000,2,2,2) t3 values(1648791213000,3,3,3);"
        )

        tdSql.checkResultsByFunc(
            f"select * from result6.streamt6 order by 3;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 2) == 10
            and tdSql.getData(1, 2) == 20
            and tdSql.getData(2, 2) == 30,
        )

    def udTableAndTag1(self):
        tdLog.info(f"udTableAndTag1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdLog.info(f"===== table name")

        tdSql.execute(f"create database result vgroups 1;")
        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result.streamt SUBTABLE( concat("aaa-", cast(a as varchar(10) ) )  ) as select  _wstart, count(*) c1 from st partition by a interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdLog.info(f"===== insert into 1")
        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result" order by 1;',
            lambda: tdSql.getRows() == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from result.streamt;", lambda: tdSql.getRows() == 2
        )

        tdLog.info(f"===== step3")
        tdLog.info(f"===== column name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result2 vgroups 1;")
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result2.streamt2 TAGS(cc varchar(100)) as select  _wstart, count(*) c1 from st partition by concat("col-", cast(a as varchar(10) ) ) as cc interval(10s);'
        )
        tdStream.checkStreamStatus()

        tdLog.info(f"===== insert into 2")
        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result2" and stable_name = "streamt2" order by 1;',
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "cc"
            and tdSql.getData(1, 0) == "cc",
        )

        tdSql.checkResultsByFunc(
            f"select cc from result2.streamt2 order by 1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "col-1"
            and tdSql.getData(1, 0) == "col-2",
        )

        tdSql.checkResultsByFunc(
            f"select * from result2.streamt2;", lambda: tdSql.getRows() == 2
        )

        tdLog.info(f"===== step4")
        tdLog.info(f"===== column name + table name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result3 vgroups 1;")
        tdSql.execute(f"create database test3  vgroups 4;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result3.streamt3 TAGS(dd varchar(100)) SUBTABLE(concat("tbn-",  cast(a as varchar(10) ) ) ) as select  _wstart, count(*) c1 from st partition by concat("col-", cast(a as varchar(10) ) ) as dd, a interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdLog.info(f"===== insert into 3")
        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result3" and stable_name = "streamt3" order by 1;',
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "dd"
            and tdSql.getData(1, 0) == "dd",
        )

        tdSql.checkResultsByFunc(
            f"select dd from result3.streamt3 order by 1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == "col-1"
            and tdSql.getData(1, 0) == "col-2",
        )

        tdSql.checkResultsByFunc(
            f"select * from result3.streamt3;", lambda: tdSql.getRows() == 2
        )

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result3" order by 1;',
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(f"===== step5")
        tdLog.info(f"===== tag name + table name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result4 vgroups 1;")
        tdSql.execute(f"create database test4  vgroups 4;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")

        tdSql.execute(
            f'create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result4.streamt4 TAGS(dd varchar(100)) SUBTABLE(concat("tbn-", dd)) as select  _wstart, count(*) c1 from st partition by concat("t", cast(a as varchar(10) ) ) as dd interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000,1,1,1) t2 values(1648791213000,2,2,2) t3 values(1648791213000,3,3,3);"
        )

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result4" order by 1;',
            lambda: tdSql.getRows() == 3,
        )

        tdSql.checkResultsByFunc(
            f"select * from result4.streamt4 order by 3;",
            lambda: tdSql.getRows() == 3
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == "t1"
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == "t2"
            and tdSql.getData(2, 1) == 1
            and tdSql.getData(2, 2) == "t3",
        )

        tdLog.info(f"===== step6")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test5  vgroups 4;")
        tdSql.execute(f"use test5;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stable streamt5(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stream streams5 trigger at_once ignore expired 0 ignore update 0 into streamt5(ts,b,a) as select  _wstart, count(*), 1000 c1 from t1 interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791233002,3,2,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791243003,4,2,3,3.1);")

        tdSql.checkResultsByFunc(
            f"select * from streamt5;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 1000
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(3, 1) == 1000
            and tdSql.getData(3, 2) == 1,
        )

    def udTableAndTag2(self):
        tdLog.info(f"udTableAndTag2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdLog.info(f"===== table name")

        tdSql.execute(f"create database result vgroups 1;")
        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result.streamt SUBTABLE("aaa") as select  _wstart, count(*) c1 from st interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdLog.info(f"===== insert into 1")
        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "aaa",
        )

        tdSql.checkResultsByFunc(
            f"select * from result.streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"===== step3")
        tdLog.info(f"===== column name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result2 vgroups 1;")
        tdSql.execute(f"create database test2  vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result2.streamt2 TAGS(cc varchar(100)) as select  _wstart, count(*) c1 from st interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdLog.info(f"===== insert into 2")
        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result2" and stable_name = "streamt2" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "cc",
        )

        tdSql.checkResultsByFunc(
            f"select cc from result2.streamt2 order by 1;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == None,
        )

        tdSql.checkResultsByFunc(
            f"select * from result2.streamt2;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdLog.info(f"===== step4")
        tdLog.info(f"===== column name + table name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result3 vgroups 1;")
        tdSql.execute(f"create database test3  vgroups 4;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams3 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result3.streamt3 TAGS(dd varchar(100)) SUBTABLE(concat("tbn-",  "1") ) as select  _wstart, count(*) c1 from st interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdLog.info(f"===== insert into 3")
        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result3" and stable_name = "streamt3" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "dd",
        )

        tdSql.checkResultsByFunc(
            f"select dd from result3.streamt3 order by 1;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == None,
        )

        tdSql.checkResultsByFunc(
            f"select * from result3.streamt3;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result3" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "tbn-1",
        )

        tdLog.info(f"===== step5")
        tdLog.info(f"===== tag name + table name")
        tdStream.checkStreamStatus()

        tdSql.execute(f"create database result4 vgroups 1;")
        tdSql.execute(f"create database test4  vgroups 1;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")

        tdSql.execute(
            f'create stream streams4 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result4.streamt4 TAGS(dd varchar(100)) SUBTABLE(concat("tbn-", "1")) as select  _wstart, count(*) c1 from st interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(
            f"insert into t1 values(1648791213000,1,1,1) t2 values(1648791213000,2,2,2) t3 values(1648791213000,3,3,3);"
        )

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result4" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "tbn-1",
        )

        tdSql.checkResultsByFunc(
            f"select * from result4.streamt4 order by 3;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
        )

        tdLog.info(f"===== step6")
        tdLog.info(f"===== table name")
        tdStream.dropAllStreamsAndDbs()

        tdSql.execute(f"create database result5 vgroups 1;")
        tdSql.execute(f"create database test5  vgroups 1;")
        tdSql.execute(f"use test5;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f'create stream streams51 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result5.streamt51 SUBTABLE("aaa") as select  _wstart, count(*) c1 from st interval(10s);'
        )
        tdSql.execute(
            f"create stream streams52 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result5.streamt52 TAGS(cc varchar(100)) as select  _wstart, count(*) c1 from st interval(10s);"
        )
        tdSql.execute(
            f'create stream streams53 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into result5.streamt53 TAGS(dd varchar(100)) SUBTABLE(concat("aaa-", "1") ) as select  _wstart, count(*) c1 from st interval(10s);'
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.checkResultsByFunc(
            f'select table_name from information_schema.ins_tables where db_name="result5" order by 1;',
            lambda: tdSql.getRows() == 3 and tdSql.getData(0, 0) == "aaa",
        )

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result5" and stable_name = "streamt52" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "cc",
        )

        tdSql.checkResultsByFunc(
            f'select tag_name from information_schema.ins_tags where db_name="result5" and stable_name = "streamt53" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "dd",
        )

        tdSql.checkResultsByFunc(
            f"select * from result5.streamt51;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from result5.streamt52;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )

        tdSql.checkResultsByFunc(
            f"select * from result5.streamt53;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )
