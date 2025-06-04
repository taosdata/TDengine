import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseCheck:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_check(self):
        """Stream check stable

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkStreamSTable.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkStreamSTable1.sim

        """

        self.checkStreamSTable()
        self.checkStreamSTable1()

    def checkStreamSTable(self):
        tdLog.info(f"checkStreamSTable")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdSql.execute(f"create database result vgroups 1;")
        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stable result.streamt0(ts timestamp,a int,b int) tags(ta int,tb varchar(100),tc int);"
        )
        tdSql.execute(
            f"create stream streams0 trigger at_once  into result.streamt0 tags(tb) as select  _wstart, count(*) c1, max(a) c2 from st partition by tbname tb interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791213000,2,2,3);")

        tdSql.query(
            f"select  _wstart, count(*) c1, max(a) c2 from st partition by tbname interval(10s);"
        )
        tdSql.printResult()

        tdSql.checkResultsByFunc(
            f"select * from result.streamt0 order by ta;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(0, 3) == None
            and tdSql.getData(0, 4) == "t1"
            and tdSql.getData(1, 1) == 1
            and tdSql.getData(1, 2) == 2
            and tdSql.getData(1, 3) == None
            and tdSql.getData(1, 4) == "t2",
        )

        tdLog.info(f"===== step3")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database result1 vgroups 1;")
        tdSql.execute(f"create database test1  vgroups 4;")
        tdSql.execute(f"use test1;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(
            f"create stable result1.streamt1(ts timestamp,a int,b int,c int) tags(ta varchar(100),tb int,tc int);"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once  into result1.streamt1(ts,c,a,b) tags(ta) as select  _wstart, count(*) c1, max(a),min(b) c2 from st partition by tbname as ta interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,10,20,30);")
        tdSql.execute(f"insert into t2 values(1648791213000,40,50,60);")

        tdSql.query(
            f"select  _wstart, count(*) c1, max(a),min(b) c2 from st partition by tbname interval(10s);"
        )
        tdSql.printResult()

        tdSql.checkResultsByFunc(
            f"select * from result1.streamt1 order by ta;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(0, 2) == 20
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(1, 1) == 40
            and tdSql.getData(1, 2) == 50
            and tdSql.getData(1, 3) == 1,
        )

        tdLog.info(f"===== step4")
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
            f"create stable result2.streamt2(ts timestamp, a int , b int) tags(ta varchar(20));"
        )

        # tag dest 1, source 2
        tdSql.error(
            f"create stream streams2 trigger at_once  into result2.streamt2 TAGS(aa varchar(100), ta int) as select  _wstart, count(*) c1, max(a) from st partition by tbname as aa, ta interval(10s);"
        )

        # column dest 3, source 4
        tdSql.error(
            f"create stream streams2 trigger at_once  into result2.streamt2 as select  _wstart, count(*) c1, max(a), max(b) from st partition by tbname interval(10s);"
        )

        # column dest 3, source 4
        tdSql.error(
            f"create stream streams2 trigger at_once  into result2.streamt2(ts, a, b) as select  _wstart, count(*) c1, max(a), max(b) from st partition by tbname interval(10s);"
        )

        # column dest 3, source 2
        tdSql.error(
            f"create stream streams2 trigger at_once  into result2.streamt2 as select  _wstart, count(*) c1 from st partition by tbname interval(10s);"
        )

        # column dest 3, source 2
        tdSql.execute(
            f"create stream streams2 trigger at_once  into result2.streamt2(ts, a) tags(ta) as select  _wstart, count(*) c1 from st partition by tbname as ta interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdLog.info(f"===== step5")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database result3 vgroups 1;")
        tdSql.execute(f"create database test3  vgroups 4;")
        tdSql.execute(f"use test3;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,2,3);")
        tdSql.execute(f"create table t2 using st tags(4,5,6);")

        tdSql.execute(
            f"create stable result3.streamt3(ts timestamp,a int,b int,c int, d int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(
            f"create stream streams3 trigger at_once  into result3.streamt3(ts,c,a,b) as select  _wstart, count(*) c1, max(a),min(b) c2 from st interval(10s);"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,10,20,30);")
        tdSql.execute(f"insert into t2 values(1648791213000,40,50,60);")

        tdSql.query(
            f"select  _wstart, count(*) c1, max(a),min(b) c2 from st interval(10s);"
        )
        tdSql.printResult()

        tdSql.checkResultsByFunc(
            f"select * from result3.streamt3;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == 40
            and tdSql.getData(0, 2) == 20
            and tdSql.getData(0, 3) == 2
            and tdSql.getData(0, 4) == None,
        )

        tdLog.info(f"===== drop ...")

        tdSql.execute(f"drop stream if exists streams0;")
        tdSql.execute(f"drop stream if exists streams1;")
        tdSql.execute(f"drop stream if exists streams2;")
        tdSql.execute(f"drop stream if exists streams3;")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"drop database if exists test3;")
        tdSql.execute(f"drop database if exists result;")
        tdSql.execute(f"drop database if exists result1;")
        tdSql.execute(f"drop database if exists result2;")
        tdSql.execute(f"drop database if exists result3;")

        tdLog.info(f"===== step6")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database result4 vgroups 1;")
        tdSql.execute(f"create database test4  vgroups 4;")
        tdSql.execute(f"use test4;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,2,3);")
        tdSql.execute(f"create table t2 using st tags(4,5,6);")

        tdSql.execute(
            f"create stable result4.streamt4(ts timestamp,a int,b int,c int, d int) tags(tg1 int,tg2 int,tg3 int);"
        )
        tdSql.execute(
            f'create stream streams4 trigger at_once  into result4.streamt4(ts,c,a,b) tags(tg2, tg3, tg1) subtable( concat("tbl-", cast(tg1 as varchar(10)) ) )  as select  _wstart, count(*) c1, max(a),min(b) c2 from st partition by ta+1 as tg1, cast(tb as bigint) as tg2, tc as tg3 interval(10s);'
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,10,20,30);")
        tdSql.execute(f"insert into t2 values(1648791213000,40,50,60);")

        tdSql.query(
            f"select  _wstart, count(*) c1, max(a),min(b) c2 from st partition by ta+1 as tg1, cast(tb as bigint) as tg2, tc as tg3 interval(10s);"
        )
        tdSql.printResult()

        tdSql.checkResultsByFunc(
            f"select * from result4.streamt4 order by tg1;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 1) == 10
            and tdSql.getData(0, 2) == 20
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(0, 4) == None
            and tdSql.getData(1, 1) == 40
            and tdSql.getData(1, 2) == 50
            and tdSql.getData(1, 3) == 1
            and tdSql.getData(1, 4) == None,
        )

        tdLog.info(f"===== step7")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database result5 vgroups 1;")
        tdSql.execute(f"create database test5  vgroups 4;")
        tdSql.execute(f"use test5;")

        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,2,3);")
        tdSql.execute(f"create table t2 using st tags(4,5,6);")

        tdSql.execute(
            f"create stable result5.streamt5(ts timestamp,a int,b int,c int, d int) tags(tg1 int,tg2 int,tg3 int);"
        )
        tdSql.execute(
            f'create stream streams5 trigger at_once  into result5.streamt5(ts,c,a,b) tags(tg2, tg3, tg1) subtable( concat("tbl-", cast(tg3 as varchar(10)) ) )  as select  _wstart, count(*) c1, max(a),min(b) c2 from st partition by ta+1 as tg1, cast(tb as bigint) as tg2, a as tg3 session(ts, 10s);'
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000,NULL,NULL,NULL);")

        tdSql.query(
            f"select  _wstart, count(*) c1, max(a),min(b) c2 from st partition by ta+1 as tg1, cast(tb as bigint) as tg2, a as tg3 session(ts, 10s);"
        )
        tdSql.printResult()

        tdSql.checkResultsByFunc(
            f"select * from result5.streamt5 order by tg1;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == None
            and tdSql.getData(0, 2) == None
            and tdSql.getData(0, 3) == 1
            and tdSql.getData(0, 4) == None
            and tdSql.getData(0, 5) == 2
            and tdSql.getData(0, 6) == 2
            and tdSql.getData(0, 7) == None,
        )

        tdSql.execute(f"drop stream if exists streams4;")
        tdSql.execute(f"drop stream if exists streams5;")
        tdSql.execute(f"drop database if exists test4;")
        tdSql.execute(f"drop database if exists test5;")
        tdSql.execute(f"drop database if exists result4;")
        tdSql.execute(f"drop database if exists result5;")

        tdLog.info(f"===== step8")
        tdStream.dropAllStreamsAndDbs()
        tdSql.execute(f"create database test8  vgroups 1;")
        tdSql.execute(f"use test8;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams8 trigger at_once  into streamt8 as select  _wstart as ts, count(*) c1, count(d) c2, count(c) c3  from t1 partition by tbname interval(10s) ;"
        )

        tdSql.execute(f"drop stream streams8;")
        tdSql.execute(
            f"create stream streams71 trigger at_once  into streamt8(ts, c2) tags(group_id)as select _wstart, count(*) from t1 partition by tbname as group_id interval(10s);"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791233000,1,2,3,1.0);")

        tdSql.checkResultsByFunc(
            f"select * from streamt8;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 1) == None
            and tdSql.getData(0, 2) == 1
            and tdSql.getData(0, 3) == None,
        )

    def checkStreamSTable1(self):
        tdLog.info(f"checkStreamSTable1")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdSql.execute(f"create database test  vgroups 4;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(
            f"create stream streams1 trigger at_once  into streamt1 as select  _wstart, count(*) c1, count(a) c2  from st interval(1s) ;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791212000,2,2,3);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 2,
        )

        tdLog.info(f"drop stream streams1")
        tdSql.execute(f"drop stream streams1;")

        tdLog.info(f"alter table streamt1 add column c3 double")
        tdSql.execute(f"alter table streamt1 add column c3 double;")

        tdLog.info(
            f"create stream streams1 trigger at_once  into streamt1 as select  _wstart, count(*) c1, count(a) c2, avg(b) c3  from st interval(1s) ;"
        )
        tdSql.execute(
            f"create stream streams1 trigger at_once  into streamt1 as select  _wstart, count(*) c1, count(a) c2, avg(b) c3  from st interval(1s) ;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t2 values(1648791213000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791214000,1,2,3);")

        tdSql.checkResultsByFunc(
            f"select * from streamt1;",
            lambda: tdSql.getRows() == 4,
        )
