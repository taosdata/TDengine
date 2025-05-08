from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDelete:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_delete(self):
        """delete

        1. -

        Catalog:
            - DataIngestion:Delete

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/regressiontest.sim

        """

        dbPrefix = "reg_db"
        tb = "tb"
        rowNum = 8200

        ts0 = 1537146000000
        delta = 100
        tdLog.info(f"========== reg.sim")
        i = 0
        db = dbPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 1 cachemodel 'last_row'")

        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, c1 int)")

        i = 0
        ts = ts0

        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
            x = x + 1

        tdSql.execute(f"flush database {db}")

        tdSql.execute(f"delete from {tb} where ts=1537146000000")
        tdSql.execute(f"delete from {tb} where ts=1537146409500")

        tdLog.info(f"=========================> TS-2410")
        tdSql.query(f"select * from  {tb} limit 20 offset 4090")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdLog.info(f"{tdSql.getData(1,0)}")
        tdLog.info(f"{tdSql.getData(2,0)}")
        tdLog.info(f"{tdSql.getData(3,0)}")
        tdLog.info(f"{tdSql.getData(4,0)}")
        tdLog.info(f"{tdSql.getData(5,0)}")
        tdLog.info(f"{tdSql.getData(6,0)}")
        tdLog.info(f"{tdSql.getData(7,0)}")
        tdLog.info(f"{tdSql.getData(8,0)}")
        tdLog.info(f"{tdSql.getData(9,0)}")

        tdSql.checkData(4, 0, "2018-09-17 09:06:49.600")

        tdSql.query(f"select * from {tb} order by ts desc;")
        tdSql.checkRows(8198)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1;")

        tdSql.execute(f"use {db}")
        tdSql.execute(f"create stable st1 (ts timestamp, c  int) tags(a  int);")
        tdSql.execute(f"create table t1 using st1 tags(1);")
        tdSql.execute(f"create table t2 using st1 tags(2);")

        i = 0
        ts = 1674977959000
        rowNum = 200

        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into t1 values ( {ts} , {x} )")
            tdSql.execute(f"insert into t2 values ( {ts} + 1000a, {x} )")
            x = x + 1
            ts = ts + 1000

        tdSql.execute(f"flush database {db}")

        tdLog.info(f"===========================>  TD-22077  && TD-21877")

        tdSql.execute(f"insert into t1 values('2018-09-17 09:00:26', 26);")
        tdSql.execute(f"insert into t2 values('2018-09-17 09:00:25', 25);")

        tdSql.execute(f"insert into t2 values('2018-09-17 09:00:30', 30);")
        tdSql.execute(f"flush  database reg_db0;")

        tdSql.execute(f"delete from st1 where ts<='2018-9-17 09:00:26';")
        tdSql.query(f"select * from st1;")

        tdSql.execute(f"drop table t1")
        tdSql.execute(f"drop table t2")

        tdLog.info(f"=========================================>TD-22196")
        tdSql.execute(f"create table t1 using st1 tags(1);")

        i = 0
        ts = 1674977959000
        rowNum = 200

        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into t1 values ( {ts} , {x} )")
            x = x + 1
            ts = ts + 1000

        tdSql.execute(f"flush database {db}")
        tdSql.query(f"select min(c),max(c) from t1")
        tdSql.checkData(0, 0, 0)

        tdSql.checkData(0, 1, 199)

        tdSql.execute(f"drop table t1")

        rowNum = 8200
        ts0 = 1537146000000
        tdSql.execute(f"create table t1 (ts timestamp, c1 int)")

        i = 0
        ts = ts0

        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into t1 values ( {ts} , {x} )")
            x = x + 1

        tdSql.execute(f"delete from t1 where ts<=1537146409500")

        tdSql.execute(f"flush database {db}")

        tdLog.info(f"======================================>TS-2639")
        tdSql.query(f"show table distributed t1;")

        tdLog.info(f"=====================================>TD-22007")
        tdSql.query(f"select count(*) from t1 interval(10a)")
        tdSql.execute(f"drop table t1")

        tdSql.execute(f"drop table st1")
        tdSql.execute(f"create table st1 (ts timestamp, k int) tags(a int);")
        tdSql.execute(
            f"insert into t1 using st1 tags(1) values('2020-1-1 10:10:10', 0);"
        )
        tdSql.execute(
            f"insert into t2 using st1 tags(1) values('2020-1-1 10:10:11', 1);"
        )
        tdSql.execute(
            f"insert into t3 using st1 tags(1) values('2020-1-1 10:10:12', 2);"
        )
        tdSql.execute(
            f"insert into t4 using st1 tags(1) values('2020-1-1 10:10:13', 3);"
        )
        tdSql.execute(
            f"insert into t5 using st1 tags(1) values('2020-1-1 10:10:14', 4);"
        )
        tdSql.execute(
            f"insert into t6 using st1 tags(2) values('2020-1-1 10:10:15', 5);"
        )
        tdSql.execute(
            f"insert into t7 using st1 tags(2) values('2020-1-1 10:10:16', 6);"
        )
        tdSql.execute(
            f"insert into t8 using st1 tags(2) values('2020-1-1 10:10:17', 7);"
        )
        tdSql.execute(
            f"insert into t9 using st1 tags(2) values('2020-1-1 10:10:18', 8);"
        )
        tdSql.execute(
            f"insert into t10 using st1 tags(2) values('2020-1-1 10:10:19', 9);"
        )

        tdSql.query(f"select count(*) from st1")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select last_row(*) from st1 group by a order by a desc")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2020-01-01 10:10:19.000")

        tdSql.checkData(0, 1, 9)

        tdSql.checkData(1, 0, "2020-01-01 10:10:14.000")

        tdSql.checkData(1, 1, 4)

        tdLog.info(f"===============================================> TS-2613")
        tdSql.query(f"select * from information_schema.ins_databases limit 1 offset 1;")
        tdSql.checkRows(1)
