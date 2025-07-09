from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertColumns:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_columns(self):
        """insert sub table with columns

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-29 Simon Guan Migrated from tsim/insert/insert_stb.sim

        """

        tdSql.execute(f"create database d1")
        tdSql.execute(f"create database d2")

        tdSql.execute(f"use d1;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")
        tdSql.execute(f"insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")
        tdSql.execute(f"insert into ct1 values('2021-04-19 00:00:02', 2);")
        tdSql.execute(f"create table st2(ts timestamp, f int) tags(t int);")

        tdSql.execute(f"use d2;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")
        tdSql.execute(f"insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")

        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"create table db1.stb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 int);")

        tdSql.execute(f"use d1;")
        tdSql.execute(f"insert into st (tbname, ts, f, t) values('ct3', '2021-04-19 08:00:03', 3, 3);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:04', 6);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:05', 7)('ct8', '2021-04-19 08:00:06', 8);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:07', 9, 9)('ct8', '2021-04-19 08:00:08', 10, 10);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:09', 9, 9)('ct8', '2021-04-19 08:00:10', 10, 10) d2.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:11', 9, 9)('ct8', '2021-04-19 08:00:12', 10, 10);")

        tdSql.query(f"select * from d1.st")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(11)

        tdSql.query(f"select * from d2.st;")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(4)

        tdSql.execute(f"insert into d2.st(ts, f, tbname) values('2021-04-19 08:00:13', 1, 'ct1') d1.ct1 values('2021-04-19 08:00:14', 1);")

        tdSql.query(f"select * from d1.st")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(12)

        tdSql.query(f"select * from d2.st;")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(5)

        tdSql.execute(f"create database dgxy;")
        tdSql.execute(f"use dgxy;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values(now, 1);")
        tdSql.execute(f"insert into st(tbname, ts, f) values('ct1', now, 2);")
        tdSql.query(f"select * from ct1;")
        tdSql.checkRows(2)

        tdSql.query(f"show tables like 'ct1';")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into st(tbname, ts, f, t) values('ct2',now,20,NULL)('ct3',now,30,NULL)")
        tdSql.execute(f"insert into st(tbname, t, ts, f) values('ct4',NULL, now,20)('ct5',NULL, now,30)")
        tdSql.query(f"show create table ct2")
        tdSql.query(f"show create table ct3")
        tdSql.query(f"show create table ct4")
        tdSql.query(f"show create table ct5")
        tdSql.query(f"show tags from ct2")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"show tags from ct3")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"show tags from ct4")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"show tags from ct5")
        tdSql.checkData(0, 5, None)

        tdSql.error(f"insert into d2.st values(now, 1, 1)")
        tdSql.error(f"insert into d2.st(ts, f) values(now, 1);")
        tdSql.error(f"insert into d2.st(ts, f, tbname) values(now, 1);")
        tdSql.error(f"insert into d2.st(ts, f, tbname) values(now, 1, '');")
        tdSql.error(f"insert into d2.st(ts, f, tbname) values(now, 1, 'd2.ct2');")
        tdSql.error(f"insert into d2.st(ts, tbname) values(now, 1, 34)")
        tdSql.error(f"insert into st using st2 tags(2) values(now,1);")
