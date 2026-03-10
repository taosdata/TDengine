from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertDelete:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_delete(self):
        """Delete data

        1. Deleting data from normal tables
        2. Deleting data from super tables
        3. Deleting data from child tables

        Catalog:
            - DataDeletion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/insert/delete0.sim

        """

        tdLog.info(f"=============== create database with different precision")
        tdSql.execute(f"create database d0 keep 365")
        tdSql.execute(f"create database d1 keep 365 precision 'ms'")
        tdSql.execute(f"create database d2 keep 365 precision 'us'")
        tdSql.execute(f"create database d3 keep 365 precision 'ns'")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(6)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdSql.execute(
            f"create table if not exists d0.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d1.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d2.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d3.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d0.ntb (ts timestamp, c1 int, c2 float, c3 double)"
        )
        tdSql.execute(
            f"create table if not exists d1.ntb (ts timestamp, c1 int, c2 float, c3 double)"
        )
        tdSql.execute(
            f"create table if not exists d2.ntb (ts timestamp, c1 int, c2 float, c3 double)"
        )
        tdSql.execute(
            f"create table if not exists d3.ntb (ts timestamp, c1 int, c2 float, c3 double)"
        )

        tdSql.execute(f"create table d0.ct1 using d0.stb tags(1000)")
        tdSql.execute(f"create table d1.ct1 using d1.stb tags(1000)")
        tdSql.execute(f"create table d2.ct1 using d2.stb tags(1000)")
        tdSql.execute(f"create table d3.ct1 using d3.stb tags(1000)")
        tdSql.execute(f"create table d0.ct2 using d0.stb tags(1000)")
        tdSql.execute(f"create table d1.ct2 using d1.stb tags(1000)")
        tdSql.execute(f"create table d2.ct2 using d2.stb tags(1000)")
        tdSql.execute(f"create table d3.ct2 using d3.stb tags(1000)")

        tdSql.execute(f"insert into d0.ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d1.ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d2.ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d3.ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d0.ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d1.ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d2.ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d3.ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d0.ntb values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d1.ntb values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d2.ntb values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(f"insert into d3.ntb values(now+0s, 10, 2.0, 3.0)")

        tdLog.info(f"=============== query data from super table")
        tdSql.query(f"select count(*) from d0.stb")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from d1.stb")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from d2.stb")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from d3.stb")
        tdSql.checkData(0, 0, 2)

        tdLog.info(f"=============== delete from child table")
        tdSql.execute(f"delete from d0.ct1 where ts < now()")
        tdSql.execute(f"delete from d1.ct1 where ts < now()")
        tdSql.execute(f"delete from d2.ct1 where ts < now()")
        tdSql.execute(f"delete from d3.ct1 where ts < now()")

        tdLog.info(f"=============== query data from super table")
        tdSql.query(f"select count(*) from d0.stb")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from d1.stb")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from d2.stb")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from d3.stb")
        tdSql.checkData(0, 0, 1)

        tdLog.info(f"=============== query data from normal table")
        tdSql.query(f"select count(*) from d0.ntb")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from d1.ntb")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from d2.ntb")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from d3.ntb")
        tdSql.checkData(0, 0, 1)

        tdLog.info(f"=============== delete from super table")
        tdSql.execute(f"delete from d0.stb where ts < now()")
        tdSql.execute(f"delete from d1.stb where ts < now()")
        tdSql.execute(f"delete from d2.stb where ts < now()")
        tdSql.execute(f"delete from d3.stb where ts < now()")

        tdLog.info(f"=============== query data from super table")
        tdSql.query(f"select count(*) from d0.stb")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from d1.stb")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from d2.stb")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from d3.stb")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== delete from normal table")
        tdSql.execute(f"delete from d0.ntb where ts < now()")
        tdSql.execute(f"delete from d1.ntb where ts < now()")
        tdSql.execute(f"delete from d2.ntb where ts < now()")
        tdSql.execute(f"delete from d3.ntb where ts < now()")

        tdLog.info(f"=============== query data from normal table")
        tdSql.query(f"select count(*) from d0.ntb")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from d1.ntb")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from d2.ntb")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from d3.ntb")
        tdSql.checkData(0, 0, 0)
