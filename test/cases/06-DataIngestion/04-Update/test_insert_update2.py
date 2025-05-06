from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertUpdate2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_update2(self):
        """update sub table data

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-29 Simon Guan Migrated from tsim/insert/update2.sim

        """

        ### 4096*0.8 - 1 = 3275
        rowSuperBlk = 3275
        ### 4096 - 3275 -1 - 1 = 819
        rowSubBlk = 819
        ts0 = 1672372800000
        ts1 = 1672372900000
        ts2 = 1672686800000

        i = 0
        db = "db0"
        stb1 = "stb1"
        tb1 = "tb1"
        stb2 = "stb2"
        tb2 = "tb2"

        tdLog.info(f"====== create database")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} keep 1000 duration 10")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb1} (ts timestamp, c1 bigint, c2 bigint, c3 bigint) tags(t1 int)"
        )
        tdSql.execute(
            f"create table {stb2} (ts timestamp, c1 bigint, c2 bigint, c3 bigint, c4 bigint, c5 bigint, c6 bigint, c7 bigint, c8 bigint, c9 bigint, c10 bigint, c11 bigint, c12 bigint, c13 bigint, c14 bigint, c15 bigint, c16 bigint, c17 bigint, c18 bigint, c19 bigint, c20 bigint, c21 bigint, c22 bigint, c23 bigint, c24 bigint, c25 bigint, c26 bigint, c27 bigint, c28 bigint, c29 bigint, c30 bigint) tags(t1 int)"
        )
        tdSql.execute(f"create table {tb1} using {stb1} tags(1)")
        tdSql.execute(f"create table {tb2} using {stb2} tags(2)")
        tdLog.info(f"====== tables created")

        tdLog.info(f"========== step 1: merge dataRow in mem")

        i = 0
        while i < rowSuperBlk:
            xs = i * 10
            ts = ts2 + xs
            tdSql.execute(f"insert into {tb1} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdSql.execute(f"insert into {tb1} values ( {ts0} , 1,NULL,0)")
        tdSql.execute(f"insert into {tb1} (ts,c2,c3) values ( {ts0} , 1,1)")

        tdSql.query(f"select * from {tb1} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)

        tdLog.info(f"========== step 2: merge kvRow in mem")
        i = 0
        while i < rowSuperBlk:
            xs = i * 10
            ts = ts2 + xs
            tdSql.execute(f"insert into {tb2} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdSql.execute(f"insert into {tb2} (ts,c3,c8,c10) values ( {ts0} , 1,NULL,0)")
        tdSql.execute(f"insert into {tb2} (ts,c8,c10) values ( {ts0} , 1,1)")

        tdSql.query(f"select ts,c1,c3,c8,c10 from {tb2} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdLog.info(f"========== step 3: merge dataRow in file")
        tdSql.execute(f"insert into {tb1} (ts,c1) values ( {ts0} , 2)")
        tdLog.info(f"========== step 4: merge kvRow in file")
        tdSql.execute(f"insert into {tb2} (ts,c3) values ( {ts0} , 2)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.query(f"select * from {tb1} where ts = {ts0}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)

        tdSql.query(f"select ts,c1,c3,c8,c10 from {tb2} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"========== step 5: merge dataRow in file/mem")
        i = 0
        while i < rowSubBlk:
            xs = i * 1
            ts = ts1 + xs
            tdSql.execute(f"insert into {tb1} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdLog.info(f"========== step 6: merge kvRow in file/mem")
        i = 0
        while i < rowSubBlk:
            xs = i * 1
            ts = ts1 + xs
            tdSql.execute(f"insert into {tb2} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"insert into {tb1} (ts,c3) values ( {ts0} , 3)")
        tdSql.execute(f"insert into {tb2} (ts,c3) values ( {ts0} , 3)")
        tsN = ts0 + 1
        tdSql.execute(f"insert into {tb1} (ts,c1,c3) values ( {tsN} , 1,0)")
        tdSql.execute(f"insert into {tb2} (ts,c3,c8) values ( {tsN} , 100,200)")
        tsN = ts0 + 2
        tdSql.execute(f"insert into {tb1} (ts,c1,c3) values ( {tsN} , 1,0)")
        tdSql.execute(f"insert into {tb2} (ts,c3,c8) values ( {tsN} , 100,200)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.query(f"select * from {tb1} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 3)

        tdSql.query(f"select ts,c1,c3,c8,c10 from {tb2} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
