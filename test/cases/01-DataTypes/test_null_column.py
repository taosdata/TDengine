from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestNullColumn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_null_column(self):
        """NULL: column

        1. Create table
        2. Insert data with NULL
        3. Query data

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/null.sim
            - 2025-8-22 Simon Guan Migrated from tsim/query/nullColSma.sim

        """

        self.ComputeNull()
        tdStream.dropAllStreamsAndDbs()
        self.NullColSma()
        tdStream.dropAllStreamsAndDbs()

    def ComputeNull(self):
        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int, tbcol2 int) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc

                v1 = str(x)
                v2 = str(x)
                if x == 0:
                    v1 = "NULL"

                tdSql.execute(f"insert into {tb} values ({ms} , {v1} , {v2} )")
                x = x + 1

            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 0)

        tdSql.checkRows(20)

        tdLog.info(f"=============== step3")
        tdSql.query(
            f"select count(tbcol), count(tbcol2), avg(tbcol), avg(tbcol2), sum(tbcol), sum(tbcol2) from {tb}"
        )
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdSql.checkData(0, 0, 19)
        tdSql.checkData(0, 1, 20)
        tdSql.checkData(0, 2, 10.000000000)
        tdSql.checkData(0, 3, 9.500000000)
        tdSql.checkData(0, 4, 190)
        tdSql.checkData(0, 5, 190)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select * from {tb}  where tbcol2 = 19")
        tdLog.info(f"===> {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 19)
        tdSql.checkData(0, 2, 19)

        tdSql.query(f"select * from {tb}  where tbcol is NULL")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {tb}  where tbcol = NULL")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step5")
        tdSql.execute(f"create table tt using {mt} tags( NULL )")

        tdSql.query(f"select * from {mt}  where tgcol is NULL")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol), count(tbcol2), avg(tbcol), avg(tbcol2), sum(tbcol), sum(tbcol2) from {mt}"
        )
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdSql.checkData(0, 0, 190)
        tdSql.checkData(0, 1, 200)
        tdSql.checkData(0, 2, 10.000000000)
        tdSql.checkData(0, 3, 9.500000000)
        tdSql.checkData(0, 4, 1900)
        tdSql.checkData(0, 5, 1900)

        tdLog.info(f"=============== step7")
        tdSql.execute(f"create table t1 (ts timestamp, i bool)")
        tdSql.execute(f"create table t2 (ts timestamp, i smallint)")
        tdSql.execute(f"create table t3 (ts timestamp, i tinyint)")
        tdSql.execute(f"create table t4 (ts timestamp, i int)")
        tdSql.execute(f"create table t5 (ts timestamp, i bigint)")
        tdSql.execute(f"create table t6 (ts timestamp, i float)")
        tdSql.execute(f"create table t7 (ts timestamp, i double)")
        tdSql.execute(f"create table t8 (ts timestamp, i binary(10))")
        tdSql.execute(f"insert into t1 values(now, NULL)")
        tdSql.execute(f"insert into t2 values(now, NULL)")
        tdSql.execute(f"insert into t3 values(now, NULL)")
        tdSql.execute(f"insert into t4 values(now, NULL)")
        tdSql.execute(f"insert into t5 values(now, NULL)")
        tdSql.execute(f"insert into t6 values(now, NULL)")
        tdSql.execute(f"insert into t7 values(now, NULL)")

        tdSql.query(f"select * from t2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from t3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from t4")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from t5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from t6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from t7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def NullColSma(self):
        dbPrefix = "m_in_db"
        tbPrefix = "m_in_tb"
        mtPrefix = "m_in_mt"
        tbNum = 1
        rowNum = 200
        totalNum = 400

        tdLog.info(f"print =============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"drop database if exists f{db}")
        tdSql.execute(f"create database {db} vgroups 1 maxrows 200 minrows 10")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, f1 int, f2 float) TAGS(tgcol int)"
        )

        tdLog.info(f"print ====== start create child tables and insert data")
        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i})")
            x = 0
            while x < rowNum:
                cc = x * 1
                ms = 1601481600000 + cc

                tdSql.execute(f"insert into {tb} values ({ms} , NULL ,{x} )")
                x = x + 1
            i = i + 1

        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags( {i})")

        x = 0
        while x < rowNum:
            cc = x * 1
            ms = 1601481600000 + cc
            tdSql.execute(f"insert into {tb} values ({ms} ,{x} , NULL )")
            x = x + 1

        tdSql.execute(f"flush database {db}")

        tdLog.info(f"print =============== step2")
        i = 0
        tb = tbPrefix + str(i)
        tdLog.info(f"select max(f1) from {db}.{tb}")
        tdSql.query(f"select max(f1) from {db}.{tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        i = 1
        tb = tbPrefix + str(i)
        tdSql.query(f"select max(f2) from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        rowNum = 10

        tdLog.info(f"print ====== insert more data")
        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)

            x = 0
            while x < rowNum:
                cc = x * 1
                ms = 1601481700000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} ,{x} ,{x} )")
                x = x + 1
            i = i + 1

        i = 1
        tb = tbPrefix + str(i)
        x = 0
        while x < rowNum:
            cc = x * 1
            ms = 1601481700000 + cc
            tdSql.execute(f"insert into {tb} values ({ms} ,{x} ,{x} )")
            x = x + 1

        tdSql.execute(f"flush database {db}")

        tdLog.info(f"print =============== step3")

        i = 0
        tb = tbPrefix + str(i)
        tdSql.query(f"select max(f1) from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9)

        i = 1
        tb = tbPrefix + str(i)
        tdSql.query(f"select max(f2) from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9.00000)
