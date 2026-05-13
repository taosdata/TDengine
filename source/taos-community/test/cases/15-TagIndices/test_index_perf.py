from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, cluster, sc, clusterComCheck


class TestIndexPerf:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_index_perf(self):
        """Tagindex: perf

        1. Create a super table
        2. Create a large number of child tables (this example uses 10, but should use 100,000 after the data preloading feature is implemented)
        3. Insert one record into each child table
        4. Create an index
        5. Query each child table using tag filtering

        Catalog:
            - Index

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tagindex/perf.sim

        """

        tdLog.info(f"======== step0")
        dbPrefix = "ta_3_db"
        tbPrefix = "ta_3_tb"
        mtPrefix = "ta_3_mt"
        tbNum = 100
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbPrefix, drop=True)
        tdSql.execute(f"use {dbPrefix}")

        tdLog.info(f"=============== create super table and register tag index")
        tdSql.execute(
            f"create table if not exists {mtPrefix} (ts timestamp, c1 int) tags (t1 int, t2 int, t3 int, t4 int, t5 int)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(
                f"create table {tb} using {mtPrefix} tags( {i} , {i} , {i} , {i} , {i} );"
            )
            i = i + 1

        tdSql.query(f"show tables")
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== insert data into each table")
        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"insert into {tb} values(now, {i} );")
            i = i + 1

        tdSql.execute(f"create index ti2 on {mtPrefix} (t2)")

        tdLog.info(f"==== test name conflict")
        #
        tdSql.error(f"create index ti3 on {mtPrefix}(t2)")
        tdSql.error(f"create index ti2 on {mtPrefix}(t2)")
        tdSql.error(f"create index ti2 on {mtPrefix}(t1)")
        tdSql.error(f"create index ti2 on {mtPrefix}(t3)")
        tdSql.error(f"create index ti2 on {mtPrefix}(txx)")

        tdLog.info(f"===== test operator equal")

        i = 0
        while i < tbNum:
            tdSql.query(f"select * from {mtPrefix} where t2= {i} ;")
            tdSql.checkRows(1)
            i = i + 1

        tdLog.info(f"===== test operator great equal")
        # great equal than
        i = 0
        while i < tbNum:
            tdSql.query(f"select * from {mtPrefix} where t2 >= {i} ;")
            tmp = tbNum - i
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"===== test operator great")
        i = 0
        while i < tbNum:
            tdSql.query(f"select * from {mtPrefix} where t2 > {i} ;")
            tmp = tbNum - i
            tmp = tmp - 1
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"===== test operator lower")
        i = 0
        while i < tbNum:
            tdSql.query(f"select * from {mtPrefix} where t2 <= {i} ;")
            tmp = i + 1
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"===== test operator lower than")
        # lower equal than
        i = 0
        while i < tbNum:
            tdSql.query(f"select * from {mtPrefix} where t2 < {i} ;")
            tmp = i
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"===== add table after index created")
        interval = tbNum + tbNum
        i = interval
        limit = interval + tbNum
        while i < limit:
            tb = tbPrefix + str(i)
            tdSql.execute(
                f"insert into {tb} using {mtPrefix} tags( {i} , {i} , {i} , {i} , {i} ) values( now, {i} )"
            )
            i = i + 1

        tdLog.info(f"===== add table after index created (opeator  great equal than)")
        # great equal than
        i = interval
        while i < limit:
            tdSql.query(f"select * from {mtPrefix} where t2 >= {i} ;")
            tmp = limit - i
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"===== add table after index created (opeator great than)")
        # great than
        i = interval
        while i < limit:
            tdSql.query(f"select * from {mtPrefix} where t2 > {i} ;")
            tmp = limit - i
            tmp = tmp - 1
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"===== drop table after index created")

        # drop table
        dropTbNum = tbNum / 2
        i = 0

        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"drop table {tb}")
            i = i + 1

        tdLog.info(f"===== drop table after index created(opeator lower than)")

        # lower equal than
        i = interval
        while i < limit:
            tdSql.query(f"select * from {mtPrefix} where t2 < {i} ;")
            tmp = i - interval
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"===== drop table after index created(opeator lower equal than)")

        # lower equal than
        i = interval
        while i < limit:
            tdSql.query(f"select * from {mtPrefix} where t2 <= {i} ;")
            tmp = i - interval
            tmp = tmp + 1
            tdSql.checkRows(tmp)
            i = i + 1

        tdLog.info(f"=== show index")

        tdSql.query(f"select * from information_schema.ins_indexes")
        tdSql.checkRows(2)

        tdLog.info(f"=== drop index ti2")
        tdSql.execute(f"drop index ti2")

        tdLog.info(f"=== drop not exist index")
        tdSql.error(f"drop index t2")
        tdSql.error(f"drop index t3")

        tdSql.error(f"create index ti0 on {mtPrefix} (t1)")
        tdSql.error(f"create index t2i on ta_3_tb17 (t2)")
