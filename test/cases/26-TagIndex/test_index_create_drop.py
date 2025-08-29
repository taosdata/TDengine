from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, cluster, sc, clusterComCheck


class TestIndexCreateDrop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_index_create_drop(self):
        """Tagindex: create and drop

        1. Create a super table
        2. Create an index on a specific tag column
        3. Query using the indexed tag
        4. Drop the existing index
        5. Query using the tag whose index was just dropped

        Catalog:
            - Index

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tagindex/add_index.sim

        """

        tdLog.info(f"======== step0")
        dbPrefix = "ta_3_db"
        tbPrefix = "ta_3_tb"
        mtPrefix = "ta_3_mt"
        tbNum = 50
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbPrefix, drop=True)
        tdSql.execute(f"use {dbPrefix}")

        tdLog.info(f"=============== create super table and register tag index")
        tdSql.execute(
            f"create table if not exists {mtPrefix} (ts timestamp, c1 int) tags (t1 int, t2 int, t3 int, t4 int, t5 int)"
        )

        tdSql.execute(f"use information_schema;")
        tdSql.execute(f"create index t2i on ta_3_db.ta_3_mt(t2)")
        tdSql.error(f"create index t3i on ta_3_mt(t3)")
        tdSql.error(f"drop index t2i")
        tdSql.execute(f"drop index ta_3_db.t2i")
        tdSql.execute(f"use {dbPrefix}")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")

        i = 0
        val = 1
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(
                f"create table {tb} using {mtPrefix} tags( {i} , {i} , {i} , {i} , {val} );"
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

        tdSql.error(f"create index ti1 on {mtPrefix} (t1)")
        tdSql.execute(f"create index ti2 on {mtPrefix} (t2)")
        tdSql.execute(f"create index ti5 on {mtPrefix} (t5)")

        tdLog.info(f"==== test name conflict")

        tdSql.error(f"create index ti1 on {mtPrefix}(t1)")
        tdSql.error(f"create index ti11 on {mtPrefix}(t1)")
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

        i = 0
        while i < tbNum:
            tdSql.query(f"select * from {mtPrefix} where t1= {i} ;")
            tdSql.checkRows(1)
            i = i + 1

        tdLog.info(f"===== test operator great equal")
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
        i = 0
        while i < tbNum:
            tdSql.query(f"select * from {mtPrefix} where t2 < {i} ;")
            tmp = i
            tdSql.checkRows(tmp)
            i = i + 1

        # test special boundary condtion
        tdLog.info(f"===== test operator low on ti5")
        # great equal than
        tdSql.query(f"select * from {mtPrefix} where t5 < 0 ;")
        tdSql.checkRows(0)

        tdLog.info(f"===== test operator lower equal on ti5")
        # great equal than
        tdSql.query(f"select * from {mtPrefix} where t5 <= {val} ;")
        tdSql.checkRows(tbNum)

        tdLog.info(f"===== test operator equal on ti5")
        # great equal than
        tdSql.query(f"select * from {mtPrefix} where t5 = {val} ;")
        tdSql.checkRows(tbNum)

        tdLog.info(f"===== test operator great equal on ti5")
        # great equal than
        tdSql.query(f"select * from {mtPrefix} where t5 >= {val} ;")
        tdSql.checkRows(tbNum)

        tdLog.info(f"===== test operator great than on ti5")
        # great equal than
        tdSql.query(f"select * from {mtPrefix} where t5 > {val} ;")
        tdSql.checkRows(0)

        tdLog.info(f"===== test operator great on ti5")
        # great equal than
        tdSql.query(f"select * from {mtPrefix} where t5 > {val} + 1 ;")
        tdSql.checkRows(0)

        tdSql.execute(f"drop index ti5")

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
        tdSql.query(f"select * from information_schema.ins_indexes")
        tdSql.checkRows(1)

        tdSql.error(f"drop index t2")
        tdSql.error(f"drop index t3")

        # sql create index ti0 on $mtPrefix (t1)

        i = interval
        while i < limit:
            tdSql.query(f"select * from {mtPrefix} where t1 <= {i} ;")
            tmp = i - interval
            tmp = tmp + 1
            tdSql.checkRows(tmp)
            i = i + 1

        tdSql.error(f"create index ti0 on {mtPrefix} (t1)")
        tdSql.error(f"create index ti2 on {mtPrefix} (t1)")
        tdSql.error(f"create index t2i on ta_3_tb17 (t2)")
