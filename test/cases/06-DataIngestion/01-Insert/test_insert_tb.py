from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertTb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_tb(self):
        """insert tb

        1. 待补充

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/parser/insert_tb.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
        mtPrefix = "fi_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"create_tb test")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, col1 int, col2 bigint, col3 float, col4 double, col5 binary(20), col6 bool, col7 smallint, col8 tinyint, col9 nchar(10)) tags (tag1 int)"
        )

        # case: insert multiple records in a query
        tdLog.info(
            f"=========== create_tb.sim case: insert multiple records in a query"
        )
        ts = 1500000000000
        col1 = 1
        col2 = 1
        col3 = "1.1e3"
        col4 = "1.1e3"
        col5 = '"Binary"'
        col6 = "true"
        col7 = 1
        col8 = 1
        col9 = '"Nchar"'
        tag1 = 1
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"create table {tb} using {mt} tags( {tag1} )")
        tdSql.execute(
            f"insert into {tb} values ( {ts} , {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} ) ( {ts} + 1000a, {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} )"
        )
        tdSql.query(f"select * from {tb} order by ts desc")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, col1)

        # sql drop table $tb

        # insert values for specified columns
        col1 = 2
        col3 = 3
        col5 = 5
        tdSql.execute(f"create table if not exists {tb} using {mt} tags( {tag1} )")
        tdSql.execute(
            f"insert into {tb} ( ts, col1, col3, col5) values ( {ts} + 2000a, {col1} , {col3} , {col5} )"
        )
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, col1)

        tdLog.info(f"tdSql.getData(0,3) = {tdSql.getData(0,3)}")
        tdSql.checkData(0, 3, 3.00000)
        tdSql.checkData(0, 5, col5)

        tdSql.execute(
            f"insert into {tb} (ts, col1, col2, col3, col4, col5, col6, col7, col8, col9) values ( {ts} + 3000a, {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} ) ( {ts} + 4000a, {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} )"
        )
        tdSql.query(f"select * from {tb} order by ts desc")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(5)

        # case: insert records from .csv files
        # manually test
        # sql insert into $tb file parser/parser_test_data.csv
        # sql select * from $tb
        # if $rows != 10 then
        #  return -1
        # endi

        tdSql.execute(f"drop table {tb}")
        tdSql.execute(f"create table tb1 (ts timestamp, c1 int)")
        tdSql.execute(f"create table tb2 (ts timestamp, c1 int)")
        tdSql.execute(f"insert into tb1 values(now, 1) tb2 values (now, 2)")
        tdSql.query(f"select count(*) from tb1")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from tb2")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table stb1 (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute(
            f"create table stb2 (ts timestamp, c1 double, c2 binary(10)) tags(t1 binary(10))"
        )
        tdSql.execute(f"create table tb1 using stb1 tags(1)")
        tdSql.execute(f"create table tb2 using stb2 tags('tb2')")
        tdSql.execute(
            f"insert into tb1 (ts, c1) values (now-1s, 1) (now, 2) tb2 (ts, c1) values (now-2s, 1) (now-1s, 2) (now, 3)"
        )
        tdSql.query(f"select * from tb1 order by ts asc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)

        tdSql.query(f"select * from tb2 order by ts desc")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 3.000000000)
        tdSql.checkData(1, 1, 2.000000000)
        tdSql.checkData(2, 1, 1.000000000)

        tdSql.execute(f"drop database {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 nchar(10), c6 binary(20)) tags(t1 int, t2 bigint, t3 double, t4 float, t5 nchar(10))"
        )
        tdSql.execute(f"create table tb0 using stb tags(0, 0, 0, 0, '涛思')")
        tdSql.execute(f"create table tb1 using stb tags('1', 1, 1, 1, '涛思')")
        tdSql.execute(f"create table tb2 using stb tags(2, '2', 2, 2, '涛思')")
        tdSql.execute(f"create table tb3 using stb tags(3, 3, '3', 3, '涛思')")
        tdSql.execute(f"create table tb4 using stb tags(4, 4, 4, '4', '涛思')")
        tdSql.execute(
            f"insert into tb0 values ('2018-09-17 09:00:00.000', 1, 1, 1, 1, '涛思nchar', 'none quoted')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2018-09-17 09:00:00.000', '1', 1, 1, 1, '涛思nchar', 'quoted int')"
        )
        tdSql.execute(
            f"insert into tb2 values ('2018-09-17 09:00:00.000', 1, '1', 1, 1, '涛思nchar', 'quoted bigint')"
        )
        tdSql.execute(
            f"insert into tb3 values ('2018-09-17 09:00:00.000', 1, 1, '1', 1, '涛思nchar', 'quoted float')"
        )
        tdSql.execute(
            f"insert into tb4 values ('2018-09-17 09:00:00.000', 1, 1, 1, '1', '涛思nchar', 'quoted double')"
        )
        tdSql.query(f"select * from stb order by t1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(1, 0, "2018-09-17 09:00:00")
        tdSql.checkData(2, 0, "2018-09-17 09:00:00")
        tdSql.checkData(3, 0, "2018-09-17 09:00:00")
        tdSql.checkData(4, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.00000)
        tdSql.checkData(0, 4, 1.000000000)
        tdSql.checkData(0, 5, "涛思nchar")
        tdSql.checkData(0, 6, "none quoted")
        tdSql.checkData(1, 3, 1.00000)
        tdSql.checkData(1, 4, 1.000000000)
        tdSql.checkData(1, 5, "涛思nchar")
        tdSql.checkData(1, 6, "quoted int")
        tdSql.checkData(2, 5, "涛思nchar")
        tdSql.checkData(2, 6, "quoted bigint")
        tdSql.checkData(3, 5, "涛思nchar")
        tdSql.checkData(3, 6, "quoted float")
        tdSql.checkData(4, 5, "涛思nchar")
        tdSql.checkData(4, 6, "quoted double")

        # case: support NULL char of the binary field [TBASE-660]
        tdSql.execute(
            f"create table NULLb (ts timestamp, c1 binary(20), c2 binary(20), c3 float)"
        )
        tdSql.execute(
            f"insert into NULLb values ('2018-09-17 09:00:00.000', '', '', 3.746)"
        )
        tdSql.query(f"select * from NULLb")
        tdSql.checkRows(1)
