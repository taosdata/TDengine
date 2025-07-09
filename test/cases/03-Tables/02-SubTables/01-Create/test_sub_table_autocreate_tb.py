from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableAutoCreateTb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sub_table_auto_create_tb(self):
        """auto create tb

        1.

        Catalog:
            - Table:SubTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/auto_create_tb.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ac_db"
        tbPrefix = "ac_tb"
        stbPrefix = "ac_stb"
        tbNum = 10
        rowNum = 20
        totalNum = 200
        ts0 = 1537146000000

        tdLog.info(f"excuting test script auto_create_tb.sim")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix
        stb = stbPrefix
        tb = tbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdLog.info(f"=========== auto_create_tb.sim case1: test")
        tdSql.execute(
            f"CREATE TABLE {stb} (TS TIMESTAMP, C1 INT, C2 BIGINT, C3 FLOAT, C4 DOUBLE, C5 BINARY(10), C6 BOOL, C7 SMALLINT, C8 TINYINT, C9 NCHAR(10)) TAGS (T1 INT)"
        )
        tdSql.query(f"show stables")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, stb)

        ### create table on the fly
        tdSql.execute(
            f"insert into tb1 using {stb} tags (1) values ( {ts0} , 1,1,1,1,'bin',1,1,1,'涛思数据')"
        )
        tdSql.query(f"select * from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 9, "涛思数据")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.00000)
        tdSql.checkData(0, 4, 1.000000000)
        tdSql.checkData(0, 5, "bin")

        ### insert into an existing table
        ts1 = ts0 + 1000
        ts2 = ts0 + 2000
        tdSql.execute(
            f"insert into tb1 using {stb} tags (1) values ( {ts1} , 1,1,1,1,'bin',1,1,1,'涛思数据') ( {ts2} , 2,2,2,2,'binar', 1,1,1,'nchar')"
        )
        tdSql.query(f"select * from {stb}")
        tdSql.checkRows(3)
        tdSql.checkData(1, 9, "涛思数据")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 2.00000)
        tdSql.checkData(2, 5, "binar")
        tdSql.checkData(2, 9, "nchar")

        ### [TBASE-410] auto create table with a negative tag value
        ts1 = ts0 + 1000
        ts2 = ts0 + 2000
        tdSql.execute(
            f"insert into tb_1 using {stb} tags (-1) values ( {ts1} , 1,1,1,1,'bin',1,1,1,'涛思数据') ( {ts2} , 2,2,2,2,'binar', 1,1,1,'nchar')"
        )
        tdSql.query(f"select * from {stb}")
        tdSql.checkRows(5)
        tdSql.checkData(0, 9, "涛思数据")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 2.00000)
        tdSql.checkData(4, 5, "binar")
        tdSql.checkData(4, 9, "nchar")
        tdSql.execute(f"drop table tb_1")

        #### insert into an existing table with wrong tags
        ts3 = ts0 + 3000
        ts4 = ts0 + 4000
        tdSql.execute(
            f"insert into tb1 using {stb} tags (2) values ( {ts3} , 1,1,1,1,'bin',1,1,1,'涛思数据') ( {ts4} , 2,2,2,2,'binar', 1,1,1,'nchar')"
        )
        tdSql.query(f"select * from {stb}")
        tdSql.checkRows(5)
        tdSql.checkData(0, 9, "涛思数据")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 2.00000)
        tdSql.checkData(4, 5, "binar")
        tdSql.checkData(4, 9, "nchar")
        tdSql.execute(f"drop table tb1")

        #### auto create multiple tables
        tdSql.execute(
            f"insert into tb1 using {stb} tags(1) values ( {ts0} , 1, 1, 1, 1, 'bin1', 1, 1, 1, '涛思数据1') tb2 using {stb} tags(2) values ( {ts0} , 2, 2, 2, 2, 'bin2', 2, 2, 2, '涛思数据2') tb3 using {stb} tags(3) values ( {ts0} , 3, 3, 3, 3, 'bin3', 3, 3, 3, '涛思数据3')"
        )
        tdSql.query(f"show tables")
        tdLog.info(
            f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(3)
        tdSql.checkKeyData("tb1", 0, "tb1")
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb3", 0, "tb3")

        tdSql.query(f"select c1,c1,c2,c3,c4,c5,c7,c8,c9 from {stb}")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 1, 1)
        tdSql.checkKeyData(1, 8, "涛思数据1")
        tdSql.checkKeyData(2, 4, 2.000000000)
        tdSql.checkKeyData(2, 8, "涛思数据2")
        tdSql.checkKeyData(3, 8, "涛思数据3")

        tdSql.query(
            f"select t1, count(*), first(c9) from {stb} partition by t1 order by t1 asc slimit 3"
        )
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 1, 1)
        tdSql.checkKeyData(1, 2, "涛思数据1")
        tdSql.checkKeyData(2, 1, 1)
        tdSql.checkKeyData(2, 2, "涛思数据2")
        tdSql.checkKeyData(3, 1, 1)
        tdSql.checkKeyData(3, 2, "涛思数据3")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        #### auto create multiple tables
        tdSql.execute(
            f"insert into tb1 using {stb} tags(1) values ( {ts0} , 1, 1, 1, 1, 'bin1', 1, 1, 1, '涛思数据1') tb2 using {stb} tags(2) values ( {ts0} , 2, 2, 2, 2, 'bin2', 2, 2, 2, '涛思数据2') tb3 using {stb} tags(3) values ( {ts0} , 3, 3, 3, 3, 'bin3', 3, 3, 3, '涛思数据3')"
        )
        tdSql.query(f"show tables")
        tdSql.checkRows(3)
        tdSql.checkKeyData("tb1", 0, "tb1")
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb3", 0, "tb3")

        tdSql.query(f"select c1,c1,c2,c3,c4,c5,c7,c8,c9 from {stb}")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 1, 1)
        tdSql.checkKeyData(1, 8, "涛思数据1")
        tdSql.checkKeyData(2, 4, 2.000000000)
        tdSql.checkKeyData(2, 8, "涛思数据2")
        tdSql.checkKeyData(3, 8, "涛思数据3")

        tdSql.query(
            f"select t1, count(*), first(c9) from {stb} partition by t1 order by t1 asc slimit 3"
        )
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 1, 1)
        tdSql.checkKeyData(1, 2, "涛思数据1")
        tdSql.checkKeyData(2, 1, 1)
        tdSql.checkKeyData(2, 2, "涛思数据2")
        tdSql.checkKeyData(3, 1, 1)
        tdSql.checkKeyData(3, 2, "涛思数据3")

        tdLog.info(f"======= too many columns in auto create tables")
        tdSql.execute(
            f"create table tick (ts timestamp , last_prc double , volume int, amount double, oi int , bid_prc1 double, ask_prc1 double, bid_vol1 int, ask_vol1 int , bid_prc2 double, ask_prc2 double, bid_vol2 int, ask_vol2 int , bid_prc3 double, ask_prc3 double, bid_vol3 int, ask_vol3 int , bid_prc4 double, ask_prc4 double, bid_vol4 int, ask_vol4 int , bid_prc5 double, ask_prc5 double, bid_vol5 int, ask_vol5 int , open_prc double, highest_prc double, low_prc double, close_prc double , upper_limit double, lower_limit double) TAGS (ticker BINARY(32), product BINARY(8));"
        )
        tdSql.query(f"show stables")

        tdSql.checkRows(2)

        tdSql.execute(
            f"insert into tick_000001 (ts, last_prc, volume, amount, oi, bid_prc1, ask_prc1) using tick tags ('000001', 'Stocks') VALUES (1546391700000, 0.000000, 0, 0.000000, 0, 0.000000, 10.320000);"
        )
        tdSql.query(f"select tbname from tick")
        tdSql.checkRows(1)

        # sql drop database $db
        # sql select * from information_schema.ins_databases
        # if $rows != 0 then
        #  return -1
        # endi

        # [tbase-673]
        tdSql.execute(f"create table tu(ts timestamp, k int);")
        tdSql.error(f"create table txu using tu tags(0) values(now, 1);")

        tdLog.info(f"=================> [TBASE-675]")
        tdSql.execute(
            f"insert into tu values(1565971200000, 1) (1565971200000,2) (1565971200001, 3)(1565971200001, 4)"
        )
        tdSql.query(f"select * from tu")
        tdSql.checkRows(2)
