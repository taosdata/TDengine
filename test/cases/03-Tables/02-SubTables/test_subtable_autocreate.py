from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestSubTableAutoCreate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    #
    # ------------------- sim ----------------
    #
    def do_sim_subtable_auto_create(self):
        self.TagsDynamicallySpec()
        tdStream.dropAllStreamsAndDbs()
        self.AutoCreateDropTable()
        tdStream.dropAllStreamsAndDbs()
        self.AutoCreateTb()
        tdStream.dropAllStreamsAndDbs()
        self.CreateTbWithTagName()
        tdStream.dropAllStreamsAndDbs()

    def TagsDynamicallySpec(self):
        db = "dytag_db"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== tags_dynamically_specify.sim")
        i = 0

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table stb (ts timestamp, c1 binary(10), c2 int, c3 float) tags (t1 binary(10), t2 int, t3 float)"
        )

        tdSql.execute(
            f"insert into tb1 using stb (t1) tags ('tag1') values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.execute(
            f"insert into tb2 using stb (t2) tags (2)      values ( now + 2s, 'binary2', 2, 2.2)"
        )
        tdSql.execute(
            f"insert into tb3 using stb (t3) tags (3.3)    values ( now + 3s, 'binary3', 3, 3.3)"
        )

        tdSql.execute(
            f"insert into tb4 (ts, c1, c2) using stb (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4', 4)"
        )
        tdSql.execute(
            f"insert into tb5 (ts, c1, c3) using stb (t1, t3) tags ('tag5', 11.11) values ( now + 5s, 'binary5', 5.5)"
        )
        tdSql.execute(
            f"insert into tb6 (ts, c1, c3) using stb tags ('tag5', 6, 11.11) values ( now + 5s, 'binary6', 6.6)"
        )
        tdSql.execute(
            f"insert into tb7 (ts, c1, c2, c3) using stb tags ('tag5', 7, 11.11) values ( now + 5s, 'binary7', 7, 7.7)"
        )
        tdSql.query(f"select * from stb order by ts asc")
        tdSql.checkRows(7)

        tdSql.error(
            f"insert into tb11 using stb (t1) tags () values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.error(
            f"insert into tb12 using stb (t1, t3) tags () values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.error(
            f"insert into tb13 using stb (t1, t2, t3) tags (8, 9.13, 'ac') values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.error(
            f"insert into tb14 using stb () tags (2)  values ( now + 2s, 'binary2', 2, 2.2)"
        )
        tdSql.error(
            f"insert into tb15 using stb (t2, t3) tags (3.3)    values ( now + 3s, 'binary3', 3, 3.3)"
        )
        tdSql.error(
            f"insert into tb16 (ts, c1, c2) using stb (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4')"
        )
        tdSql.error(
            f"insert into tb17 (ts, c1, c3) using stb (t1, t3) tags ('tag5', 11.11, 5) values ( now + 5s, 'binary5', 5.5)"
        )
        tdSql.error(
            f"insert into tb18 (ts, c1, c3) using stb tags ('tag5', 16) values ( now + 5s, 'binary6', 6.6)"
        )
        tdSql.error(
            f"insert into tb19 (ts, c1, c2, c3) using stb tags (19, 'tag5', 91.11) values ( now + 5s, 'binary7', 7, 7.7)"
        )

        tdSql.execute(
            f"create table stbx (ts timestamp, c1 binary(10), c2 int, c3 float) tags (t1 binary(10), t2 int, t3 float)"
        )
        tdSql.execute(
            f"insert into tb100 (ts, c1, c2, c3) using stbx (t1, t2, t3) tags ('tag100', 100, 100.123456) values ( now + 10s, 'binary100', 100, 100.9) tb101 (ts, c1, c2, c3) using stbx (t1, t2, t3) tags ('tag101', 101, 101.9) values ( now + 10s, 'binary101', 101, 101.9) tb102 (ts, c1, c2, c3) using stbx (t1, t2, t3) tags ('tag102', 102, 102.9) values ( now + 10s, 'binary102', 102, 102.9)"
        )

        tdSql.query(f"select * from stbx order by t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 4, "tag100")

        tdSql.checkData(0, 5, 100)

        tdSql.checkData(0, 6, 100.12346)

        tdSql.execute(
            f"create table stby (ts timestamp, c1 binary(10), c2 int, c3 float) tags (t1 binary(10), t2 int, t3 float)"
        )
        tdSql.execute(f"reset query cache")
        tdSql.execute(
            f"insert into tby1 using stby (t1) tags ('tag1') values ( now + 1s, 'binary1', 1, 1.1)"
        )
        tdSql.execute(
            f"insert into tby2 using stby (t2) tags (2)      values ( now + 2s, 'binary2', 2, 2.2)"
        )
        tdSql.execute(
            f"insert into tby3 using stby (t3) tags (3.3)    values ( now + 3s, 'binary3', 3, 3.3)"
        )
        tdSql.execute(
            f"insert into tby4 (ts, c1, c2) using stby (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4', 4)"
        )
        tdSql.execute(
            f"insert into tby5 (ts, c1, c3) using stby (t1, t3) tags ('tag5', 11.11) values ( now + 5s, 'binary5', 5.5)"
        )
        tdSql.execute(
            f"insert into tby6 (ts, c1, c3) using stby tags ('tag5', 6, 11.11) values ( now + 5s, 'binary6', 6.6)"
        )
        tdSql.execute(
            f"insert into tby7 (ts, c1, c2, c3) using stby tags ('tag5', 7, 11.11) values ( now + 5s, 'binary7', 7, 7.7)"
        )
        tdSql.query(f"select * from stby order by ts asc")
        tdSql.checkRows(7)

        tdSql.execute(f"reset query cache")
        tdSql.execute(
            f"insert into tby1 using stby (t1) tags ('tag1') values ( now + 1s, 'binary1y', 1, 1.1)"
        )
        tdSql.execute(
            f"insert into tby2 using stby (t2) tags (2)      values ( now + 2s, 'binary2y', 2, 2.2)"
        )
        tdSql.execute(
            f"insert into tby3 using stby (t3) tags (3.3)    values ( now + 3s, 'binary3y', 3, 3.3)"
        )
        tdSql.execute(
            f"insert into tby4 (ts, c1, c2) using stby (t1, t2) tags ('tag4', 4)   values ( now + 4s, 'binary4y', 4)"
        )
        tdSql.execute(
            f"insert into tby5 (ts, c1, c3) using stby (t1, t3) tags ('tag5', 11.11) values ( now + 5s, 'binary5y', 5.5)"
        )
        tdSql.execute(
            f"insert into tby6 (ts, c1, c3) using stby tags ('tag5', 6, 11.11) values ( now + 5s, 'binary6y', 6.6)"
        )
        tdSql.execute(
            f"insert into tby7 (ts, c1, c2, c3) using stby tags ('tag5', 7, 11.11) values ( now + 5s, 'binary7y', 7, 7.7)"
        )

        tdSql.query(f"select * from stby order by ts asc")
        tdSql.checkRows(14)

        tdLog.info(f"===============================> td-1765")
        tdSql.execute(
            f"create table m1(ts timestamp, k int) tags(a binary(4), b nchar(4));"
        )
        tdSql.execute(f"create table tm0 using m1 tags('abcd', 'abcd');")
        tdSql.error(f"alter table tm0 set tag b = 'abcd1';")
        tdSql.error(f"alter table tm0 set tag a = 'abcd1';")
        print("do sim auto create .................... [passed]") 

    def AutoCreateDropTable(self):
        dbPrefix = "db"
        tbPrefix = "tb"
        stbPrefix = "stb"
        tbNum = 5
        rowNum = 1361
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== auto_create_tb_drop_tb.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")

        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")

        i = 0
        ts = ts0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags (t1 binary(10))")
        x = 0
        t = 0
        while t < tbNum:
            t1 = "'tb" + str(t) + "'"
            tbname = "tb" + str(t)
            tdLog.info(f"t = {t}")
            tdLog.info(f"tbname = {tbname}")
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(
                    f"insert into {tbname} using {stb} tags( {t1} ) values ( {ts} , {x} )"
                )
                x = x + 1
            t = t + 1
            x = 0
        tdLog.info(f"====== tables created")

        tdSql.execute(f"drop table tb2")
        x = 0
        while x < rowNum:
            ts = ts + delta
            t1 = "'tb" + str(t) + "'"
            tdSql.execute(
                f"insert into tb1 using {stb} tags( {t1} ) values ( {ts} , {x} )"
            )
            x = x + 1

        ts = ts0 + delta
        ts = ts + 1

        x = 0
        while x < 100:
            ts = ts + delta
            tdSql.execute(f"insert into tb2 using stb0 tags('tb2') values ( {ts} , 1)")
            tdSql.query(f"select * from tb2")
            res = x + 1
            tdSql.checkRows(res)

            tdSql.checkData(0, 1, 1)

            x = x + 1
            tdLog.info(f"loop {x}")

    def AutoCreateTb(self):
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

    def CreateTbWithTagName(self):
        tdLog.info(f"======================== dnode1 start")
        db = "testdb"

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int) tags (id int, t1 int, t2 nchar(4), t3 double)"
        )

        tdSql.execute(f"insert into tb1 using st2 (id, t1) tags(1,2) values (now, 1)")
        tdSql.query(f"select id,t1,t2,t3 from tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, None)

        tdSql.checkData(0, 3, None)

        tdSql.execute(f'create table tb2 using st2 (t2,t3) tags ("12",22.0)')
        tdSql.query(f"show tags from tb2")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, None)

        tdSql.checkData(1, 5, None)

        tdSql.checkData(2, 5, 12)

        tdSql.checkData(3, 5, "22.000000000")

        tdSql.execute(f'create table tb3 using st2 tags (1,2,"3",33.0);')
        tdSql.query(f"show tags  from tb3;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, 3)

        tdSql.checkData(3, 5, "33.000000000")

        tdSql.execute(f'insert into tb4 using st2 tags(1,2,"33",44.0) values (now, 1);')
        tdSql.query(f"show tags from tb4;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, 33)

        tdSql.checkData(3, 5, "44.000000000")

        tdSql.error(f'create table tb5 using st2() tags (3,3,"3",33.0);')
        tdSql.error(f'create table tb6 using st2 (id,t1) tags (3,3,"3",33.0);')
        tdSql.error(f"create table tb7 using st2 (id,t1) tags (3);")
        tdSql.error(f"create table tb8 using st2 (ide) tags (3);")
        tdSql.error(f"create table tb9 using st2 (id);")
        tdSql.error(f"create table tb10 using st2 (id t1) tags (1,1);")
        tdSql.error(f"create table tb10 using st2 (id,,t1) tags (1,1,1);")
        tdSql.error(f"create table tb11 using st2 (id,t1,) tags (1,1,1);")

        tdSql.execute(f"create table tb12 using st2 (t1,id) tags (2,1);")
        tdSql.query(f"show tags from tb12;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, None)

        tdSql.checkData(3, 5, None)

        tdSql.execute(f"create table tb13 using st2 (t1,id) tags (2,1);")
        tdSql.query(f"show tags from tb13;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 5, 1)

        tdSql.checkData(1, 5, 2)

        tdSql.checkData(2, 5, None)

        tdSql.checkData(3, 5, None)


    #
    # ------------------- army ----------------
    #
    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS test")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS test")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")


    def insert_table_auto_create(self):
        tdLog.info(f"insert table auto create")
        tdSql.execute("USE test")
        tdLog.info("start to test auto create insert...")
        tdSql.execute("INSERT INTO t_0 USING stb TAGS (0) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.execute("INSERT INTO t_0 USING stb TAGS (0) VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_0")
        tdSql.checkRows(2)

    def insert_table_pre_create(self):
        tdLog.info(f"insert table pre create")
        tdSql.execute("USE test")
        tdLog.info("start to pre create table...")
        tdSql.execute("CREATE TABLE t_1 USING stb TAGS (1)")
        tdLog.info("start to test pre create insert...")
        tdSql.execute("INSERT INTO t_1 USING stb TAGS (1) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.execute("INSERT INTO t_1 VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_1")
        tdSql.checkRows(2)

    def insert_table_auto_insert_with_cache(self):
        tdLog.info(f"insert table auto insert with cache")
        tdSql.execute("USE test")
        tdLog.info("start to test auto insert with cache...")
        tdSql.execute("CREATE TABLE t_2 USING stb TAGS (2)")
        tdLog.info("start to insert to init cache...")
        tdSql.execute("INSERT INTO t_2 VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.execute("INSERT INTO t_2 USING stb TAGS (2) VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_2")
        tdSql.checkRows(2)

    def insert_table_auto_insert_with_multi_rows(self):
        tdLog.info(f"insert table auto insert with multi rows")
        tdSql.execute("USE test")
        tdLog.info("start to test auto insert with multi rows...")
        tdSql.execute("CREATE TABLE t_3 USING stb TAGS (3)")
        tdLog.info("start to insert multi rows...")
        tdSql.execute("INSERT INTO t_3 VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test'), ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_3")
        tdSql.checkRows(2)

        tdLog.info("start to insert multi rows with direct insert and auto create...")
        tdSql.execute("INSERT INTO t_4 USING stb TAGS (4) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test'), t_3 VALUES ('2024-01-01 00:00:02', 1, 2.0, 'test')")
        tdSql.query("select * from t_4")
        tdSql.checkRows(1)
        tdSql.query("select * from t_3")
        tdSql.checkRows(3)

        tdLog.info("start to insert multi rows with auto create and direct insert...")
        tdSql.execute("INSERT INTO t_3 VALUES ('2024-01-01 00:00:03', 1, 2.0, 'test'),t_4 USING stb TAGS (4) VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test'),")
        tdSql.query("select * from t_4")
        tdSql.checkRows(2)
        tdSql.query("select * from t_3")
        tdSql.checkRows(4)

        tdLog.info("start to insert multi rows with auto create into same table...")
        tdSql.execute("INSERT INTO t_10 USING stb TAGS (10) VALUES ('2024-01-01 00:00:04', 1, 2.0, 'test'),t_10 USING stb TAGS (10) VALUES ('2024-01-01 00:00:05', 1, 2.0, 'test'),")
        tdSql.query("select * from t_10")
        tdSql.checkRows(2)

    def check_some_err_case(self):
        tdLog.info(f"check some err case")
        tdSql.execute("USE test")

        tdLog.info("start to test err stb name...")
        tdSql.error("INSERT INTO t_5 USING errrrxx TAGS (5) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table does not exist", fullMatched=False)

        tdLog.info("start to test err syntax name...")
        tdSql.error("INSERT INTO t_5 USING stb TAG (5) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error", fullMatched=False)

        tdLog.info("start to test err syntax values...")
        tdSql.error("INSERT INTO t_5 USING stb TAG (5) VALUS ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error", fullMatched=False)

        tdLog.info("start to test err tag counts...")
        tdSql.error("INSERT INTO t_5 USING stb TAG (5,1) VALUS ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error", fullMatched=False)

        tdLog.info("start to test err tag counts...")
        tdSql.error("INSERT INTO t_5 USING stb TAG ('dasds') VALUS ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error", fullMatched=False)

        tdLog.info("start to test err values counts...")
        tdSql.error("INSERT INTO t_5 USING stb TAGS (5) VALUES ('2024-01-01 00:00:00', 1, 1 ,2.0, 'test')", expectErrInfo="Illegal number of columns", fullMatched=False)

        tdLog.info("start to test err values...")
        tdSql.error("INSERT INTO t_5 USING stb TAGS (5) VALUES ('2024-01-01 00:00:00', 'dasdsa', 1 ,2.0, 'test')", expectErrInfo="syntax error", fullMatched=False)

    def check_same_table_same_ts(self):
        tdLog.info(f"check same table same ts")
        tdSql.execute("USE test")
        tdSql.execute("INSERT INTO t_6 USING stb TAGS (6) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test') t_6 USING stb TAGS (6) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.query("select * from t_6")
        tdSql.checkRows(1)

    def check_tag_parse_error_with_cache(self):
        tdLog.info(f"check tag parse error with cache")
        tdSql.execute("USE test")
        tdSql.execute("INSERT INTO t_7 USING stb TAGS (7) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.error("INSERT INTO t_7 USING stb TAGS ('ddd') VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error", fullMatched=False)
        tdSql.query("select * from t_7")
        tdSql.checkRows(1)

    def check_duplicate_table_with_err_tag(self):
        tdLog.info(f"check tag parse error with cache")
        tdSql.execute("USE test")
        tdSql.execute("INSERT INTO t_8 USING stb TAGS (8) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test') t_8 USING stb TAGS (ddd) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.query("select * from t_8")
        tdSql.checkRows(1)

    def check_table_with_another_stb_name(self):
        tdLog.info(f"check table with another stb name")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb2 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
        tdSql.execute("INSERT INTO t_20 USING stb2 TAGS (20) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.query("select * from t_20")
        tdSql.checkRows(1)
        tdSql.error("INSERT INTO t_20 USING stb TAGS (20) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables", fullMatched=False)
        tdSql.error("INSERT INTO t_20 USING stb TAGS (20) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables", fullMatched=False)

    def check_table_with_same_name(self):
        tdLog.info(f"check table with same name")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb3 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
        tdSql.error("INSERT INTO stb3 USING stb3 TAGS (30) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables")
        tdSql.query("select * from stb3")
        tdSql.checkRows(0)

    def check_table_with_same_name(self):
        tdLog.info(f"check table with same name")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb3 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
        tdSql.error("INSERT INTO stb3 USING stb3 TAGS (30) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables")
        tdSql.query("select * from stb3")
        tdSql.checkRows(0)

    # run
    def do_army_auto_create_insert(self):
        # prepare database
        self.prepare_database()

        # insert table auto create
        self.insert_table_auto_create()

        # insert table pre create
        self.insert_table_pre_create()

        # insert table auto insert with cache
        self.insert_table_auto_insert_with_cache()

        # insert table auto insert with multi rows
        self.insert_table_auto_insert_with_multi_rows()

        # check some err case
        self.check_some_err_case()

        # check same table same ts
        self.check_same_table_same_ts()

        # check tag parse error with cache
        self.check_tag_parse_error_with_cache()

        # check duplicate table with err tag
        self.check_duplicate_table_with_err_tag()

        # check table with another stb name
        self.check_table_with_another_stb_name()

        # check table with same name
        self.check_table_with_same_name()

        print("do army auto create ................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_subtable_auto_create(self):
        """Auto create child table

        1. Automatically create child tables by inserting data, query data, and query tag values
        2. Delete some of the created child tables and repeat the auto-creation
        3. When automatically creating child tables, use some Chinese tag values
        4. When automatically creating child tables, specify partial tag values
        5. Insert table auto insert with cache
        6. Check duplicate table with err tag
        7. Check table with another stb name
        8. Check table with same name
        9. Check same table same timestamp
        10. Check some error cases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/parser/tags_dynamically_specifiy.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/auto_create_tb_drop_tb.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/auto_create_tb.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/create_tb_with_tag_name.sim
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/create/test_auto_create_insert.py

        """
        self.do_sim_subtable_auto_create()
        self.do_army_auto_create_insert()