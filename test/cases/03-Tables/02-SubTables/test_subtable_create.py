# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableCreate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")

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

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/parser/tags_dynamically_specifiy.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/auto_create_tb_drop_tb.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/auto_create_tb.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/create_tb_with_tag_name.sim
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/create/test_auto_create_insert.py
            - 2026-11-20 Alex Merged into 02-SubTables/test_subtable_create.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        self.do_sim_subtable_auto_create()
        self.do_army_auto_create_insert()


    def test_sub_table_batch_create(self):
        """Batch create

        1. Create super table
        2. Batch-create multiple child tables
        3. Insert data
        4. Query results
        5. Create a normal table
        6. Batch-insert data
        7. Query results

        Catalog:
            - Table:SubTable

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/table/basic1.sim
            - 2026-11-20 Alex Merged into 02-SubTables/test_subtable_create.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        # =========== TD-14042 start
        tdSql.prepare(dbname="db1")
        tdSql.prepare(dbname="db2")
        tdSql.prepare(dbname="db3")

        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table st1 (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table tb1 using st1 tags(1);")

        tdSql.execute(f"use db2;")
        tdSql.execute(f"create table st2 (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table tb2 using st2 tags(1);")

        tdSql.execute(f"use db3;")
        tdSql.execute(f"create table st3 (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table tb3 using st3 tags(1);")

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        # =========== TD-14042 end

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(6)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdSql.execute(f"use d1")

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create table st (ts timestamp, i int) tags (j int)")
        tdSql.execute(
            f"create table if not exists st (ts timestamp, i int) tags (j int)"
        )
        tdSql.execute(
            f"create table if not exists st (ts timestamp, i int) tags (j int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table st2 (ts timestamp, i float) tags (j int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(2)

        tdSql.execute(f"drop table st2")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table c1 using st tags(1)")
        tdSql.execute(f"create table c2 using st tags(2)")

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.execute(
            f"create table c3 using st tags(3) c4 using st tags(4) c5 using st tags(5) c6 using st tags(6) c7 using st tags(7)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(7)

        tdLog.info(f"=============== create normal table")
        tdSql.prepare(dbname="ndb")
        tdSql.execute(f"use ndb")
        tdSql.execute(f"create table nt0 (ts timestamp, i int)")
        # sql create table if not exists nt0 (ts timestamp, i int)
        tdSql.execute(f"create table nt1 (ts timestamp, i int)")
        # sql create table if not exists nt1 (ts timestamp, i int)
        tdSql.execute(f"create table if not exists nt3 (ts timestamp, i int)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdSql.execute(f"insert into nt0 values(now+1s, 1)(now+2s, 2)(now+3s, 3)")
        tdSql.execute(f"insert into nt1 values(now+1s, 1)(now+2s, 2)(now+3s, 3)")

        tdSql.query(f"select * from nt1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)

        tdLog.info(f"=============== insert data")
        tdSql.execute(f"use d1")
        tdSql.execute(f"insert into c1 values(now+1s, 1)")
        tdSql.execute(f"insert into c1 values(now+2s, 2)")
        tdSql.execute(f"insert into c1 values(now+3s, 3)")
        tdSql.execute(f"insert into c2 values(now+1s, 1)")
        tdSql.execute(f"insert into c2 values(now+2s, 2)")
        tdSql.execute(f"insert into c2 values(now+3s, 3)")
        tdSql.execute(f"insert into c3 values(now+1s, 1)")
        tdSql.execute(f"insert into c3 values(now+2s, 2)")
        tdSql.execute(f"insert into c3 values(now+3s, 3)")
        tdSql.execute(f"insert into c4 values(now+1s, 1)")
        tdSql.execute(f"insert into c4 values(now+2s, 2)")
        tdSql.execute(f"insert into c4 values(now+3s, 3)")
        tdSql.execute(f"insert into c5 values(now+1s, 1)")
        tdSql.execute(f"insert into c5 values(now+2s, 2)")
        tdSql.execute(f"insert into c5 values(now+3s, 3)")
        tdSql.execute(f"insert into c6 values(now+1s, 1)")
        tdSql.execute(f"insert into c6 values(now+2s, 2)")
        tdSql.execute(f"insert into c6 values(now+3s, 3)")
        tdSql.execute(f"insert into c7 values(now+1s, 1)")
        tdSql.execute(f"insert into c7 values(now+2s, 2)")
        tdSql.execute(f"insert into c7 values(now+3s, 3)")

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)

        tdSql.query(f"select * from c2")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c4")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c5")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c6")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c7")
        tdSql.checkRows(3)

        tdLog.info(f"=============== query data from st")
        tdLog.info(f"==============select * against super will cause crash.")
        tdSql.query(f"select ts from st")
        tdSql.checkRows(21)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)

        tdSql.query(f"select * from c2")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c4")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c5")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c6")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c7")
        tdSql.checkRows(3)

        tdLog.info(f"=============== query data from st")
        tdSql.query(f"select ts from st")
        tdSql.checkRows(21)

        tdLog.info(f"=============== query data from normal table after restart dnode")
        tdSql.execute(f"use ndb")
        tdSql.query(f"select * from nt1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 3)


    def test_subtable_drop(self):
        """Drop then query

        1. Repeatedly create and drop
        2. Query tags after deleting child tables

        Catalog:
            - Table:SubTable

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/stable/refcount.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/drop_tag.sim
            - 2026-11-20 Alex Merged into 02-SubTables/test_subtable_create.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        self.RefCount()
        tdStream.dropAllStreamsAndDbs()
        self.DropTag()
        tdStream.dropAllStreamsAndDbs()


    def RefCount(self):
        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname="d1", drop=True)
        tdSql.execute(f"use d1;")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t3 (ts timestamp, i int);")
        tdSql.execute(f"insert into d1.t1 values(now, 1);")
        tdSql.execute(f"insert into d1.t2 values(now, 1);")
        tdSql.execute(f"drop table d1.t1;")
        tdSql.execute(f"drop database d1;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step2")
        tdSql.prepare(dbname="d2", drop=True)
        tdSql.execute(f"use d2;")
        tdSql.execute(f"create table d2.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t3 (ts timestamp, i int);")
        tdSql.execute(f"insert into d2.t1 values(now, 1);")
        tdSql.execute(f"insert into d2.t2 values(now, 1);")
        tdSql.execute(f"drop table d2.t1;")
        tdSql.execute(f"drop table d2.t2;")
        tdSql.execute(f"drop table d2.t3;")

        tdSql.query(f"show d2.tables;")
        tdSql.checkRows(0)

        tdSql.query(f"show d2.vgroups;")
        tdSql.checkRows(2)

        tdSql.execute(f"drop database d2;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3")
        tdSql.prepare(dbname="d3", drop=True)
        tdSql.execute(f"use d3;")
        tdSql.execute(f"create table d3.st (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table d3.t1 using d3.st tags(1);")
        tdSql.execute(f"create table d3.t2 using d3.st tags(1);")
        tdSql.execute(f"create table d3.t3 using d3.st tags(1);")
        tdSql.execute(f"insert into d3.t1 values(now, 1);")
        tdSql.execute(f"drop table d3.t1;")
        tdSql.execute(f"drop table d3.t2;")
        tdSql.execute(f"drop table d3.t3;")

        tdSql.query(f"show d3.tables;")
        tdSql.checkRows(0)

        tdSql.query(f"show d3.vgroups;")
        tdSql.checkRows(2)

        tdSql.execute(f"drop database d3;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step4")
        tdSql.prepare(dbname="d4", drop=True)
        tdSql.execute(f"use d4;")
        tdSql.execute(f"create table d4.st (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table d4.t1 using d4.st tags(1);")
        tdSql.execute(f"create table d4.t2 using d4.st tags(1);")
        tdSql.execute(f"create table d4.t3 using d4.st tags(1);")
        tdSql.execute(f"insert into d4.t1 values(now, 1);")
        tdSql.execute(f"drop table d4.t1;")
        tdSql.execute(f"drop table d4.st;")

        tdSql.query(f"show d4.tables;")
        tdSql.checkRows(0)

        tdSql.query(f"show d4.stables;")
        tdSql.checkRows(0)

        tdSql.execute(f"drop database d4;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step5")
        tdSql.prepare(dbname="d5", drop=True)
        tdSql.execute(f"create table d5.st (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table d5.t1 using d5.st tags(1);")
        tdSql.execute(f"create table d5.t2 using d5.st tags(1);")
        tdSql.execute(f"create table d5.t3 using d5.st tags(1);")
        tdSql.execute(f"insert into d5.t1 values(now, 1);")
        tdSql.execute(f"drop table d5.t1;")

        tdSql.execute(f"drop database d5;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)


    def DropTag(self):
        dbPrefix = "ta_bib_db"
        tbPrefix = "ta_bib_tb"
        mtPrefix = "ta_bib_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"======== test bigint")
        tdSql.execute(
            f"create table if not exists st( ts timestamp, order_id bigint) tags (account_id bigint)"
        )

        tdSql.execute(f"create table t1 using st tags(111)")
        tdSql.execute(f"create table t2 using st tags(222)")

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")
        tdSql.query(
            f"select  account_id,count(*) from st where account_id = 111  group by account_id"
        )

        tdSql.execute(f"drop table t1")

        tdSql.execute(f"create table t1 using st tags(111)")

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")

        tdSql.query(
            f"select account_id,count(*) from st where account_id = 111  group by account_id"
        )

        tdSql.checkRows(1)

        tdLog.info(f"======== test varchar")

        tdSql.execute(f"drop stable st")

        tdSql.execute(
            f"create table if not exists st( ts timestamp, order_id bigint) tags (account_id binary(16))"
        )

        tdSql.execute(f'create table t1 using st tags("aac")')
        tdSql.execute(f'create table t2 using st tags("abc")')

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")
        tdSql.query(
            f'select  account_id,count(*) from st where account_id = "aac"  group by account_id'
        )

        tdSql.execute(f"drop table t1")

        tdSql.execute(f'create table t1 using st tags("aac")')

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")

        tdSql.query(
            f'select account_id,count(*) from st where account_id = "aac"  group by account_id'
        )

        tdSql.checkRows(1)

        tdLog.info(f"====== test empty table")
        tdSql.execute(f"drop table t1")

        tdSql.query(
            f'select account_id,count(*) from st where account_id = "aac"  group by account_id'
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")


    def test_subtable_query(self):
        """Query

        1. Create super tables and child tables
        2. Check results from:
            SHOW TABLES
            SELECT * FROM ins_tables
            SELECT * FROM ins_stables

        Catalog:
            - Table:SubTable

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/stable/metrics.sim
            - 2026-11-20 Alex Merged into 02-SubTables/test_subtable_create.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        dbPrefix = "m_me_db"
        tbPrefix = "m_me_tb"
        mtPrefix = "m_me_mt"

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(dbname=db, drop=True)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {mt} (ts timestamp, speed int) TAGS(sp int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"drop table {mt}")
        tdSql.query(f"show stables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"create table {mt} (ts timestamp, speed int) TAGS(sp int)")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = '{db}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)
        tdSql.checkData(0, 4, 1)

        tdSql.query(f"select * from {mt}")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(1)")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(2)")
        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} using {mt} tags(3)")

        tdSql.query(
            f"select * from information_schema.ins_tables where db_name = '{db}'"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, mt)

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = '{db}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step5")
        i = 0
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now + 1m , 1 )")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now + 1m , 1 )")
        i = 2
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now + 1m , 1 )")

        tdLog.info(f"=============== step6")

        tdSql.query(f"select * from {mt}")
        tdLog.info(f"select * from {mt} ==> {tdSql.getRows()} {tdSql.getData(0,0)}")
        tdSql.checkRows(3)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select * from {mt} where sp = 1")
        tdLog.info(
            f"select * from {mt} where sp = 1 ==> {tdSql.getRows()} {tdSql.getData(0,0)}"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============== step8")
        tdSql.execute(f"drop table {mt}")

        tdLog.info(f"=============== step9")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdSql.query(f"show stables")
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
