import platform
import os
from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestWriteOutOfOrderData:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_write_out_of_order_data(self):
        """Write expired data

        1. Write out-of-order and expired data, including:
            Data distributed across multiple files
            Data existing in multiple blocks
            Data present in both memory and files
        2. Restart the dnode
        3. Query data integrity

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/import/basic.sim
            - 2025-8-12 Simon Guan Migrated from tsim/import/commit.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/import_commit1.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/import_commit2.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/import_commit3.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/import_file.sim
            - 2025-8-12 Simon Guan Migrated from tsim/import/large.sim
            - 2025-8-12 Simon Guan Migrated from tsim/import/replica1.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/import.sim

        """
        
        self.ImportBasic()
        tdStream.dropAllStreamsAndDbs()
        self.ImportCommit()
        tdStream.dropAllStreamsAndDbs()
        self.ImportCommit1()
        tdStream.dropAllStreamsAndDbs()
        self.ImportCommit2()
        tdStream.dropAllStreamsAndDbs()
        self.ImportCommit3()
        tdStream.dropAllStreamsAndDbs()
        self.ImportFile()
        tdStream.dropAllStreamsAndDbs()
        self.ImportLarge()
        tdStream.dropAllStreamsAndDbs()
        self.ImportReplica1()
        tdStream.dropAllStreamsAndDbs()
        self.ParserImport()
        tdStream.dropAllStreamsAndDbs()

    def ImportBasic(self):
        tdSql.execute(f"create database ibadb")
        tdSql.execute(f"use ibadb")
        tdSql.execute(f"create table tb(ts timestamp, i int)")

        tdLog.info(f"================= step1")

        tdSql.execute(f"import into tb values(1564641710000, 10000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(1)

        tdLog.info(f"================= step2")
        tdSql.execute(f"insert into tb values(1564641708000, 8000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(2)

        tdLog.info(f"================= step3")
        tdSql.execute(f"insert into tb values(1564641720000, 20000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(3)

        tdLog.info(f"================= step4")
        tdSql.execute(f"import into tb values(1564641708000, 8000)")
        tdSql.execute(f"import into tb values(1564641715000, 15000)")
        tdSql.execute(f"import into tb values(1564641730000, 30000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(5)

        tdLog.info(f"================= step5")
        tdSql.execute(f"insert into tb values(1564641708000, 8000)")
        tdSql.execute(f"insert into tb values(1564641714000, 14000)")
        tdSql.execute(f"insert into tb values(1564641725000, 25000)")
        tdSql.execute(f"insert into tb values(1564641740000, 40000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(8)

        tdLog.info(f"================= step6")
        tdSql.execute(f"import into tb values(1564641707000, 7000)")
        tdSql.execute(f"import into tb values(1564641712000, 12000)")
        tdSql.execute(f"import into tb values(1564641723000, 23000)")
        tdSql.execute(f"import into tb values(1564641734000, 34000)")
        tdSql.execute(f"import into tb values(1564641750000, 50000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(13)

        tdLog.info(f"================= step7")
        tdSql.execute(f"import into tb values(1564641707001, 7001)")
        tdSql.execute(f"import into tb values(1564641712001, 12001)")
        tdSql.execute(f"import into tb values(1564641723001, 23001)")
        tdSql.execute(f"import into tb values(1564641734001, 34001)")
        tdSql.execute(f"import into tb values(1564641750001, 50001)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(18)

        tdLog.info(f"================= step8")
        tdSql.execute(f"insert into tb values(1564641708002, 8002)")
        tdSql.execute(f"insert into tb values(1564641714002, 14002)")
        tdSql.execute(f"insert into tb values(1564641725002, 25002)")
        tdSql.execute(f"insert into tb values(1564641900000, 200000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(22)

        tdLog.info(f"================= step9 only insert last one")
        tdSql.execute(
            f"import into tb values(1564641705000, 5000)(1564641718000, 18000)(1564642400000, 700000)"
        )
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(25)

        tdLog.info(f"================= step10")
        tdSql.execute(
            f"import into tb values(1564641705000, 5000)(1564641718000, 18000)(1564642400000, 70000)"
        )
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(25)

        tdLog.info(f"================= step11")
        tdSql.execute(f"import into tb values(1564642400000, 700000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(25)

        tdLog.info(f"================= step12")
        tdSql.execute(
            f"import into tb values(1564641709527, 9527)(1564641709527, 9528)"
        )
        tdSql.query(f"select * from tb;")
        tdLog.info(f"rows=> {tdSql.getRows()})")
        tdSql.checkRows(26)

        tdLog.info(f"================= step13")
        tdSql.execute(
            f"import into tb values(1564641709898, 9898)(1564641709897, 9897)"
        )
        tdSql.query(f"select * from tb;")
        tdLog.info(f"rows=> {tdSql.getRows()})")
        tdSql.checkRows(28)

    def ImportCommit(self):
        tdLog.info(f"========= step1")
        tdSql.execute(f"create database ic1db duration 7;")
        tdSql.execute(f"create table ic1db.tb(ts timestamp, s int);")
        tdSql.execute(f"insert into ic1db.tb values(now-30d, -30);")
        tdSql.execute(f"insert into ic1db.tb values(now-20d, -20);")
        tdSql.execute(f"insert into ic1db.tb values(now-10d, -10);")
        tdSql.execute(f"insert into ic1db.tb values(now-5d, -5);")
        tdSql.execute(f"insert into ic1db.tb values(now+1m, 1);")
        tdSql.execute(f"insert into ic1db.tb values(now+2m, 2);")
        tdSql.execute(f"insert into ic1db.tb values(now+3m, 6);")
        tdSql.execute(f"insert into ic1db.tb values(now+4m, 8);")
        tdSql.execute(f"insert into ic1db.tb values(now+5m, 10);")
        tdSql.execute(f"insert into ic1db.tb values(now+6m, 12);")
        tdSql.execute(f"insert into ic1db.tb values(now+7m, 14);")
        tdSql.execute(f"insert into ic1db.tb values(now+8m, 16);")
        tdSql.query(f"select * from ic1db.tb;")
        tdSql.checkRows(12)

        tdLog.info(f"========= step2")
        tdSql.execute(f"create database ic2db duration 7;")
        tdSql.execute(f"create table ic2db.tb(ts timestamp, s int);")
        tdSql.execute(f"insert into ic2db.tb values(now, 0);")
        tdSql.execute(f"import into ic2db.tb values(now-30d, -30);")
        tdSql.execute(f"import into ic2db.tb values(now-20d, -20);")
        tdSql.execute(f"import into ic2db.tb values(now-10d, -10);")
        tdSql.execute(f"import into ic2db.tb values(now-5d, -5);")
        tdSql.execute(f"import into ic2db.tb values(now+1m, 1);")
        tdSql.execute(f"import into ic2db.tb values(now+2m, 2);")
        tdSql.execute(f"import into ic2db.tb values(now+3m, 6);")
        tdSql.execute(f"import into ic2db.tb values(now+4m, 8);")
        tdSql.execute(f"import into ic2db.tb values(now+5m, 10);")
        tdSql.execute(f"import into ic2db.tb values(now+6m, 12);")
        tdSql.execute(f"import into ic2db.tb values(now+7m, 14);")
        tdSql.execute(f"import into ic2db.tb values(now+8m, 16);")
        tdSql.query(f"select * from ic2db.tb;")
        tdSql.checkRows(13)

        tdLog.info(f"========= step3")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"========= step4")
        tdSql.query(f"select * from ic2db.tb;")
        tdSql.checkRows(13)

        tdSql.query(f"select * from ic1db.tb;")
        tdSql.checkRows(12)

    def ImportCommit1(self):
        dbPrefix = "ic_db"
        tbPrefix = "ic_tb"
        stbPrefix = "ic_stb"
        tbNum = 1
        # $rowNum = 166
        rowNum = 1361
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")

        i = 0
        ts = ts0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, c1 int)")
        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
            x = x + 1

        tdLog.info(f"====== tables created")

        ts = ts0 + delta
        ts = ts + 1
        tdSql.execute(f"import into {tb} values ( {ts} , -1)")
        tdSql.query(f"select count(*) from {tb}")
        res = rowNum + 1
        tdSql.checkData(0, 0, res)

    def ImportCommit2(self):
        dbPrefix = "ic_db"
        tbPrefix = "ic_tb"
        stbPrefix = "ic_stb"
        tbNum = 1
        rowNum = 166
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")

        i = 0
        ts = ts0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, c1 int)")
        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
            x = x + 1

        tdLog.info(f"====== tables created")

        ts = ts0 + delta
        ts = ts + 1
        tdSql.execute(f"import into {tb} values ( {ts} , -1)")
        tdSql.query(f"select count(*) from {tb}")
        res = rowNum + 1
        tdSql.checkData(0, 0, res)

    def ImportCommit3(self):
        dbPrefix = "ic_db"
        tbPrefix = "ic_tb"
        stbPrefix = "ic_stb"
        tbNum = 1
        rowNum = 582
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"reset query cache")
        i = 0
        ts = ts0
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {tb} (ts timestamp, c1 int, c2 int, c3 int, c4 int, c5 int)"
        )
        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(
                f"insert into {tb} values ( {ts} , {x} , {x} , {x} , {x} , {x} )"
            )
            x = x + 1

        tdLog.info(f"====== tables created")

        ts = ts + 1
        tdSql.execute(f"insert into {tb} values ( {ts} , -1, -1, -1, -1, -1)")
        ts = ts0 + delta
        ts = ts + 1
        tdSql.execute(f"import into {tb} values ( {ts} , -2, -2, -2, -2, -2)")

        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.query(f"select count(*) from {tb}")
        res = rowNum + 2
        tdSql.checkData(0, 0, res)

    def ImportFile(self):
        tdSql.execute(f"drop database if exists indb")
        tdSql.execute(f"create database if not exists indb")
        tdSql.execute(f"use indb")

        inFileName = os.path.join(os.path.dirname(__file__), "data.sql")
        if platform.system() == "Windows":
            inFileName = inFileName.replace("\\", "\\\\")

        #os.system("tools/gendata.sh")

        tdSql.execute(
            f"create table stbx (ts TIMESTAMP, collect_area NCHAR(12), device_id BINARY(16), imsi BINARY(16),  imei BINARY(16),  mdn BINARY(10), net_type BINARY(4), mno NCHAR(4), province NCHAR(10), city NCHAR(16), alarm BINARY(2)) tags(a int, b binary(12))"
        )

        tdSql.execute(
            f"create table tbx (ts TIMESTAMP, collect_area NCHAR(12), device_id BINARY(16), imsi BINARY(16),  imei BINARY(16),  mdn BINARY(10), net_type BINARY(4), mno NCHAR(4), province NCHAR(10), city NCHAR(16), alarm BINARY(2))"
        )
        tdLog.info(f"====== create tables success, starting insert data")

        tdSql.execute(f'insert into tbx file "{inFileName}"')
        tdSql.execute(f'import into tbx file "{inFileName}"')

        tdSql.query(f"select count(*) from tbx")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.execute(f"drop table tbx")

        tdSql.execute(f'insert into tbx using stbx tags(1,"abc") file "{inFileName}"')
        tdSql.execute(f'insert into tbx using stbx tags(1,"abc") file "{inFileName}"')

        tdSql.query(f"select count(*) from tbx")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.execute(f"drop table tbx")
        tdSql.execute(f'insert into tbx using stbx(b) tags("abcf") file "{inFileName}"')

        tdSql.query(f"select ts,a,b from tbx")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2020-01-01 01:01:01")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, "abcf")

        os.system(f"rm -f {inFileName}")

    def ImportLarge(self):
        tdSql.execute(f"create database ir1db duration 7")
        tdSql.execute(f"use ir1db")
        tdSql.execute(f"create table tb(ts timestamp, i bigint)")

        tdLog.info(f"================= step1")
        tdSql.execute(f"import into tb values(1520000010000, 1520000010000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdLog.info(f"================= step2")
        tdSql.execute(f"insert into tb values(1520000008000, 1520000008000)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(2)

        tdLog.info(f"================= step3")
        tdSql.execute(f"insert into tb values(1520000020000, 1520000020000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3)

        tdLog.info(f"================= step4")
        tdSql.execute(f"import into tb values(1520000009000, 1520000009000)")
        tdSql.execute(f"import into tb values(1520000015000, 1520000015000)")
        tdSql.execute(f"import into tb values(1520000030000, 1520000030000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(6)

        tdLog.info(f"================= step5")
        tdSql.execute(f"insert into tb values(1520000008000, 1520000008000)")
        tdSql.execute(f"insert into tb values(1520000014000, 1520000014000)")
        tdSql.execute(f"insert into tb values(1520000025000, 1520000025000)")
        tdSql.execute(f"insert into tb values(1520000040000, 1520000040000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(9)

        tdLog.info(f"================= step6")
        tdSql.execute(f"import into tb values(1520000007000, 1520000007000)")
        tdSql.execute(f"import into tb values(1520000012000, 1520000012000)")
        tdSql.execute(f"import into tb values(1520000023000, 1520000023000)")
        tdSql.execute(f"import into tb values(1520000034000, 1520000034000)")
        tdSql.execute(f"import into tb values(1520000050000, 1520000050000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(14)

        tdLog.info(f"================== dnode restart")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use ir1db")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(14)

        tdLog.info(f"================= step7")
        tdSql.execute(f"import into tb values(1520000007001, 1520000007001)")
        tdSql.execute(f"import into tb values(1520000012001, 1520000012001)")
        tdSql.execute(f"import into tb values(1520000023001, 1520000023001)")
        tdSql.execute(f"import into tb values(1520000034001, 1520000034001)")
        tdSql.execute(f"import into tb values(1520000050001, 1520000050001)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(19)

        tdLog.info(f"================= step8")
        tdSql.execute(f"insert into tb values(1520000008002, 1520000008002)")
        tdSql.execute(f"insert into tb values(1520000014002, 1520000014002)")
        tdSql.execute(f"insert into tb values(1520000025002, 1520000025002)")
        tdSql.execute(f"insert into tb values(1520000060000, 1520000060000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(23)

        tdLog.info(f"================= step9")
        # 1520000000000
        # sql import into tb values(now-30d, 7003)
        # sql import into tb values(now-20d, 34003)
        # sql import into tb values(now-10d, 34003)
        # sql import into tb values(now-5d, 34003)
        # sql import into tb values(now+1d, 50001)
        # sql import into tb values(now+2d, 50001)
        # sql import into tb values(now+6d, 50001)
        # sql import into tb values(now+8d, 50002)
        # sql import into tb values(now+10d, 50003)
        # sql import into tb values(now+12d, 50004)
        # sql import into tb values(now+14d, 50001)
        # sql import into tb values(now+16d, 500051)

        tdSql.execute(f"import into tb values(1517408000000, 1517408000000)")
        tdSql.execute(f"import into tb values(1518272000000, 1518272000000)")
        tdSql.execute(f"import into tb values(1519136000000, 1519136000000)")
        tdSql.execute(f"import into tb values(1519568000000, 1519568000000)")
        tdSql.execute(f"import into tb values(1519654400000, 1519654400000)")
        tdSql.execute(f"import into tb values(1519827200000, 1519827200000)")
        tdSql.execute(f"import into tb values(1520345600000, 1520345600000)")
        tdSql.execute(f"import into tb values(1520691200000, 1520691200000)")
        tdSql.execute(f"import into tb values(1520864000000, 1520864000000)")
        tdSql.execute(f"import into tb values(1521900800000, 1521900800000)")
        tdSql.execute(f"import into tb values(1523110400000, 1523110400000)")
        tdSql.execute(f"import into tb values(1521382400000, 1521382400000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(35)

        tdLog.info(f"================= step10")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use ir1db")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(35)

        tdLog.info(f"================= step11")

        # sql import into tb values(now-50d, 7003) (now-48d, 7003) (now-46d, 7003) (now-44d, 7003) (now-42d, 7003)
        tdSql.execute(
            f"import into tb values(1515680000000, 1) (1515852800000, 2) (1516025600000, 3) (1516198400000, 4) (1516371200000, 5)"
        )
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(40)

        tdLog.info(f"================= step12")
        # 1520000000000
        # sql import into tb values(now-19d, -19) (now-18d, -18) (now-17d, -17) (now-16d, -16) (now-15d, -15) (now-14d, -14) (now-13d, -13) (now-12d, -12) (now-11d, -11)
        tdSql.execute(
            f"import into tb values(1518358400000, 6) (1518444800000, 7) (1518531200000, 8) (1518617600000, 9) (1518704000000, 10) (1518790400000, 11) (1518876800000, 12) (1518963200000, 13) (1519049600000, 14)"
        )
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(49)

        tdLog.info(f"================= step14")
        # 1520000000000
        # sql import into tb values(now-48d, -48)
        # sql import into tb values(now-38d, -38)
        # sql import into tb values(now-28d, -28)

        tdSql.execute(f"import into tb values(1515852800001, -48)")
        tdSql.execute(f"import into tb values(1516716800000, -38)")
        tdSql.execute(f"import into tb values(1517580800000, -28)")

        tdSql.query(f"select * from tb;")
        tdSql.checkRows(52)

    def ImportReplica1(self):
        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== insert_drop.sim")
        i = 0
        db = "iddb"
        stb = "stb"

        tdSql.prepare(dbname=db)
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags(t1 int)")

        i = 0
        ts = ts0
        while i < 10:
            tb = "tb" + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"====== tables created")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use {db}")
        tdSql.execute(f"drop table tb5")
        i = 0
        while i < 4:
            tb = "tb" + str(i)
            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table tb5 using {stb} tags(5)")
        tdSql.query(f"select * from tb5")
        tdLog.info(f"{tdSql.getRows()}) should be 0")
        tdSql.checkRows(0)

    def ParserImport(self):
        dbPrefix = "impt_db"
        tbPrefix = "tb"
        stbPrefix = "stb"
        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        tdLog.info(f"========== import.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table tb (ts timestamp, c1 int, c2 timestamp)")
        tdSql.execute(f"insert into tb values ('2019-05-05 11:30:00.000', 1, now)")
        tdSql.execute(f"insert into tb values ('2019-05-05 12:00:00.000', 1, now)")
        tdSql.execute(f"import into tb values ('2019-05-05 11:00:00.000', -1, now)")
        tdSql.execute(f"import into tb values ('2019-05-05 11:59:00.000', -1, now)")
        tdSql.execute(f"import into tb values ('2019-05-04 08:00:00.000', -1, now)")
        tdSql.execute(f"import into tb values ('2019-05-04 07:59:00.000', -1, now)")

        tdSql.query(f"select * from tb")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2019-05-04 07:59:00")
        tdSql.checkData(1, 0, "2019-05-04 08:00:00")
        tdSql.checkData(2, 0, "2019-05-05 11:00:00")
        tdSql.checkData(3, 0, "2019-05-05 11:30:00")
        tdSql.checkData(4, 0, "2019-05-05 11:59:00")
        tdSql.checkData(5, 0, "2019-05-05 12:00:00")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        tdSql.connect("root")

        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from tb")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2019-05-04 07:59:00")
        tdSql.checkData(1, 0, "2019-05-04 08:00:00")
        tdSql.checkData(2, 0, "2019-05-05 11:00:00")
        tdSql.checkData(3, 0, "2019-05-05 11:30:00")
        tdSql.checkData(4, 0, "2019-05-05 11:59:00")
        tdSql.checkData(5, 0, "2019-05-05 12:00:00")
