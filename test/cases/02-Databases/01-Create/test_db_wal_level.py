from new_test_framework.utils import tdLog, tdSql, tdDnodes
import sys
import taos
import os
import time

class TestWalLevelSkip:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def preData(self):
        tdSql.execute("drop database if exists db0;")
        tdSql.execute("create database db0 KEEP 30 vgroups 1 buffer 3 wal_level 0;")
        tdSql.execute("create table if not exists db0.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned);")
        tdSql.execute("create table db0.ct1 using db0.stb tags(1000);")
        tdSql.execute("create table db0.ct2 using db0.stb tags(2000);")
        tdSql.execute("create table if not exists db0.ntb (ts timestamp, c1 int, c2 float, c3 double) ;")
        tdSql.query("show db0.stables;")
        tdSql.execute("insert into db0.ct1 values(now+0s, 10, 2.0, 3.0);")
        tdSql.execute("insert into db0.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);")
        tdSql.execute("insert into db0.ntb values(now+2s, 10, 2.0, 3.0);")

    def insertData(self):
        tdSql.execute("insert into db0.ct1 values(now+0s, 10, 2.0, 3.0);")
        tdSql.execute("insert into db0.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);")
        tdSql.execute("insert into db0.ntb values(now+2s, 10, 2.0, 3.0);")

    def createSubTableAndInsertData(self):
        tdSql.execute("create table db0.ct1 using db0.stb tags(1000);")
        tdSql.execute("create table db0.ct2 using db0.stb tags(2000);")
        tdSql.execute("create table if not exists db0.ntb (ts timestamp, c1 int, c2 float, c3 double) ;")
        tdSql.execute("insert into db0.ct1 values(now+0s, 10, 2.0, 3.0);")
        tdSql.execute("insert into db0.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);")
        tdSql.execute("insert into db0.ntb values(now+2s, 10, 2.0, 3.0);")


    def alterWalLevel(self,level):
        tdSql.execute("alter database db0 wal_level %d;"%level)

    def test_wal_level_skip(self):
        """Options: wal level 

        1. create database wal_level = 0 and insert data
        2. stop/kill taosd before alter wal level
        3. restart taosd
        4. alter wal level from 0 to 1/2
        5. insert data
        6. stop/kill taosd after alter wal level
        7. restart taosd

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_wal_level_skip.py

        """
        tdSql.prepare()

        tdLog.info("-----------test for stop taosd before alter wal level-----------")
        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.stop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info(" alter wal level from 0 to 1")
        self.alterWalLevel(1)
        time.sleep(1)
        self.insertData()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.stop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info(" alter wal level from 0 to 2")
        self.alterWalLevel(2)
        time.sleep(1)
        self.insertData()
        tdDnodes.forcestop(1)
        tdDnodes.start(1)
        time.sleep(1)


        tdLog.info("-----------test for kill taosd before alter wal level-----------")
        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.forcestop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info(" alter wal level from 0 to 1")
        self.alterWalLevel(1)
        time.sleep(1)
        tdDnodes.forcestop(1)
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.forcestop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info(" alter wal level from 0 to 2")
        self.alterWalLevel(2)
        time.sleep(1)
        tdDnodes.forcestop(1)
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info("-----------test for stop taosd after alter wal level-----------")
        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdLog.info(" alter wal level from 0 to 1")
        self.alterWalLevel(1)
        time.sleep(1)
        self.insertData()
        tdDnodes.stop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        time.sleep(1)


        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdLog.info(" alter wal level from 0 to 2")
        self.alterWalLevel(2)
        time.sleep(1)
        self.insertData()
        tdDnodes.stop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        time.sleep(1)

        tdLog.info("-----------test for kill taosd after alter wal level-----------")
        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdLog.info(" alter wal level from 0 to 1")
        self.alterWalLevel(1)
        time.sleep(1)
        self.insertData()
        tdDnodes.forcestop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        time.sleep(1)


        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdLog.info(" alter wal level from 0 to 2")
        self.alterWalLevel(2)
        time.sleep(1)
        self.insertData()
        tdDnodes.forcestop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)
        tdLog.success(f"{__file__} successfully executed")

