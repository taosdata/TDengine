

import sys
import taos
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:

    def init(self, conn, logSql,replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

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

    def run(self):  
        tdSql.prepare()

        tdLog.info("-----------test for stop taosd before alter wal level-----------")
        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.stop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)

        tdLog.info(" alter wal level from 0 to 1")
        self.alterWalLevel(1)
        self.insertData()
        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.stop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)

        tdLog.info(" alter wal level from 0 to 2")
        self.alterWalLevel(2)
        self.insertData()
        tdDnodes.forcestop(1)
        tdDnodes.start(1)


        tdLog.info("-----------test for kill taosd before alter wal level-----------")
        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.forcestop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)

        tdLog.info(" alter wal level from 0 to 1")
        self.alterWalLevel(1)
        tdDnodes.forcestop(1)
        tdDnodes.start(1)

        tdLog.info("create database wal_level = 0 and insert data")
        self.preData()
        tdDnodes.forcestop(1)
        time.sleep(2)
        tdLog.info("restart taosd")
        tdDnodes.start(1)

        tdLog.info(" alter wal level from 0 to 2")
        self.alterWalLevel(2)
        tdDnodes.forcestop(1)
        tdDnodes.start(1)

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

        
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
