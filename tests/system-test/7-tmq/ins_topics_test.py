
import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:
    hostname = socket.gethostname()
    # rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    # updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
    #print ("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def test(self):
        tdLog.info("create database, stb, ctb")
        tdSql.execute("create database if not exists db1 vgroups 4 wal_retention_period 3600")
        tdSql.execute("create table if not exists db1.st(ts timestamp, c1 int, c2 bool, c3 tinyint, c4 double, c5 nchar(8)) tags(t1 int, t2 float, t3 binary(4))")
        tdSql.execute("create table if not exists db1.nt(ts timestamp, c1 smallint, c2 float, c3 binary(64), c4 bigint)")
        tdSql.execute("create table if not exists db1.st1 using db1.st tags(1, 9.3, \"st1\")")

        tdLog.info("create topic")
        tdSql.execute("create topic topic_1 as database db1")
        tdSql.execute("create topic topic_2 as stable db1.st")
        tdSql.execute("create topic topic_3 as select * from db1.nt")
        tdSql.execute("create topic topic_4 as select ts,c3,c5 from db1.st")

        tdSql.query("select * from information_schema.ins_topics")
        tdSql.checkRows(4)
        # tdSql.checkData(0, 1, 51)
        # tdSql.checkData(0, 4, 940)
        # tdSql.checkData(1, 1, 23)
        # tdSql.checkData(1, 4, None)

        tdLog.printNoPrefix("======== test case end ...... ")

    def run(self):
        self.test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
