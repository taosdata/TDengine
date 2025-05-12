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

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

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

    def create_tables(self):
        tdSql.execute("create database if not exists dbus precision 'us'")
        tdSql.execute("create database if not exists dbns precision 'ns'")

        tdSql.execute("use dbus")

        tdSql.execute(f"CREATE STABLE `stb_us` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_us` USING `stb_us` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_us` USING `stb_us` (`t1`) TAGS (2)")

        tdSql.execute("use dbns")

        tdSql.execute(f"CREATE STABLE `stb_ns` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_ns` USING `stb_ns` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_ns` USING `stb_ns` (`t1`) TAGS (2)")

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        tdSql.execute(f"INSERT INTO `dbus`.`ctb1_us` VALUES ('2023-07-01 00:00:00.000', 10.30000, 100)")
        tdSql.execute(f"INSERT INTO `dbus`.`ctb2_us` VALUES ('2023-08-01 00:00:00.000', 20.30000, 200)")

        tdSql.execute(f"INSERT INTO `dbns`.`ctb1_ns` VALUES ('2023-07-01 00:00:00.000', 10.30000, 100)")
        tdSql.execute(f"INSERT INTO `dbns`.`ctb2_ns` VALUES ('2023-08-01 00:00:00.000', 20.30000, 200)")

        tdLog.debug("insert data ............ [OK]")

    def run(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        tdLog.printNoPrefix("======== test TS-3311")

        # test ns
        tdSql.query(f"select _wstart, _wend, count(*) from `dbns`.`stb_ns` interval(1n)")
        tdSql.checkRows(2)

        tdSql.checkData(0,  0, '2023-07-01 00:00:00.000000000')
        tdSql.checkData(1,  0, '2023-08-01 00:00:00.000000000')

        tdSql.checkData(0,  1, '2023-08-01 00:00:00.000000000')
        tdSql.checkData(1,  1, '2023-09-01 00:00:00.000000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbns`.`stb_ns` interval(12n)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbns`.`stb_ns` interval(1y)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000000')


        ## test us
        tdSql.query(f"select _wstart, _wend, count(*) from `dbus`.`stb_us` interval(1n)")
        tdSql.checkRows(2)

        tdSql.checkData(0,  0, '2023-07-01 00:00:00.000000')
        tdSql.checkData(1,  0, '2023-08-01 00:00:00.000000')

        tdSql.checkData(0,  1, '2023-08-01 00:00:00.000000')
        tdSql.checkData(1,  1, '2023-09-01 00:00:00.000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbus`.`stb_us` interval(12n)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbus`.`stb_us` interval(1y)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
