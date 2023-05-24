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
        tdSql.execute(f"CREATE STABLE `stb5` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `t_11` USING `stb5` (`t1`) TAGS (1)")

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-10 09:30:47.722', 10.30000, 100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-10 09:30:56.383', 12.30000, 100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-10 09:48:55.778', 13.30000, 100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-10 09:51:50.821', 9.30000,  100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-10 09:58:07.162', 9.30000,  100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-10 13:41:16.075', 9.30000,  100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-13 14:12:58.318', 21.00000, 100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-13 14:13:21.328', 1.10000,  100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-13 14:35:24.258', 1.30000,  100)")
        tdSql.execute(f"INSERT INTO `t_11` VALUES ('2023-05-13 16:56:49.033', 1.80000,  100)")

        tdLog.debug("insert data ............ [OK]")

    def run(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        tdLog.printNoPrefix("======== test TS-3404")

        tdSql.query(f"select _irowts, interp(ip_value) from t_11 range('2023-05-13 14:00:00', '2023-05-13 15:00:00') every(300s) fill(linear);")
        tdSql.checkRows(13)

        tdSql.checkData(0,  0, '2023-05-13 14:00:00.000')
        tdSql.checkData(1,  0, '2023-05-13 14:05:00.000')
        tdSql.checkData(2,  0, '2023-05-13 14:10:00.000')
        tdSql.checkData(3,  0, '2023-05-13 14:15:00.000')
        tdSql.checkData(4,  0, '2023-05-13 14:20:00.000')
        tdSql.checkData(5,  0, '2023-05-13 14:25:00.000')
        tdSql.checkData(6,  0, '2023-05-13 14:30:00.000')
        tdSql.checkData(7,  0, '2023-05-13 14:35:00.000')
        tdSql.checkData(8,  0, '2023-05-13 14:40:00.000')
        tdSql.checkData(9,  0, '2023-05-13 14:45:00.000')
        tdSql.checkData(10, 0, '2023-05-13 14:50:00.000')
        tdSql.checkData(11, 0, '2023-05-13 14:55:00.000')
        tdSql.checkData(12, 0, '2023-05-13 15:00:00.000')

        tdSql.checkData(0,  1, 20.96512)
        tdSql.checkData(1,  1, 20.97857)
        tdSql.checkData(2,  1, 20.99201)
        tdSql.checkData(3,  1, 1.114917)
        tdSql.checkData(4,  1, 1.160271)
        tdSql.checkData(5,  1, 1.205625)
        tdSql.checkData(6,  1, 1.250978)
        tdSql.checkData(7,  1, 1.296333)
        tdSql.checkData(8,  1, 1.316249)
        tdSql.checkData(9,  1, 1.333927)
        tdSql.checkData(10, 1, 1.351607)
        tdSql.checkData(11, 1, 1.369285)
        tdSql.checkData(12, 1, 1.386964)


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
