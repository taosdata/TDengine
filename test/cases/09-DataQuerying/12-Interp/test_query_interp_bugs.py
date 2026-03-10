import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils.log import tdLog
from new_test_framework.utils.sql import tdSql

class TestTS_3404:
    hostname = socket.gethostname()

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    #
    # ------------------- test_TS_3404.py ----------------
    #
    def create_tables(self):
        tdSql.execute(f"CREATE STABLE `stb5` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `t_11` USING `stb5` (`t1`) TAGS (1)")

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        sql = "INSERT INTO `t_11` VALUES ('2023-05-10 09:30:47.722', 10.30000, 100)"
        sql += ", ('2023-05-10 09:30:56.383', 12.30000, 100)"
        sql += ", ('2023-05-10 09:48:55.778', 13.30000, 100)"
        sql += ", ('2023-05-10 09:51:50.821', 9.30000,  100)"
        sql += ", ('2023-05-10 09:58:07.162', 9.30000,  100)"
        sql += ", ('2023-05-10 13:41:16.075', 9.30000,  100)"
        sql += ", ('2023-05-13 14:12:58.318', 21.00000, 100)"
        sql += ", ('2023-05-13 14:13:21.328', 1.10000,  100)"
        sql += ", ('2023-05-13 14:35:24.258', 1.30000,  100)"
        sql += ", ('2023-05-13 16:56:49.033', 1.80000,  100)"
        tdSql.execute(sql)

    def do_ts_3404(self):
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

        print("do TS-3404 ............................ [passed]")

    #
    # ------------------- main ----------------
    #
    def test_query_inerp_bugs(self):
        """Interp bugs

        1. Verify bug TS-3404 (timestamp precision cause wrong window function result)
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_TS_3404.py

        """
        self.do_ts_3404()
    