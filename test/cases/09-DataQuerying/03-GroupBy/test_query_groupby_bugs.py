import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils.log import tdLog
from new_test_framework.utils.sql import tdSql

class TestTS_3821:
    hostname = socket.gethostname()

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
    #
    # ------------------- test_ts_3821.py ----------------
    #
    def create_tables(self):
        tdSql.execute(f'''CREATE STABLE `s_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators`
                          (`ts` TIMESTAMP, `event_time` TIMESTAMP, `wbli` DOUBLE, `vrc` DOUBLE, `csd` DOUBLE,
                          `oiv` DOUBLE, `tiv` DOUBLE, `flol` DOUBLE, `capacity` DOUBLE, `ispc` NCHAR(50)) TAGS
                          (`device_identification` NCHAR(64))''')
        tdSql.execute(f''' CREATE TABLE `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators`
                           USING `s_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` (`device_identification`)
                           TAGS ("1000000000001001")''')

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:35.685','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:36.239','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:37.290','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:38.414','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:39.471','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")

        tdLog.debug("insert data ............ [OK]")

    def do_ts_3821(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        tdLog.printNoPrefix("======== test TS-3821")

        tdSql.query(f'''select count(*),device_identification from s_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators
                        where 1=1 and device_identification ='1000000000001001' group by device_identification;''')
        tdSql.checkRows(1)
        tdSql.checkCols(2)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, "1000000000001001")

        tdSql.query(f'''select count(*),device_identification from t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators
                        group by device_identification;''')
        tdSql.checkRows(1)
        tdSql.checkCols(2)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, "1000000000001001")

        print("do TS-3821 ............................ [passed]")

    #
    # ------------------- main ----------------
    #
    def test_query_groupby_bugs(self):
        """Group by bugs

        1. Verify bug TS-3821 (tag value not show with group by query)
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_ts_3821.py

        """
        self.do_ts_3821()