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
        tdSql.execute(f'''CREATE STABLE `dwd_log_master` (`ts` TIMESTAMP, `dim_ip` NCHAR(64)) TAGS (`group_id` BIGINT, `st_hour` NCHAR(2), `org_id` NCHAR(32),
                          `dev_manufacturer_name` NCHAR(64), `dev_manufacturer_id` INT, `dev_category_name` NCHAR(64), `dev_category_id` INT, `dev_feature_name` NCHAR(64),
                          `dev_feature_id` INT, `dev_ip` NCHAR(64), `black_list` TINYINT, `white_list` TINYINT)''')
        tdSql.execute(f'''CREATE TABLE `dwd_log_master_475021043` USING `dwd_log_master` (`group_id`, `st_hour`, `org_id`, `dev_manufacturer_name`, `dev_manufacturer_id`,
                          `dev_category_name`, `dev_category_id`, `dev_feature_name`, `dev_feature_id`, `dev_ip`, `black_list`, `white_list`) TAGS
                          (475021043, "14", NULL, NULL, NULL, NULL, NULL, NULL, NULL, "172.18.22.230", NULL, NULL)''')

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:30.000','192.168.192.102')")
        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:31.000','172.18.23.249')")
        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:32.000','192.168.200.231')")
        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:33.000','172.18.22.231')")
        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:34.000','192.168.210.231')")
        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:35.000','192.168.192.100')")
        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:36.000','192.168.192.231')")
        tdSql.execute(f"INSERT INTO `dwd_log_master_475021043` VALUES ('2023-06-26 14:38:37.000','172.18.23.231')")

        tdLog.debug("insert data ............ [OK]")

    def run(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        tdLog.printNoPrefix("======== test TS-3581")

        for i in range(100):
          tdSql.query(f"select first(ts), last(ts), count(*) from dwd_log_master;")
          tdSql.checkRows(1)
          print(tdSql.queryResult)
          tdSql.checkData(0, 0, '2023-06-26 14:38:30.000')
        return


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
