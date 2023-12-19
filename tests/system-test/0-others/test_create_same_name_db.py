import time
import os
import platform
import taos
import threading
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *


class TDTestCase:
    """This test case is used to veirfy TD-25762
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.db_name = "db"

    def run(self):
        try:
            # create same name database multiple times
            for i in range(100):
                tdLog.debug(f"round {str(i+1)} create database {self.db_name}")
                tdSql.execute(f"create database {self.db_name}")
                tdLog.debug(f"round {str(i+1)} drop database {self.db_name}")
                tdSql.execute(f"drop database {self.db_name}")
        except Exception as ex:
            tdLog.exit(str(ex))

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
