import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *

PRIVILEGES_ALL      = "ALL"
PRIVILEGES_READ     = "READ"
PRIVILEGES_WRITE    = "WRITE"

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def create_user_current(self):
        for i in range(self.users_count):
            tdSql.execute(f"create user test{i} pass 'taosdata{i}' ")

    def grant_user_privileges(self, dbname):
        return "grant "


    def run(self):
        self.users_count = 5
        tdSql.execute("")




    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
