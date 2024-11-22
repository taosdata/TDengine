
from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
from util.cluster import *

class TDTestCase:
    updateCfgDict = {"clientCfg" : {'timezone': 'UTC'} , 'timezone': 'UTC-8'}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def timezone_check(self, timezone, sql):
        tdSql.execute(sql)
        rows = tdSql.getRows()
        for i in range(rows):
            if tdSql.getData(i, 0) == "timezone" :
                if tdSql.getData(i, 1).find(timezone) == -1:
                    tdLog.exit("show timezone:%s != %s"%(tdSql.getData(i, 1),timezone))

    def run(self):
        self.timezone_check('UTC', "show local variables")
        self.timezone_check('UTC-8', "show dnode 1 variables")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
