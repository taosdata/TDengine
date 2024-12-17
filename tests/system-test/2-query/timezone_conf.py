
from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
from util.cluster import *

class TDTestCase:
    updatecfgDict = {"clientCfg" : {'timezone': 'UTC', 'charset': 'gbk'}, 'timezone': 'UTC-8', 'charset': 'gbk'}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def timezone_check(self, timezone, sql):
        tdSql.query(sql)
        rows = tdSql.getRows()
        for i in range(rows):
            if tdSql.getData(i, 0) == "timezone" :
                if tdSql.getData(i, 1).find(timezone) == -1:
                    tdLog.exit("show timezone:%s != %s"%(tdSql.getData(i, 1), timezone))

    def charset_check(self, charset, sql):
        tdSql.query(sql)
        rows = tdSql.getRows()
        for i in range(rows):
            if tdSql.getData(i, 0) == "charset" :
                if tdSql.getData(i, 1).find(charset) == -1:
                    tdLog.exit("show charset:%s != %s"%(tdSql.getData(i, 1), charset))

    def run(self):
        self.charset_check('gbk', "show local variables")
        self.charset_check('gbk', "show dnode 1 variables")
        self.timezone_check('UTC', "show local variables")
        # time.sleep(50)
        self.timezone_check('UTC-8', "show dnode 1 variables")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
