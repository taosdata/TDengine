
import taos

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")

    def check(self):
        sql = "select 'a;b' as x"
        tdSql.query(f"%s" %sql)
        tdSql.checkRows(1)

        self.checksql('select \\\"a;b\\\" as x\G')
        self.checksql('select \\\"a;b\\\" as x >> temp.txt')

    def run(self):
        tdSql.prepare()
        self.check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
