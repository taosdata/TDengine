from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        
    def prepare_data(self):
        tdSql.execute("drop database if exists test;")
        
        tdCases.taosBenchmarkExec("-t 2 -n 1000000 -b int,float,nchar -y")
        
        while True:
            tdSql.query("select ts from test.d0;")
            num1 = tdSql.queryRows
            tdSql.query("select ts from test.d1;")
            num2 = tdSql.queryRows
            if num1 == 1000000 and num2 == 1000000:
                break
            tdLog.info(f"waiting for data ready, d0: {num1}, d1: {num2}")
            time.sleep(1)

    def ts5803(self):           
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 join test.d1 on d0.ts=d1.ts;")
        num1 = tdSql.queryRows
        
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)
        
        tdSql.query("select d0.ts from test.d0 join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)
        
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 left join test.d1 on d0.ts=d1.ts;")
        num1 = tdSql.queryRows
        
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 left join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)
        
        tdSql.query("select d0.ts from test.d0 left join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)

    def run(self):
        self.prepare_data()
        self.ts5803()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
