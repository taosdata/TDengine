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
        
    def ts6136(self): 
        start = 1500000000000
        tdSql.execute("drop database if exists test1;")
        
        tdCases.taosBenchmarkExec(f"-d test1 -t 3 -n 1000000 -s {start} -y")
        
        while True:
            tdSql.query("select count(*) from test1.meters;")
            tdSql.checkRowCol(0, 0)
            if tdSql.queryResult[0][0] == 3000000: 
                tdLog.info(f"ts6136 data ready, sum: {tdSql.queryResult[0][0]}")
                break
            tdLog.info(f"waiting for data ready, sum: {tdSql.queryResult[0][0]}")
            time.sleep(1)

        tdSql.query(f"insert into test1.d0 values({start + 962000}, 1, 1, 1);")
        tdSql.query(f"insert into test1.d0 values({start + 961000}, 1, 1, 1);")
        tdSql.query(f"insert into test1.d0 values({start + 960000}, 1, 1, 1);")
        tdSql.query(f"insert into test1.d0 values({start + 602000}, 1, 1, 1);")
        tdSql.query(f"insert into test1.d1 values({start + 602000}, 1, 1, 1);")
        tdSql.query(f"insert into test1.d0 values({start + 302000}, 1, 1, 1);")
        tdSql.query(f"insert into test1.d0 values({start +   2000}, 1, 1, 1);")
        tdSql.query(f"insert into test1.d0 values({start +      0}, 1, 1, 1);")
            
            # Query with limit 20 and store the results
        tdSql.query("select tbname, ts, current from test1.meters where current = 1 and voltage = 1 and phase = 1 order by ts desc limit 8;")
        tdSql.checkRows(8)
        
        tbname_list = [row[0] for row in tdSql.queryResult]
        ts_list = [row[1] for row in tdSql.queryResult]
        
        # Test with different limits
        for limit in range(1, 10):
            row = min(8, limit)
            tdSql.query(f"select tbname, ts, current from test1.meters where current = 1 and voltage = 1 and phase = 1 order by ts desc limit {limit};")
            tdSql.checkRows(row)
            result = tdSql.queryResult[:row]
            
            for i in range(min(10, len(result))):
                tdSql.checkData(i, 0, tbname_list[i])
                tdSql.checkData(i, 1, ts_list[i])
    
        tdLog.info("All tests passed for ts6136")
        

    def run(self):
        self.prepare_data()
        self.ts5803()
        self.ts6136()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
