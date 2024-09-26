from wsgiref.headers import tspecials
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        
        self.dbname = "db"
        self.rowNum = 10
        self.ts = 1537146000000
        
    def inAndNotinTest(self):
        dbname = self.dbname
        
        tdSql.query(f"select 1 in (1, 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True)
        
        tdSql.query(f"select 1 in (2, 3)")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False)
        
        tdSql.query(f"select 1 not in (2, 3)")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True) 
        
        tdSql.query(f"select 1 not in (1)")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False)
        
        tdSql.query(f"select 1 in (1, null)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True)
        
        tdSql.query(f"select 1 in (2, null)")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False)   # 1 not in (2, null) is NULL?
 
        tdSql.query(f"select 1 not in (1, null)")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False) 
               
        tdSql.query(f"select 1 not in (2, null)")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False)    # 1 not in (2, null) is NULL?
        
        tdSql.query(f"select 1 not in (null)")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, False)    # 1 not in (null) is NULL?
               
        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 int, col2 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(f"create table {dbname}.stb_2 using {dbname}.stb tags('shanghai')")
        
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.stb_1 values({self.ts + i + 1}, {i+1}, 'taosdata_{i+1}')" )
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.stb_2 values({self.ts + i + 1}, {i+1}, 'taosdata_{i+1}')" )

        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, 2) order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, 9, 3) order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 9)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, 9, 3, 'xy') order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 9)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, '9', 3) order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 9)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, 9, 3, null) order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 9)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col2 in (1, 'taosdata_1', 3, null) order by ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col2  not in (1, 'taosdata_1', 3, null) order by ts")
        tdSql.checkRows(0)
        
        tdSql.execute(f"insert into {dbname}.stb_1 values({self.ts + self.rowNum + 1}, {self.rowNum+1}, null)" )
        tdSql.execute(f"insert into {dbname}.stb_2 values({self.ts + self.rowNum + 1}, {self.rowNum+1}, null)" )
        
        tdSql.query(f"select * from {dbname}.stb_1 where col2 in (1, 'taosdata_1', 3, null) order by ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col2 not in (1, 'taosdata_1', 3, null) order by ts")
        tdSql.checkRows(0)
        
        tdSql.query(f"select * from {dbname}.stb where loc in ('beijing', null)")
        tdSql.checkRows(11)
        
        tdSql.query(f"select * from {dbname}.stb where loc in ('shanghai', null)")
        tdSql.checkRows(11)
        
        tdSql.query(f"select * from {dbname}.stb where loc in ('shanghai', 'shanghai', null)")
        tdSql.checkRows(11)
        
        tdSql.query(f"select * from {dbname}.stb where loc in ('beijing', 'shanghai', null)")
        tdSql.checkRows(22)
        
        tdSql.query(f"select * from {dbname}.stb where loc not in ('beijing', null)")
        tdSql.checkRows(0)
        
        tdSql.query(f"select * from {dbname}.stb where loc not in ('shanghai', 'shanghai', null)")
        tdSql.checkRows(0)

    def timeZoneTest(self):
        dbname = self.dbname
        tdsql1 = tdCom.newTdSqlWithTimezone(timezone="Asia/Shanghai")
        tdsql1.execute(f'create table {dbname}.tzt(ts timestamp, c1 int)')
        tdsql1.execute(f'insert into {dbname}.tzt values({self.ts}, 1)')
        tdsql1.query(f"select * from {dbname}.tzt")
        tdsql1.checkRows(1)
        tdsql1.checkData(0, 0, "2018-09-17 09:00:00")
        
        tdsql2 = tdCom.newTdSqlWithTimezone(timezone="UTC")
        tdsql2.query(f"select * from {dbname}.tzt")
        tdsql2.checkRows(1)
        # checkData:The expected date and time is the local time zone of the machine where the test case is executed.
        tdsql2.checkData(0, 0, "2018-09-17 09:00:00")

        tdsql2.execute(f'insert into {dbname}.tzt values({self.ts + 1000}, 2)')
        tdsql2.query(f"select * from {dbname}.tzt order by ts")
        tdsql2.checkRows(2)
        tdsql2.checkData(0, 0, "2018-09-17 09:00:00")
        tdsql2.checkData(1, 0, "2018-09-17 09:00:01")
        
        tdsql2 = tdCom.newTdSqlWithTimezone(timezone="Asia/Shanghai")
        tdsql2.query(f"select * from {dbname}.tzt order by ts")
        tdsql2.checkRows(2)
        tdsql2.checkData(0, 0, "2018-09-17 09:00:00")
        tdsql2.checkData(1, 0, "2018-09-17 09:00:01")

    def run(self):
        dbname = "db"
        tdSql.prepare()
        
        self.timeZoneTest()
        # self.inAndNotinTest()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())

tdCases.addLinux(__file__, TDTestCase())
