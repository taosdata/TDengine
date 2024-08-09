from wsgiref.headers import tspecials
from util.log import *
from util.cases import *
from util.sql import *
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

    def orderBySelectFuncTest(self):
        dbname = self.dbname
        
        tdSql.query(f"drop table if exists {dbname}.stb")
        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 int, col2 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(f"create table {dbname}.stb_2 using {dbname}.stb tags('shanghai')")
        
        tdSql.error(f"select * from {dbname}.stb_1 order by last(ts) desc")
        tdSql.error(f"select * from {dbname}.stb_1 order by min(col1) desc")
        tdSql.error(f"select * from {dbname}.stb_1 order by max(col1) desc")
        tdSql.error(f"select * from {dbname}.stb_1 order by top(col1) desc")
        tdSql.error(f"select * from {dbname}.stb_1 order by first(col1) desc")
        tdSql.error(f"select * from {dbname}.stb_1 order by bottom(col1) desc")
        tdSql.error(f"select *, last(ts) as ts from {dbname}.stb order by ts desc")
        
        tdSql.query(f"select * from {dbname}.stb_1 order by col1 desc")
        tdSql.query(f"select * from {dbname}.stb_1 order by ts desc")
        
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.stb_1 values({self.ts + i + 1}, {i+1}, 'taosdata_{i+1}')" )
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.stb_2 values({self.ts + i + 1}, {i+1}, 'taosdata_{i+1}')" )

        tdSql.query(f"select * from {dbname}.stb order by col1 desc")
        tdSql.checkRows(20)
        tdSql.checkData(0, 1, 10)
        
        tdSql.query(f"select * from {dbname}.stb_1 order by col1 asc")
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query(f"select * from {dbname}.stb order by ts asc")
        tdSql.checkRows(20)
        tdSql.checkData(0, 1, 1)
        
        tdSql.error(f"select * from {dbname}.stb order by last(ts) desc")
        tdSql.error(f"select * from {dbname}.stb order by min(col1) desc")
        tdSql.error(f"select * from {dbname}.stb order by max(col1) desc")
        tdSql.error(f"select * from {dbname}.stb order by top(col1) desc")
        tdSql.error(f"select * from {dbname}.stb order by first(col1) desc")
        tdSql.error(f"select * from {dbname}.stb order by bottom(col1) desc")
        tdSql.error(f"select last(col1) from {dbname}.stb order by last(ts) desc")
        tdSql.error(f"select first(col1) from {dbname}.stb order by first(ts) desc")

        tdSql.query(f"select *, last(ts) from {dbname}.stb order by last(ts) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, min(col1) from {dbname}.stb order by min(col1) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.query(f"select *, max(col1) from {dbname}.stb order by max(col1) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, top(col1,  2) from {dbname}.stb order by col1 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, first(col1) from {dbname}.stb order by first(col1) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.query(f"select *, bottom(col1, 3) from {dbname}.stb order by col1 desc")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2)
        tdSql.query(f"select *, last(ts) as tt from {dbname}.stb order by last(ts) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, last(ts) as tt from {dbname}.stb order by tt desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, last(ts) as ts from {dbname}.stb order by last(ts) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, last(ts) from {dbname}.stb order by last(ts) + 2 desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, last(ts) + 2 from {dbname}.stb order by last(ts) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, 2 + last(ts) from {dbname}.stb order by last(ts) + 1 desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, 2 + last(ts) from {dbname}.stb order by 2 + last(ts) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, 2 + last(col1) + 20 from {dbname}.stb order by last(col1) + 1 desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)

        # Function parameters are not supported for the time being.
        tdSql.error(f"select *, last(col1 + 2) from {dbname}.stb order by last(col1) desc")
        # tdSql.query(f"select *, last(col1 + 2) from {dbname}.stb order by last(col1) desc")
        # tdSql.checkRows(1)
        # tdSql.checkData(0, 1, 10)
        

    def run(self):
        dbname = "db"
        tdSql.prepare()
        
        self.inAndNotinTest()
        self.orderBySelectFuncTest()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
