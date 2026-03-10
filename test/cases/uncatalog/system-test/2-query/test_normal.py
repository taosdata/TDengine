from wsgiref.headers import tspecials
from new_test_framework.utils import tdLog, tdSql, tdCom

class TestNormal:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.dbname = "db"
        cls.rowNum = 10
        cls.ts = 1537146000000

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
        
        tdSql.query(f"select * from {dbname}.stb_1  order by ts")
        rows = tdSql.queryRows
        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, 2) or (1<2) order by ts")
        tdSql.checkRows(rows)
        
        tdSql.query(f"select * from (select * from {dbname}.stb_1 where col1 in (1, 9, 3) or (1<2) order by ts)")
        tdSql.checkRows(rows)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, 2) and (1<2) order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        
        tdSql.query(f"select * from {dbname}.stb_1 where col1 in (1, 9, 3) and (1<2) order by ts")
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
        tdSql.error(f"select * from {dbname}.stb order by count(*) desc")
                
        tdSql.error(f"select *, last(col1 + 2) from {dbname}.stb order by last(col1) desc")

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
        tdSql.query(f"select *, last(col1) + 2 from {dbname}.stb order by last(col1) desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        
        tdSql.query(f"select *, last(col1) + 2 from {dbname}.stb order by cast(last(col1) as bigint) desc")
        tdSql.checkRows(1)  
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, cast(last(col1) + 2 as bigint) from {dbname}.stb order by cast(last(col1) as bigint) desc")
        tdSql.checkRows(1)  
        tdSql.checkData(0, 1, 10)
        tdSql.query(f"select *, cast(last(col1) + 2 as bigint) from {dbname}.stb order by abs(last(col1)) desc")
        tdSql.checkRows(1)  
        tdSql.checkData(0, 1, 10)
        
        tdSql.query(f"select last(*) from {dbname}.stb order by last(ts)")
        tdSql.checkRows(1) 
        
        tdSql.query(f"select last_row(*) from {dbname}.stb order by last_row(ts)")
        tdSql.checkRows(1) 
        
        tdSql.query(f"select first(col1) from {dbname}.stb order by ts")
        tdSql.checkRows(1) 
        
        tdSql.query(f"select first(col1) from {dbname}.stb partition by tbname order by ts")
        tdSql.checkRows(2) 
        
        tdSql.query(f"select _wstart, _wend from stb_1 interval(3a) order by min(col1%4)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.003")
        tdSql.checkData(1, 0, "2018-09-17 09:00:00.006")
        tdSql.checkData(2, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(3, 0, "2018-09-17 09:00:00.009")
        
        tdSql.query(f"select _wstart, _wend, min(col1%4) from stb_1 interval(3a) order by min(col1%4)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.003")
        tdSql.checkData(1, 0, "2018-09-17 09:00:00.006")
        tdSql.checkData(2, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(3, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        
        
        tdSql.error(f"select *, cast(last(col1) + 2 as bigint) from  stb order by derivative(col1, 1s) desc;")
        tdSql.error(f"select *, cast(last(col1) + 2 as bigint) from  stb order by derivative(col1, 1s, 0) desc;")
        tdSql.error(f"select * from stb order by last_row(ts);")
        tdSql.error(f"select * from stb order by mode(col1);")
        
        tdSql.error(f"select first(ts), col1 from {dbname}.stb interval(10a) order by diff(ts);")
        tdSql.error(f"select first(ts), col1 from {dbname}.stb interval(10a) order by statecount(col1, \"LT\", 1);")
        tdSql.error(f"select first(ts), col1 from {dbname}.stb interval(10a) order by csum(col1);")
        tdSql.error(f"select first(ts), col1 from {dbname}.stb interval(10a) order by mavg(col1);")
        tdSql.error(f"select first(ts), col1 from {dbname}.stb interval(10a) order by tail(col1, 2);")
        tdSql.error(f"select first(ts), col1 from {dbname}.stb interval(10a) order by unique(col1);")

               
    def test_normal(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        dbname = "db"
        tdSql.prepare()
        
        self.timeZoneTest()
        self.inAndNotinTest()
        self.orderBySelectFuncTest()

        #tdSql.close()

