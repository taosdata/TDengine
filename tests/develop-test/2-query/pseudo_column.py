import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TS-3904/TS-3005] pseudo column test case
        ''' 
        return
    
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self._conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use pseudo_col")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists pseudo_col")
        tdSql.execute("create database if not exists pseudo_col")
        tdSql.execute('use pseudo_col')
        tdSql.execute('create table st(ts timestamp, f int) tags (t int)')
        tdSql.execute("insert into ct1 using st tags(1) values('2023-10-10 14:10:00', 1)('2023-10-10 14:10:01', 11)")
        tdSql.execute("insert into ct2 using st tags(2) values('2023-10-10 14:10:02', 2)('2023-10-10 14:10:03', 22)")

        tdSql.query('select tbname from (select tbname from st) order by tbname')
        tdSql.checkCols(1)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(1, 0, 'ct1')
        tdSql.checkData(2, 0, 'ct2')
        tdSql.checkData(2, 0, 'ct2')

        tdSql.query('select `tbname` from (select tbname from st) order by tbname')
        tdSql.checkCols(1)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(1, 0, 'ct1')
        tdSql.checkData(2, 0, 'ct2')
        tdSql.checkData(2, 0, 'ct2')

        tdSql.query('select `tbname` from (select tbname from st) order by tbname')
        tdSql.checkCols(1)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(1, 0, 'ct1')
        tdSql.checkData(2, 0, 'ct2')
        tdSql.checkData(2, 0, 'ct2')

        tdSql.query('select tbname from (select st.tbname from st) order by tbname')
        tdSql.checkCols(1)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(1, 0, 'ct1')
        tdSql.checkData(2, 0, 'ct2')
        tdSql.checkData(2, 0, 'ct2')
        
        tdSql.query('select * from (select tbname, avg(f) from st partition by tbname) a partition by a.tbname order by a.tbname');       
        tdSql.checkRows(2)
        tdSql.checkCols(2)
        tdSql.checkData(0, 0, 'ct1');
        tdSql.checkData(0, 1, 6.0);
        tdSql.checkData(1, 0, 'ct2');
        tdSql.checkData(1, 1, 12.0);
                         
        tdSql.error('select tbname from (select * from st)')
        tdSql.error('select st.tbname from (select st.tbname from st)')
        tdSql.error('select `st.tbname` from (select st.tbname from st) order by tbname')
        
        tdSql.query('select _wstart, _wend, _wduration, c from (select _wstart, _wend, _wduration, count(*) as c from st interval(1s)) order by _wstart')
        tdSql.checkCols(4)
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, '2023-10-10 14:10:01')
        tdSql.checkData(0, 3, 1)
        
        tdSql.error('select _wstart, _wend, _wduration, c from (select count(*) as c from st) order by _wstart')
        
        tdSql.query("select _irowts, if2 from (select _irowts, interp(f) as if2 from st range('2023-10-10 14:10:00', '2023-10-10 14:10:10') every(1s) fill(value, 8))")
        tdSql.checkRows(11)
        tdSql.checkData(9, 1, 8);
        tdSql.execute('drop database pseudo_col')
        
        tdSql.error("select _irowts, if2 from (select interp(f) as if2 from st range('2023-10-10 14:10:00', '2023-10-10 14:10:10') every(1s) fill(value, 8))")
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
