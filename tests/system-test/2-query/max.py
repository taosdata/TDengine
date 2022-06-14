from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
    def max_check_stb_and_tb_base(self):
        tdSql.prepare()
        intData = []        
        floatData = []
        tdSql.execute('''create table stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned, 
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute("create table stb_1 using stb tags('beijing')")
        for i in range(self.rowNum):
            tdSql.execute(f"insert into stb_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
            intData.append(i + 1)            
            floatData.append(i + 0.1)
        for i in ['ts','col11','col12','col13']:
            for j in ['db.stb','stb','db.stb_1','stb_1']:
                tdSql.error(f'select max({i} from {j} )')

        for i in range(1,11):
            for j in ['db.stb','stb','db.stb_1','stb_1']:
                tdSql.query(f"select max(col{i}) from {j}")
                if i<9:
                    tdSql.checkData(0, 0, np.max(intData))
                elif i>=9:
                    tdSql.checkData(0, 0, np.max(floatData))
        tdSql.query("select max(col1) from stb_1 where col2<=5")
        tdSql.checkData(0,0,5)
        tdSql.query("select max(col1) from stb where col2<=5")
        tdSql.checkData(0,0,5)
        tdSql.execute('drop database db')
        
    def max_check_ntb_base(self):
        tdSql.prepare()
        intData = []        
        floatData = []
        tdSql.execute('''create table ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned, 
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20))''')
        for i in range(self.rowNum):
            tdSql.execute(f"insert into ntb values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
            intData.append(i + 1)            
            floatData.append(i + 0.1)
        for i in ['ts','col11','col12','col13']:
            for j in ['db.ntb','ntb']:
                tdSql.error(f'select max({i} from {j} )')
        for i in range(1,11):
            for j in ['db.ntb','ntb']:
                tdSql.query(f"select max(col{i}) from {j}")
                if i<9:
                    tdSql.checkData(0, 0, np.max(intData))
                elif i>=9:
                    tdSql.checkData(0, 0, np.max(floatData))
        tdSql.query("select max(col1) from ntb where col2<=5")
        tdSql.checkData(0,0,5)
        tdSql.execute('drop database db')
    
    def run(self):
        self.max_check_stb_and_tb_base()
        self.max_check_ntb_base()

        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
