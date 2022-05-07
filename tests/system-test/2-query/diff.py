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
        self.perfix = 'dev'
        self.tables = 10

    def insertData(self):
        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, col int) tags(dev nchar(50))")

        for i in range(self.tables):
            tdSql.execute("create table %s%d using st tags(%d)" % (self.perfix, i, i))
            rows = 15 + i
            for j in range(rows):
                tdSql.execute("insert into %s%d values(%d, %d)" %(self.perfix, i, self.ts + i * 20 * 10000 + j * 10000, j))

    def run(self):
        tdSql.prepare()

        tdSql.execute('''create table stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table stb_1 using stb tags('beijing')")
        tdSql.execute("insert into stb_1 values(%d, 0, 0, 0, 0, 0.0, 0.0, False, ' ', ' ', 0, 0, 0, 0)" % (self.ts - 1))
        
        # diff verifacation 
        tdSql.query("select diff(col1) from stb_1")
        tdSql.checkRows(0)
        
        tdSql.query("select diff(col2) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col3) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col4) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col5) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col6) from stb_1")
        tdSql.checkRows(0)

        for i in range(self.rowNum):
            tdSql.execute("insert into stb_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))                                
        
        tdSql.error("select diff(ts) from stb")
        tdSql.error("select diff(ts) from stb_1")
        tdSql.error("select diff(col1) from stb")
        tdSql.error("select diff(col2) from stb")
        tdSql.error("select diff(col3) from stb")
        tdSql.error("select diff(col4) from stb")
        tdSql.error("select diff(col5) from stb")
        tdSql.error("select diff(col6) from stb")
        tdSql.error("select diff(col7) from stb")     
        tdSql.error("select diff(col7) from stb_1")               
        tdSql.error("select diff(col8) from stb") 
        tdSql.error("select diff(col8) from stb_1")
        tdSql.error("select diff(col9) from stb")        
        tdSql.error("select diff(col9) from stb_1")
        tdSql.error("select diff(col11) from stb_1")
        tdSql.error("select diff(col12) from stb_1")
        tdSql.error("select diff(col13) from stb_1")
        tdSql.error("select diff(col14) from stb_1")
        tdSql.error("select diff(col11) from stb")
        tdSql.error("select diff(col12) from stb")
        tdSql.error("select diff(col13) from stb")
        tdSql.error("select diff(col14) from stb")

        tdSql.query("select ts,diff(col1),ts from stb_1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select ts,diff(col1),ts from stb group by tbname")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select ts,diff(col1),ts from stb_1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select ts,diff(col1),ts from stb group by tbname")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 1, "2018-09-17 09:00:00.000")
        tdSql.checkData(0, 3, "2018-09-17 09:00:00.000")
        tdSql.checkData(9, 0, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 1, "2018-09-17 09:00:00.009")
        tdSql.checkData(9, 3, "2018-09-17 09:00:00.009")

        tdSql.query("select diff(col1) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col2) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col3) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col4) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col5) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col6) from stb_1")
        tdSql.checkRows(10)

        self.insertData()

        tdSql.query("select diff(col) from st group by tbname")
        tdSql.checkRows(185)

        tdSql.error("select diff(col) from st group by dev")        

        tdSql.error("select diff(col) from st group by col")
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())