
import taos

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        self.ts = 1700638570000  # 2023-11-22T07:36:10.000Z
        os.system("rm -rf %s/b.csv" %self.testcasePath)

        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def create_tb(self):
        result = os.popen("taos -s 'drop database if exists d1;'")
        result = os.popen("taos -s 'create database d1;use d1;CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);'")
        res = result.read()
        if ("OK" not in res):
            tdLog.exit(f"create tb error")

    def check(self):
        self.create_tb()
        tdSql.query(f"use d1;")

        # prepare csv
        tdSql.query(f"Create table d100 using meters tags('California.X', 1);")
        once = 10000
        qtime = 101
        rowNum = qtime * once
        for j in range(qtime):
          sql = "insert into d1.d100 values"
          ts = self.ts + j*once
          for i in range(once):
              sql += f"({ts + i}, {0.3 + (i%10)/100.0}, {210 + i%10}, {10.0 + (i%20)/20.0})"  
          tdSql.query(sql)  
        
        sql1 = "select * from d1.d100 >>'%s/b.csv';" %self.testcasePath
        os.system("taos -s '%s'" %sql1)       

        # auto create when insert
        tdSql.query(f"INSERT INTO d2001 USING meters TAGS ('California.SanFrancisco', 5) FILE '{self.testcasePath}/b.csv';")

        # create before insert 
        tdSql.query(f"Create table d2002 using meters tags('California.X', 6);")
        tdSql.query(f"INSERT INTO d2002 FILE '{self.testcasePath}/b.csv';")

        tdSql.query(f"select tbname,count(*) from meters group by tbname order by tbname;")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, rowNum)
        tdSql.checkData(2, 1, rowNum)

    def run(self):
        tdSql.prepare()
        self.check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
