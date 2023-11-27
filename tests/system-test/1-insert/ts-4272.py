
import csv
from datetime import datetime

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
        self.tb1 = 'd001'
        self.tb2 = 'd002'
        self.tag1 = 'using meters(groupId) tags(1)'
        self.tag2 = 'using meters(groupId) tags(2)'
        self.file1 = f"{self.testcasePath}/b.csv"
        self.file2 = f"{self.testcasePath}/c.csv"

        os.system("rm -rf %s/b.csv" %self.testcasePath)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def check_count(self, rows, records):
        tdSql.execute(f"use d1;")
        tdSql.query(f"select tbname,count(*) from meters group by tbname order by tbname;")
        tdSql.checkRows(rows)
        for i in range(rows):
          tdSql.checkData(i, 1, records[i])

    def reset_tb(self):
        # create database and tables
        # os.system("taos -s 'drop database if exists d1;'")
        # os.system("taos -s 'create database d1;use d1;CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);'")
        # os.system(f"taos -s 'use d1;Create table d2001 using meters(groupId) tags(5);'")
        # res = os.system(f"taos -s 'use d1;Create table d2002 using meters(groupId) tags(6);'")
        # if (0 != res):
        #    tdLog.exit(f"create tb error")

        tdSql.execute("drop database if exists d1;")
        tdSql.execute("create database d1;")
        tdSql.execute("use d1;")
        tdSql.execute("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")
        tdSql.execute("Create table d2001 using meters(groupId) tags(5);")
        tdSql.execute("Create table d2002 using meters(groupId) tags(6);")

    def test(self, sql):
        sql = "use d1;" + sql
        res = os.system(f'taos -s "{sql}"')
        # if (0 != res):
        #    tdLog.exit(f"taos sql error")


    def check(self):
        # same table, auto create + create
        sql = f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb1} {self.tag1} file '{self.file2}';"
        self.test(sql)

        # same table, create + insert
        sql = f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb1} file '{self.file2}';"
        self.test(sql)

        # same table, insert + create
        sql = f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb1} {self.tag1} file '{self.file2}';"
        self.test(sql)

        # same table, insert + insert
        sql = f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb1} file '{self.file2}';"
        self.test(sql)

        # diff table auto create + create
        sql = f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb2} {self.tag2} file '{self.file2}';"
        self.test(sql)

        # diff table, create + insert
        sql = f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb2} file '{self.file2}';"
        self.test(sql)

        # diff table, insert + create
        sql = f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb2} {self.tag2} file '{self.file2}';"
        self.test(sql)

        # diff table, insert + insert
        sql = f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb2} file '{self.file2}';"
        self.test(sql)

        # bigNum = 1010000
        # self.check_count(5, [2100, 2100, bigNum, bigNum, bigNum])

        result = os.popen("taos -s 'select count(*) from d1.%s'" %self.tb1)
        res = result.read()
        if (f"OK" in res):
            tdLog.info(f"check count success")

    def make_csv(self, filepath, once, qtime, startts):
        f = open(filepath, 'w')
        with f:
          writer = csv.writer(f)
          for j in range(qtime):
            ts = startts + j*once
            rows = []
            for i in range(once):
                rows.append([ts + i, 0.3 + (i%10)/100.0, 210 + i%10, 10.0 + (i%20)/20.0])
            writer.writerows(rows)
        f.close()
        print(datetime.now(), filepath, " ready!")

    def test_mix(self):
        #forbid use both value and file in one insert
        result = os.popen(f"insert into {self.tb1} file '{self.testcasePath}/csv/2k.csv' {self.tb2} values('2021-07-13 14:06:34.630', 10.2, 219, 0.32);")
        res = result.read()
        if (f"error" in res):
            tdLog.info(f"forbid success")

    def test_bigcsv(self):
        # prepare csv
        print("start csv data prepare")
        once = 10000
        qtime1 = 101
        qtime2 = 100
        rowNum1 = qtime1 * once
        rowNum2 = qtime2 * once
        self.make_csv(self.file1, once, qtime1, self.ts - 86400000)
        self.make_csv(self.file2, once, qtime2, self.ts)
        print("end csv data prepare")
   
        # auto create + insert
        sql = f"INSERT INTO d2001 using meters(groupId) tags(5) FILE '{self.file1}';"
        self.test(sql)

        # only insert 
        sql = f"INSERT INTO d2002 FILE '{self.file2}';"
        self.test(sql)

        #tdSql.execute(f"use d1;")
        tdSql.query(f"select tbname,count(*) from meters group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, rowNum1)
        tdSql.checkData(1, 1, rowNum2)

    def run(self):
        tdSql.prepare()
        self.reset_tb()
        self.test_bigcsv()
        self.test_mix()
        self.check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
