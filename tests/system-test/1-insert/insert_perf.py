import taos
import sys
import random
import time
import csv

from datetime import datetime

from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        
        self.testcasePath = os.path.split(__file__)[0]
        self.testcasefilename = os.path.split(__file__)[-1]
        self.file1 = f"{self.testcasePath}/int.csv"
        self.file2 = f"{self.testcasePath}/double.csv"
        self.ts = 1700638570000  # 2023-11-22T07:36:10.000Z
        self.database = "db1"
        self.tb1 = "t1"
        self.tb2 = "t2"
        self.stable1 = "st1"
        self.stable2 = "st2"
        self.tag1 = f'using {self.stable1}(groupId) tags(1)'
        self.tag2 = f'using {self.stable2}(groupId) tags(2)'
        self.once = 1000
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    def prepare_db(self):
        #tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database if not exists {self.database}")
        tdSql.execute(f"use {self.database}")
        tdSql.execute(f"create stable if not exists {self.stable1} (ts timestamp, q_bigint bigint , i1 int, i2 int, i3 int, i4 int, i5 int, i6 int, i7 int, i8 int, i9 int) tags (location binary(64), groupId int);")
        tdSql.execute(f"create stable if not exists {self.stable2} (ts timestamp, q_double double, f1 double, f2 double, f3 double, f4 double, f5 double, f6 double, f7 double, f8 double, f9 double) tags (location binary(64), groupId int);")
        tdSql.execute(f"drop table if exists {self.tb1};")
        tdSql.execute(f"drop table if exists {self.tb2};")
        tdSql.execute(f"create table {self.tb1} {self.tag1};")
        tdSql.execute(f"create table {self.tb2} {self.tag2};")
    
    def make_csv(self, filepath, once, isint):
        f = open(filepath, 'w')
        with f:
          writer = csv.writer(f)
          rows = []
          for i in range(once):
            r = []
            if isint:
              for k in range(10):
                  r.append(random.randint(-2147483648, 2147483647))
            else:
              for k in range(10):
                  r.append(random.randint(-2147483648, 2147483647) + random.random())
            rows.append(r)
          writer.writerows(rows)
        f.close()
        print(f"{filepath} ready!")

    def test_insert(self, tbname, qtime, startts, filepath, isint):
        f = open(filepath, 'r')
        rows = []
        with f:
          reader = csv.reader(f, delimiter=',', quotechar='|')
          for row in reader:
             rows.append(row)
        f.close()
        self.once = len(rows)

        sum = 0
        for j in range(qtime):
          offset = j * self.once
          ts = startts + offset
          sql = f"insert into {self.database}.{tbname} values"  
          for i in range(self.once):
              r = rows[i]
              sql +=f"({ts + i},'{r[0]}','{r[1]}','{r[2]}','{r[3]}','{r[4]}','{r[5]}','{r[6]}','{r[7]}','{r[8]}','{r[9]}')"
          
          t1 = time.time()
          tdSql.execute(f"{sql};", 1)
          t2 = time.time() 
          #print(f"{t2} insert test {j}.")
          #print(sql)
          sum += t2 - t1

        sum = sum
        tbtype = "10 double col/per row"
        if isint:
           tbtype =  "10 int col/per row"
        print(f" insert {self.once} * {qtime} rows: {sum} s, {tbtype}")

        # tdSql.query(f"select count(*) from {self.database}.{tbname};")
        # tdSql.checkData(0, 0, once*qtime)

    def run(self):
        tdSql.prepare(replica = self.replicaVar)
        # self.make_csv(self.file1, self.once, True)
        # self.make_csv(self.file2, self.once, False)
        self.prepare_db()
        self.test_insert(self.tb1, 10000, self.ts, self.file1, True)
        self.test_insert(self.tb2, 10000, self.ts, self.file2, False)
        self.test_insert(self.tb2, 10000, self.ts, self.file1, False)

        self.test_insert(self.tb1, 10000, self.ts, self.file2, True)


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
