
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
        self.tb1 = 'db.d001'
        self.tb2 = 'db.d002'
        self.tag1 = 'using db.meters(groupId) tags(1)'
        self.tag2 = 'using db.meters(groupId) tags(2)'
        self.file1 = f"{self.testcasePath}/csv/b.csv"
        self.file2 = f"{self.testcasePath}/csv/c.csv"
        os.system(f"mkdir -p {self.testcasePath}/csv" )
        os.system(f"rm -rf {self.file1}" )
        os.system(f"rm -rf {self.file2}" )

        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def check_count(self, rows, records):
        tdSql.execute(f"use db;")
        tdSql.query(f"select tbname,count(*) from meters group by tbname order by tbname;")
        tdSql.checkRows(rows)
        for i in range(rows):
          tdSql.checkData(i, 1, records[i])

    def reset_tb(self):
        tdSql.execute("drop database if exists db;")
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        tdSql.execute("CREATE STABLE db.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")
        tdSql.execute("create table db.d2001 using db.meters(groupId) tags(5);")
        tdSql.execute("create table db.d2002 using db.meters(groupId) tags(6);")

    def check(self):
        # same table, auto create + create
        tdSql.execute(f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb1} {self.tag1} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1}")
        tdSql.checkData(0, 0, self.rowNum2+self.rowNum1)
        tdSql.execute(f"drop table {self.tb1};")

        # same table, create + insert
        tdSql.execute(f"create table {self.tb1} using db.meters(groupId) tags(9);")
        tdSql.execute(f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb1} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1}")
        tdSql.checkData(0, 0, self.rowNum2+self.rowNum1)
        tdSql.execute(f"drop table {self.tb1};")


        # same table, insert + create
        tdSql.execute(f"create table {self.tb1} using db.meters(groupId) tags(10);")
        tdSql.execute(f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb1} {self.tag1} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1}")
        tdSql.checkData(0, 0, self.rowNum2+self.rowNum1)
        tdSql.execute(f"drop table {self.tb1};")


        # same table, insert + insert
        tdSql.execute(f"create table {self.tb1} using db.meters(groupId) tags(10);")
        tdSql.execute(f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb1} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1}")
        tdSql.checkData(0, 0, self.rowNum2+self.rowNum1)
        tdSql.execute(f"drop table {self.tb1};")
        
        # diff table auto create + create
        tdSql.execute(f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb2} {self.tag2} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1};")
        tdSql.checkData(0, 0, self.rowNum1)
        tdSql.query(f"select count(*) from {self.tb2};")
        tdSql.checkData(0, 0, self.rowNum2)
        tdSql.execute(f"drop table {self.tb1};")
        tdSql.execute(f"drop table {self.tb2};")


        # diff table, create + insert
        tdSql.execute(f"create table {self.tb2} using db.meters(groupId) tags(12);")
        tdSql.execute(f"INSERT INTO {self.tb1} {self.tag1} file '{self.file1}' {self.tb2} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1};")
        tdSql.checkData(0, 0, self.rowNum1)
        tdSql.query(f"select count(*) from {self.tb2};")
        tdSql.checkData(0, 0, self.rowNum2)
        tdSql.execute(f"drop table {self.tb1};")
        tdSql.execute(f"drop table {self.tb2};")

        # diff table, insert + create
        tdSql.execute(f"create table {self.tb1} using db.meters(groupId) tags(13);")
        tdSql.execute(f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb2} {self.tag2} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1};")
        tdSql.checkData(0, 0, self.rowNum1)
        tdSql.query(f"select count(*) from {self.tb2};")
        tdSql.checkData(0, 0, self.rowNum2)
        tdSql.execute(f"drop table {self.tb1};")
        tdSql.execute(f"drop table {self.tb2};")

        # diff table, insert + insert
        tdSql.execute(f"create table {self.tb1} using db.meters(groupId) tags(14);")
        tdSql.execute(f"create table {self.tb2} using db.meters(groupId) tags(15);")
        tdSql.execute(f"INSERT INTO {self.tb1} file '{self.file1}' {self.tb2} file '{self.file2}';")
        tdSql.query(f"select count(*) from {self.tb1};")
        tdSql.checkData(0, 0, self.rowNum1)
        tdSql.query(f"select count(*) from {self.tb2};")
        tdSql.checkData(0, 0, self.rowNum2)
        tdSql.execute(f"drop table {self.tb1};")
        tdSql.execute(f"drop table {self.tb2};")

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
        tdSql.error(f"insert into {self.tb1} file '{self.testcasePath}/csv/2k.csv' {self.tb2} values('2021-07-13 14:06:34.630', 10.2, 219, 0.32);")

    def test_bigcsv(self):
        # prepare csv
        print("start csv data prepare")
        once = 10001
        qtime1 = 101
        qtime2 = 100
        self.rowNum1 = qtime1 * once
        self.rowNum2 = qtime2 * once
        self.make_csv(self.file1, once, qtime1, self.ts - 86400000)
        self.make_csv(self.file2, once, qtime2, self.ts)
        print("end csv data prepare")
   
        # auto create + insert

        tdSql.execute(f"INSERT INTO db.d2001 using db.meters(groupId) tags(5) FILE '{self.file1}';")

        # only insert 
        tdSql.execute(f"INSERT INTO db.d2002 FILE '{self.file2}';")

        tdSql.query(f"select count(*) from db.d2001;")
        tdSql.checkData(0, 0, self.rowNum1)
        tdSql.query(f"select count(*) from db.d2002;")
        tdSql.checkData(0, 0, self.rowNum2)
        tdSql.execute(f"INSERT INTO db.d2001 using db.meters(groupId) tags(5) FILE '{self.file1}';")

        # insert directory error
        tdSql.error(f"INSERT INTO db.d2001 FILE '{self.testcasePath}/csv';")



    def run(self):
        tdLog.debug(f"test for TS-4272")
        self.reset_tb()
        self.test_bigcsv()
        self.test_mix()
        self.check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
