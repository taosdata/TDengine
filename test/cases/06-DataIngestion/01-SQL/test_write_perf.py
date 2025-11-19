from new_test_framework.utils import tdLog, tdSql
import random
import os
import time
import csv

class TestInsertPerf:
    def setup_class(cls):
        
        cls.testcasePath = os.path.split(__file__)[0]
        cls.testcasefilename = os.path.split(__file__)[-1]
        cls.file1 = f"{cls.testcasePath}/int.csv"
        cls.file2 = f"{cls.testcasePath}/double.csv"
        cls.file3 = f"{cls.testcasePath}/d+.csv"
        cls.file4 = f"{cls.testcasePath}/uint.csv"
        cls.ts = 1700638570000  # 2023-11-22T07:36:10.000Z
        cls.database = "db1"
        cls.tb1 = "t1"
        cls.tb2 = "t2"
        cls.tb3 = "t3"
        cls.once = 1000
        tdLog.debug(f"start to excute {__file__}")

    def prepare_db(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")
        tdSql.execute(f"create table {self.tb1} (ts timestamp, i0 bigint , i1 bigint, i2 bigint, i3 bigint, i4 bigint, i5 bigint, i6 bigint, i7 bigint, i8 bigint, i9 bigint)")
        tdSql.execute(f"create table {self.tb2} (ts timestamp, f0 double, f1 double, f2 double, f3 double, f4 double, f5 double, f6 double, f7 double, f8 double, f9 double)")
        tdSql.execute(f"create table {self.tb3} (ts timestamp, i0 int unsigned , i1 int unsigned, i2 int unsigned, i3 int unsigned, i4 int unsigned, i5 int unsigned, i6 int unsigned, i7 int unsigned, i8 int unsigned, i9 int unsigned)")

    def make_csv(self, once, intype):
        filepath = self.file1
        if intype == 2:
           filepath = self.file2
        elif intype == 3:
           filepath = self.file3
        elif intype == 4:
           filepath = self.file4

        f = open(filepath, 'w')
        with f:
          writer = csv.writer(f)
          rows = []
          for i in range(once):
            r = []
            if intype == 1:
              for k in range(10):
                  r.append(random.randint(-2147483648, 2147483647))
            elif intype == 2:
              for k in range(10):
                  r.append(random.randint(-2147483648, 2147483646) + random.random())
            elif intype == 3:
              for k in range(10):
                  r.append(random.randint(0, 4294967294) + random.random())
            else:
              for k in range(10):
                  r.append(random.randint(0, 4294967295))
            rows.append(r)
          writer.writerows(rows)
        f.close()
        print(f"{filepath} ready!")

    def check_insert(self, tbname, qtime, startts, intype, outtype):
        filepath = self.file1
        dinfo = "int"
        if intype == 2:
           filepath = self.file2
           dinfo = "double"
        elif intype == 3:
           filepath = self.file3
           dinfo = "+double"
        elif intype == 4:
           filepath = self.file4
           dinfo = "uint"
           
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
        tbinfo = "10 bigint col/per row"
        if outtype == 2:
           tbinfo =  "10 double col/per row"
        elif outtype == 3:
           tbinfo =  "10 uint col/per row"
        print(f" insert {self.once} * {qtime} rows: {sum} s, {dinfo} -> {tbinfo}")

        # tdSql.query(f"select count(*) from {self.database}.{tbname};")
        # tdSql.checkData(0, 0, once*qtime)

    def test_insert_perf(self):
        """ Write performance (obsolete)

        1. Test write performance for bigint data type
        2. Test write performance for double data type
        3. Test write performance for uint   data type

        Since: v3.0.0.0

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_perf.py
  
        """

        tdSql.prepare(replica = self.replicaVar)
        # self.make_csv(self.once, 1)
        # self.make_csv(self.once, 2)
        # self.make_csv(self.once, 3)
        # self.make_csv(self.once, 4)
        
        self.prepare_db()
        self.check_insert(self.tb1, 1000, self.ts-10000000, 1, 1)
        self.check_insert(self.tb2, 1000, self.ts-10000000, 2, 2)
        self.check_insert(self.tb3, 1000, self.ts-10000000, 4, 3)
        self.check_insert(self.tb2, 1000, self.ts, 1, 2)
        
        self.check_insert(self.tb1, 1000, self.ts, 2, 1)
        self.check_insert(self.tb3, 1000, self.ts, 3, 3)
        
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
