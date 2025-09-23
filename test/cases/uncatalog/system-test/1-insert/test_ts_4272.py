
from new_test_framework.utils import tdLog, tdSql
import csv
import os
import platform
from datetime import datetime

class TestTs4272:
    def setup_class(cls):
        cls.testcasePath = os.path.split(__file__)[0]
        cls.testcasefilename = os.path.split(__file__)[-1]
        cls.ts = 1700638570000  # 2023-11-22T07:36:10.000Z
        cls.db = 'db1'
        cls.tb1 = 'd001'
        cls.tb2 = 'd002'
        cls.stable0 = "meters"
        cls.stable1 = "stb_1"
        cls.stable2 = "stb_null"
        cls.tag1 = f'using {cls.stable0}(groupId) tags(1)'
        cls.tag2 = f'using {cls.stable0}(groupId) tags(2)'
        cls.file1 = f"{cls.testcasePath}/b.csv"
        cls.file2 = f"{cls.testcasePath}/c.csv"
        if platform.system().lower() == 'windows':
            cls.file1 = cls.file1.replace("\\","\\\\")
            cls.file2 = cls.file2.replace("\\","\\\\")
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql), logSql)

    def check_count(self, rows, records):
        tdSql.query(f"select tbname,count(*) from {self.stable0} group by tbname order by tbname;")
        tdSql.checkRows(rows)
        for i in range(rows):
          tdSql.checkData(i, 1, records[i])

    def reset_tb(self):
        # create database and tables
        tdSql.execute(f"drop database if exists {self.db};")
        tdSql.execute(f"create database {self.db};")
        tdSql.execute(f"use {self.db};")
        tdSql.execute(f"create stable {self.stable0} (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupId int);")
        tdSql.execute(f"create table {self.tb1} {self.tag1};")
        tdSql.execute(f"create table {self.tb2} {self.tag2};")
        tdSql.execute(f"create stable {self.stable1} (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);")
        tdSql.execute(f"create stable {self.stable2} (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);")
        tdSql.execute(f"create table {self.stable1}_1 using {self.stable1}(t_int) tags(1);")
        tdSql.execute(f"create table {self.stable2}_1 using {self.stable2}(t_int) tags(2);")

    def check_test(self, sql):
        # sql = f"use {self.db};" + sql
        # os.system(f'taos -s "{sql}"')
        print(f'{sql}\n')
        tdSql.execute(sql, 1)

    def check(self):
        # same table, auto create + create
        sql = f"insert into {self.tb1} {self.tag1} file '{self.file1}' {self.tb1} {self.tag1} file '{self.file2}';"
        self.check_test(sql)

        # same table, create + insert
        sql = f"insert into {self.tb1} {self.tag1} file '{self.file1}' {self.tb1} file '{self.file2}';"
        self.check_test(sql)

        # same table, insert + create
        sql = f"insert into {self.tb1} file '{self.file1}' {self.tb1} {self.tag1} file '{self.file2}';"
        self.check_test(sql)

        # same table, insert + insert
        sql = f"insert into {self.tb1} file '{self.file1}' {self.tb1} file '{self.file2}';"
        self.check_test(sql)

        # diff table auto create + create
        sql = f"insert into {self.tb1} {self.tag1} file '{self.file1}' {self.tb2} {self.tag2} file '{self.file2}';"
        self.check_test(sql)

        # diff table, create + insert
        sql = f"insert into {self.tb1} {self.tag1} file '{self.file1}' {self.tb2} file '{self.file2}';"
        self.check_test(sql)

        # diff table, insert + create
        sql = f"insert into {self.tb1} file '{self.file1}' {self.tb2} {self.tag2} file '{self.file2}';"
        self.check_test(sql)

        # diff table, insert + insert
        sql = f"insert into {self.tb1} file '{self.file1}' {self.tb2} file '{self.file2}';"
        self.check_test(sql)

        self.check_count(2, [2010000, 1000000])


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

    def check_mix(self):
        #forbid use both value and file in one insert
        self.make_csv(self.file2, 10, 10, self.ts)
        tdSql.error(f"insert into {self.tb1} file '{self.file2}' {self.tb2} values('2021-07-13 14:06:34.630', 10.2, 219, 0.32);")

    def check_bigcsv(self):
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
        sql = f"insert into {self.tb1} {self.tag1} file '{self.file1}';"
        self.check_test(sql)

        # only insert 
        sql = f"insert into {self.tb2} file '{self.file2}';"
        self.check_test(sql)
        print("end insert to table")

        tdSql.query(f"select tbname,count(*) from {self.stable0} group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, rowNum1)
        tdSql.checkData(1, 1, rowNum2)
        print("check insert file to table success")

    def make_stable_csv(self, filepath, once, qtime, startts, table_name):
        f = open(filepath, 'w')
        with f:
          writer = csv.writer(f)
          for j in range(qtime):
            offset = j*once
            ts = startts + offset
            rows = []
            for i in range(once):
                rows.append([f"\'{table_name}\'", ts + i, offset + i, 'NULL'])
            writer.writerows(rows)
        f.close()
        print(datetime.now(), filepath, " ready!")

    def check_stable_csv(self):
        # prepare csv
        print("start stable_csv data prepare")
        once = 10000
        qtime1 = 101
        qtime2 = 100
        child_1 = f"{self.stable1}_1"
        child_2 = f"{self.stable2}_1"
        self.make_stable_csv(self.file1, once, qtime1, self.ts - 86400000, child_1)
        self.make_stable_csv(self.file2, once, qtime2, self.ts, child_2)
        print("end stable_csv data prepare")

        sql = f"insert into {self.db}.{self.stable1}(tbname,ts,q_int,q_binary) file '{self.file1}' {self.db}.{self.stable2}(tbname,ts,q_int,q_binary) file '{self.file2}';"
        self.check_test(sql)
        print("end insert to stable")

        tdSql.query(f"select tbname,count(*) from {self.stable1} group by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, qtime1 * once)
        tdSql.query(f"select tbname,count(*) from {self.stable2} group by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, qtime2 * once)
        print("check stable success")

    def test_ts_4272(self):
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
        tdSql.prepare()
        self.reset_tb()
        self.check_stable_csv()
        self.check_bigcsv()
        self.check()
        self.check_mix()
        os.system(f"rm -rf {self.file1}")
        os.system(f"rm -rf {self.file2}")
        tdSql.close()
        
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
        