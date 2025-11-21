import json
import csv
import os
import platform
import taos

from datetime import datetime
from enum import Enum
from new_test_framework.utils import tdLog, tdSql
from taos import SmlProtocol, SmlPrecision
from taos.error import SchemalessError


class TestWriteBug:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        cls.dbname = 'db'
        cls.stbname = 'st'
        tdSql.execute("create database {};".format(cls.dbname))
        tdSql.execute("use {};".format(cls.dbname))
        tdSql.execute("create table st (ts timestamp, col1 int, col2 varchar(64)) tags (t1 int, t2 varchar(32));")

    #
    # ------------------ TD-27388 ------------------
    #
    def check_half_quotes(self):
        sql_list = [
            "insert into t1 using st tags(1, 'tag1) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, tag1') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, test msg');",
            "insert into t1 using st tags(1, 'tag1' values(now, 1, test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg)';",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, tag2') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1) values(now, 1, 'test msg') t2 using st tags(2, 'tag2) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg) t2 using st tags(2, 'tag2') values(now, 2, test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, 'test msg);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, 'test msg);"
        ]
        for sql in sql_list:
            tdLog.debug("execute harlf quotes sql: %s" % sql)
            tdSql.error(sql)

    def check_esc(self):
        sql_list = [
            "insert into t1 using st tags(1, 'tag1\\') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, \\'tag1') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg\\');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, \\'test msg');",
            "insert into t1 using st tags(1, \\'tag1\\') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, \\'test msg\\');",
            "insert into t1 using st tags(1, \\'tag1\\') values(now, 1, \\'test msg\\');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2\\') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, \\'tag2') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, \\'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, 'test msg\\');",
            "insert into t1 using st tags(1, \\'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2\\') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, \\'test msg\\');"
        ]
        for sql in sql_list:
            tdLog.debug("execute escape character sql: %s" % sql)
            tdSql.error(sql)

    def check_specific_character(self):
        sql_list = [
            "insert into t1 using st tags(1, 'tag1$) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1,) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1'') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1() values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1*) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1+) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1,) values(now, 1, 'test msg');",
            "isnert into t1 using st tags(1, 'tag1-) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1.) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg$);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg,);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg+%+-.);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2$) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2,) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2'') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2() values(now, 2, 'test msg');"
        ]
        for sql in sql_list:
            tdLog.debug("execute specific character sql: %s" % sql)
            tdSql.error(sql)

    def do_td27388(self):
        self.check_half_quotes()
        self.check_esc()
        self.check_specific_character()
        print("\n")
        print("bug TD-27388 ................ [passed]")

    #
    # ------------------ TD-29157 ------------------
    #
    def do_td29157(self):
        self.db_name = "td29157"
        self.conn.execute(f"drop database if exists {self.db_name}")
        self.conn.execute(f"CREATE DATABASE {self.db_name}")
        self.conn.execute(f"USE {self.db_name}")

        tdSql.execute("create table stb1 (ts timestamp, c0 varbinary(10)) tags(t0 varbinary(10));")
        tdSql.execute("insert into ctb11 using stb1 tags(\"0x11\") values(now,\"0x01\");")
        tdSql.execute("insert into ctb12 using stb1 tags(\"0x22\") values(now,\"0x02\");")
        tdSql.query("show tags from ctb11;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'VARBINARY(10)')
        tdSql.checkData(0, 5, '\\x30783131')
        
        tdSql.execute("create table stb2 (ts timestamp, c0 geometry(500)) tags(t0 geometry(100));")
        tdSql.execute("insert into ctb2 using stb2 tags('LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)') values(now,'POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))');")
        tdSql.query("show tags from ctb2;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'GEOMETRY(100)')
        tdSql.checkData(0, 5, 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)')

        tdSql.execute("create table stb3 (ts timestamp, c0 bigint, c1 varchar(10)) tags(t0 geometry(100), t1 varbinary(10));")
        tdSql.execute("insert into ctb3 using stb3 tags('POLYGON EMPTY', \"0x03\") values(now,100, \"abc\");")
        tdSql.query("show tags from ctb3;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'GEOMETRY(100)')
        tdSql.checkData(0, 5, 'POLYGON EMPTY')
        tdSql.checkData(1, 3, 't1')
        tdSql.checkData(1, 4, 'VARBINARY(10)')
        tdSql.checkData(1, 5, '\\x30783033')
        tdSql.execute("drop database if exists %s" % self.db_name)
        print("bug TD-29157 ................ [passed]")

    #
    # ------------------ TD-29793 ------------------
    #
    def do_td29793(self):
        conn = taos.connect()

        conn.execute("drop database if exists reproduce")
        conn.execute("CREATE DATABASE reproduce")
        conn.execute("USE reproduce")

        # influxDB
        conn.execute("drop table if exists meters")
        lines1 = ["meters,location=California.LosAngeles groupid=2,current=11i32,voltage=221,phase=0.28 1648432611249000",]
        lines2 = ["meters,location=California.LosAngeles,groupid=2 groupid=2,current=11i32,voltage=221,phase=0.28 1648432611249001",]
        lines3 = ["meters,location=California.LosAngeles,groupid=2 current=11i32,voltage=221,phase=0.28 1648432611249002",]
        
        try:
            conn.schemaless_insert(lines1, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
            conn.schemaless_insert(lines2, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')
        
        try:
            conn.schemaless_insert(lines3, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')
        

        # OpenTSDB
        conn.execute("drop table if exists meters")
        lines1 = ["meters 1648432611249 10i32 location=California.SanFrancisco groupid=2 groupid=3",]
        lines2 = ["meters 1648432611250 10i32 groupid=2 location=California.SanFrancisco groupid=3",]
        
        try:
            conn.schemaless_insert(lines1, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')

        try:
            conn.schemaless_insert(lines2, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')

        # OpenTSDB Json
        conn.execute("drop table if exists meters")
        lines1 = [{"metric": "meters", "timestamp": 1648432611249, "value": "a32", "tags": {"location": "California.SanFrancisco", "groupid": 2, "groupid": 3}}]
        lines2 = [{"metric": "meters", "timestamp": 1648432611250, "value": "a32", "tags": {"groupid": 2, "location": "California.SanFrancisco", "groupid": 4}}]
        try:
            lines = json.dumps(lines1)
            conn.schemaless_insert([lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            # tdSql.checkEqual('expected error', 'no error occurred')     TD-29850
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')
        
        try:
            lines = json.dumps(lines2)
            conn.schemaless_insert([lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            # tdSql.checkEqual('expected error', 'no error occurred')     TD-29850
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')
        print("bug TD-29793 ................ [passed]")

    #
    # ------------------ TS-4219 ------------------
    #
    def do_ts4219(self):
        tdSql.error("insert into ct1 using st tags('group name 1') values(now, 1, 1.1)(now+1s, 2, 2.2)  ct1 using st tags('group 1) values(now+2s, 3, 3.3); ")
        print("bug TS-4219 ................. [passed]")

    #
    # ------------------ TS-4272 ------------------
    #
    def init_ts4272(self):
        self.testcasePath = os.path.split(__file__)[0]
        self.testcasefilename = os.path.split(__file__)[-1]
        self.ts = 1700638570000  # 2023-11-22T07:36:10.000Z
        self.db = 'db1'
        self.tb1 = 'd001'
        self.tb2 = 'd002'
        self.stable0 = "meters"
        self.stable1 = "stb_1"
        self.stable2 = "stb_null"
        self.tag1 = f'using {self.stable0}(groupId) tags(1)'
        self.tag2 = f'using {self.stable0}(groupId) tags(2)'
        self.file1 = f"{self.testcasePath}/b.csv"
        self.file2 = f"{self.testcasePath}/c.csv"
        if platform.system().lower() == 'windows':
            self.file1 = self.file1.replace("\\","\\\\")
            self.file2 = self.file2.replace("\\","\\\\")
        tdLog.debug(f"start to excute {__file__}")


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

    def do_ts4272(self):
        # init
        self.init_ts4272()

        # do
        tdSql.prepare()
        self.reset_tb()
        self.check_stable_csv()
        self.check_bigcsv()
        self.check()
        self.check_mix()
        os.system(f"rm -rf {self.file1}")
        os.system(f"rm -rf {self.file2}")

        print("bug TS-4272 ................. [passed]")

    #
    # ------------------ TS-4295 ------------------
    #
    def do_ts4295(self):
        # init
        self.db_name = "db"
        self.stable_name = "st"

        # do
        tdSql.execute("create database if not exists %s" % self.db_name)
        tdSql.execute("use %s" % self.db_name)
        # create super table
        tdSql.execute("create table %s (ts timestamp, c1 varbinary(32)) tags (t1 int)" % self.stable_name)
        # create child table
        child_table_list = []
        for i in range(10):
            child_table_name = "ct_" + str(i+1)
            child_table_list.append(child_table_name)
            tdSql.execute("create table %s using st tags(%s);" % (child_table_name, str(i+1)))
            tdLog.info("create table %s successfully" % child_table_name)
        # insert data
        sql_src = "insert into table_name values"
        for j in range(10000):
            sql_src += " (now+%ss, '0x7661726331')" % str(j+1)
        
        for i in range(100):
            for child_table in child_table_list:
                sql = sql_src.replace("table_name", child_table)
                tdSql.execute(sql)
                tdLog.info("Insert data into %s successfully" % child_table)
            tdLog.info("Insert data round %s successfully" % str(i+1))
        tdSql.execute("flush database %s" % self.db_name)        

        print("bug TS-4295 ................. [passed]")

    #
    # ------------------ TS-4479 ------------------
    #
    def do_ts4479(self):
        # init
        self.db_name = "db"
        self.stable_name = "st"

        # do        
        tdSql.execute("create database if not exists %s" % self.db_name)
        tdSql.execute("use %s" % self.db_name)
        # create super table
        tdSql.execute("drop table if exists %s" % self.stable_name)
        tdSql.execute("create table %s (ts timestamp, c1 varbinary(65517)) tags (t1 varbinary(16382))" % self.stable_name)

        # varbinary tag length is more than 16382
        tag = os.urandom(16383).hex()
        tdSql.error("create table ct using st tags(%s);" % ('\\x' + tag))

        # create child table with max column and tag length
        child_table_list = []
        for i in range(2):
            child_table_name = "ct_" + str(i+1)
            child_table_list.append(child_table_name)
            tag = os.urandom(16382).hex()
            tdSql.execute("create table %s using st tags('%s');" % (child_table_name, '\\x' + tag))
            tdLog.info("create table %s successfully" % child_table_name)

        # varbinary column length is more than 65517
        value = os.urandom(65518).hex()
        tdSql.error("insert into ct_1 values(now, '\\x%s');" % value)

        # insert data
        for i in range(10):
            sql = "insert into table_name values"
            for j in range(5):
                value = os.urandom(65517).hex()
                sql += "(now+%ss, '%s')," % (str(j+1), '\\x' + value)
            for child_table in child_table_list:
                tdSql.execute(sql.replace("table_name", child_table))
                tdLog.info("Insert data into %s successfully" % child_table)
            tdLog.info("Insert data round %s successfully" % str(i+1))
        tdSql.execute("flush database %s" % self.db_name)

        # insert \\x to varbinary column
        tdSql.execute("insert into ct_1 values(now, '\\x');")
        tdSql.query("select * from ct_1 where c1 = '\\x';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, b'')

        # insert \\x to varbinary tag
        tdSql.execute("create table ct_3 using st tags('\\x');")
        tdSql.execute("insert into ct_3 values(now, '\\x45');")
        tdSql.query("select * from st where t1='';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, b'')        
        tdSql.execute("drop database if exists %s" % self.db_name)

        print("bug TS-4479 ................. [passed]")



    #
    # ------------------ main ------------------
    #
    def test_write_bug(self):
        """Write bugs

        1. TD-27388
        2. TD-29157
        3. TD-29793
        4. TS-4219
        5. TS-4272
        6. TS-4295
        7. TS-4479

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_td27388.py
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_td29157.py
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_td29793.py
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_ts4219.py
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/1-insert/test_ts4272.py
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_ts4295.py
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_ts4479.py

        """        
        # td
        self.do_td27388()
        self.do_td29157()
        self.do_td29793()
        # ts
        self.do_ts4219()
        self.do_ts4272()
        self.do_ts4295()
        self.do_ts4479()

        tdLog.success(f"{__file__} successfully executed")
