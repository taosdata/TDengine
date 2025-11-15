from new_test_framework.utils import tdLog, tdSql, AutoGen
from random import randint

import datetime
import threading
import time
import os
import random





class TestInsertDouble:

    @classmethod
    def setup_class(cls):
        cls.database = "db1"
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), True)

    #
    # ------- write double ----------------
    #    

    def prepare_db(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")

    def check_value(self, table_name, dtype, bits):
        tdSql.execute(f"drop table if exists {table_name}")
        tdSql.execute(f"create table {table_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned)")

        tdSql.execute(f"insert into {table_name} values(1717122943000, -16, +6)")
        tdSql.execute(f"insert into {table_name} values(1717122944000, 80.99, +0042)")
        tdSql.execute(f"insert into {table_name} values(1717122945000, -0042, +80.99)")
        tdSql.execute(f"insert into {table_name} values(1717122946000, 52.34354, 18.6)")
        tdSql.execute(f"insert into {table_name} values(1717122947000, -12., +3.)")
        tdSql.execute(f"insert into {table_name} values(1717122948000, -0.12, +3.0)")
        tdSql.execute(f"insert into {table_name} values(1717122949000, -2.3e1, +2.324e2)")
        tdSql.execute(f"insert into {table_name} values(1717122950000, -2e1,  +2e2)")
        tdSql.execute(f"insert into {table_name} values(1717122951000, -2.e1, +2.e2)")
        tdSql.execute(f"insert into {table_name} values(1717122952000, -0x40, +0b10000)")
        tdSql.execute(f"insert into {table_name} values(1717122953000, -0b10000, +0x40)")

        # str support
        tdSql.execute(f"insert into {table_name} values(1717122954000, '-16', '+6')")
        tdSql.execute(f"insert into {table_name} values(1717122955000, ' -80.99', ' +0042')")
        tdSql.execute(f"insert into {table_name} values(1717122956000, ' -0042', ' +80.99')")
        tdSql.execute(f"insert into {table_name} values(1717122957000, '52.34354', '18.6')")
        tdSql.execute(f"insert into {table_name} values(1717122958000, '-12.', '+5.')")
        tdSql.execute(f"insert into {table_name} values(1717122959000, '-.12', '+.5')")
        tdSql.execute(f"insert into {table_name} values(1717122960000, '-2.e1', '+2.e2')")
        tdSql.execute(f"insert into {table_name} values(1717122961000, '-2e1',  '+2e2')")
        tdSql.execute(f"insert into {table_name} values(1717122962000, '-2.3e1', '+2.324e2')")
        tdSql.execute(f"insert into {table_name} values(1717122963000, '-0x40', '+0b10010')")
        tdSql.execute(f"insert into {table_name} values(1717122964000, '-0b10010', '+0x40')")

        tdSql.query(f"select * from {table_name}")
        tdSql.checkRows(22)

        baseval = 2**(bits/2)
        negval = -baseval + 1.645
        posval = baseval + 4.323
        bigval = 2**(bits-1)
        max_i = bigval - 1
        min_i = -bigval
        max_u = 2*bigval - 1
        min_u = 0
        print("val:", baseval, negval, posval, max_i)

        tdSql.execute(f"insert into {table_name} values(1717122965000, {negval}, {posval})")
        tdSql.execute(f"insert into {table_name} values(1717122966000, -{baseval}, {baseval})")
        tdSql.execute(f"insert into {table_name} values(1717122967000, {max_i}, {max_u})")
        tdSql.execute(f"insert into {table_name} values(1717122968000, {min_i}, {min_u})")

        tdSql.query(f"select * from {table_name}")
        tdSql.checkRows(26)
        
        # error case
        tdSql.error(f"insert into {table_name} values(1717122969000, 0, {max_u+1})")
        tdSql.error(f"insert into {table_name} values(1717122970000, 0, -1)")
        tdSql.error(f"insert into {table_name} values(1717122971000, 0, -2.0)")
        tdSql.error(f"insert into {table_name} values(1717122972000, 0, '-2.0')")
        tdSql.error(f"insert into {table_name} values(1717122973000, {max_i+1}, 0)")
        tdSql.error(f"insert into {table_name} values(1717122974000, {min_i-1}, 0)")
        tdSql.error(f"insert into {table_name} values(1717122975000, '{min_i-1}', 0)")

    def check_tags(self, stable_name, dtype, bits):
        tdSql.execute(f"create stable {stable_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned) tags(id {dtype})")

        baseval = 2**(bits/2)
        negval = -baseval + 1.645
        posval = baseval + 4.323
        bigval = 2**(bits-1)
        max_i = bigval - 1
        min_i = -bigval
        max_u = 2*bigval - 1
        min_u = 0

        tdSql.execute(f"insert into {stable_name}_1 using {stable_name} tags('{negval}') values(1717122976000, {negval}, {posval})")
        tdSql.execute(f"insert into {stable_name}_2 using {stable_name} tags({posval}) values(1717122977000, -{baseval} , {baseval})")
        tdSql.execute(f"insert into {stable_name}_3 using {stable_name} tags('0x40') values(1717122978000, {max_i}, {max_u})")
        tdSql.execute(f"insert into {stable_name}_4 using {stable_name} tags(0b10000) values(1717122979000, {min_i}, {min_u})")
        
        tdSql.execute(f"insert into {stable_name}_5 using {stable_name} tags({max_i}) values(1717122980000, '{negval}', '{posval}')")
        tdSql.execute(f"insert into {stable_name}_6 using {stable_name} tags('{min_i}') values(1717122981000, '-{baseval}' , '{baseval}')")
        tdSql.execute(f"insert into {stable_name}_7 using {stable_name} tags(-0x40) values(1717122982000, '{max_i}', '{max_u}')")
        tdSql.execute(f"insert into {stable_name}_8 using {stable_name} tags('-0b10000') values(1717122983000, '{min_i}', '{min_u}')")

        tdSql.execute(f"insert into {stable_name}_9 using {stable_name} tags(12.) values(1717122984000, {negval}, {posval})")
        tdSql.execute(f"insert into {stable_name}_10 using {stable_name} tags('-8.3') values(1717122985000, -{baseval} , {baseval})")
        tdSql.execute(f"insert into {stable_name}_11 using {stable_name} tags(2.e1) values(1717122986000, {max_i}, {max_u})")
        tdSql.execute(f"insert into {stable_name}_12 using {stable_name} tags('-2.3e1') values(1717122987000, {min_i}, {min_u})")

        tdSql.query(f"select * from {stable_name}")
        tdSql.checkRows(12)

    # sub moudule
    def do_write_double(self):
        tdSql.prepare(replica = self.replicaVar)
        self.prepare_db()

        self.check_value("t1", "bigint", 64)
        self.check_value("t2", "int", 32)
        self.check_value("t3", "smallint", 16)
        self.check_value("t4", "tinyint", 8)
        tdLog.printNoPrefix("==========end case1 run ...............")

        self.check_tags("t_big", "bigint", 64)
        self.check_tags("t_int", "int", 32)
        self.check_tags("t_small", "smallint", 16)
        self.check_tags("t_tiny", "tinyint", 8)
        tdLog.printNoPrefix("==========end case2 run ...............")
        
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


    #
    #  ------- write timestamp ----------------
    #
    def do_write_timestamp(self):
        tdSql.execute("create database test_insert_timestamp PRECISION 'ns';")
        tdSql.execute("use test_insert_timestamp;")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(id int);")
        tdSql.execute("create table test_t using st tags(1);")

        expectErrInfo = "syntax error"
        # abnormal scenario: timestamp + timestamp
        tdSql.error("insert into test_t values(now + today(), 1 );", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(now - today(), 1 );", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(today() + now(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(today() - now(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('2023-11-28 00:00:00.000' + '2023-11-28 00:00:00.000', 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('2023-11-28 00:00:00.000' + 1701111600000, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(1701111500000 + 1701111600000, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: timestamp + interval + interval
        tdSql.error("insert into test_t values(today() + 1d + 1s, 1);", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: interval - timestamp
        tdSql.error("insert into test_t values(2h - now(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(2h - today(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: interval + interval
        tdSql.error("insert into test_t values(2h - 1h, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(2h + 1h, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: non-support datatype - n
        tdSql.error("insert into test_t values(today() + 2n, 7); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: non-support datatype - y
        tdSql.error("insert into test_t values(today() - 2y, 8);", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: non-support datatype
        tdSql.error("insert into test_t values('a1701619200000', 8);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('ss2023-12-05 00:00:00.000' + '1701619200000', 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(123456, 1);", expectErrInfo="Timestamp data out of range")
        tdSql.error("insert into test_t values(123.456, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(True, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(None, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(null, 1);", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: incorrect format
        tdSql.error("insert into test_t values('2023-122-05 00:00:00.000' + '1701619200000', 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('2023-12--05 00:00:00.000' + '1701619200000', 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('12/12/2023' + 10a, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(1701619200000111, 1);", expectErrInfo="Timestamp data out of range", fullMatched=False)

        # normal scenario:timestamp + interval
        tdSql.execute("insert into test_t values(today() + 2b, 1);")
        tdSql.execute("insert into test_t values(1701619200000000000 + 2u, 2);")
        tdSql.execute("insert into test_t values(today + 2a, 3);")
        tdSql.execute("insert into test_t values('2023-12-05 23:59:59.999' + 2a, 4);")
        tdSql.execute("insert into test_t values(1701921599000000000 + 3a, 5);")

        # normal scenario:timestamp - interval
        tdSql.execute("insert into test_t values(today() - 2s, 6);")
        tdSql.execute("insert into test_t values(now() - 2m, 7);")
        tdSql.execute("insert into test_t values(today - 2h, 8);")
        tdSql.execute("insert into test_t values('2023-12-05 00:00:00.000000000' - 2a, 9);")
        tdSql.execute("insert into test_t values(1701669000000000000 - 2a, 10);")

        # normal scenario:interval + timestamp
        tdSql.execute("insert into test_t values(2d + now, 11);")
        tdSql.execute("insert into test_t values(2w + today, 12);")

        # normal scenario:timestamp
        tdSql.execute("insert into test_t values('2023-12-05 00:00:00.000', 13);")
        tdSql.execute("insert into test_t values(1701629100000000000, 14);")
        tdSql.execute("insert into test_t values(now() + 2s, 15);")
        tdSql.execute("insert into test_t values('2023-12-05 00:00:59.999999999+07:00' + 10a, 16);")
        tdSql.execute("insert into test_t values('2023-12-05T00:00:59.110+07:00' + 10a, 17);")
        tdSql.execute("insert into test_t values('2023-12-05' + 10a, 18);")
        tdSql.execute("insert into test_t values('2023-11-15', -15);")
        tdSql.execute("insert into test_t values(1701619200000000000 - 2a, -10);")
        tdSql.execute("insert into test_t values(1701619200000000000, -5);")
        tdSql.execute("insert into test_t values('2023-12-05 12:12:12' + 10a, 19);")

        # data verification
        tdSql.query(f'select ts,c1  from test_t order by c1;')
        tdSql.checkRows(22)
        tdSql.checkEqual(tdSql.queryResult[0][0], 1699977600000000000)     # c1=-15
        tdSql.checkEqual(tdSql.queryResult[1][0], 1701619199998000000)     # c1=-10
        tdSql.checkEqual(tdSql.queryResult[2][0], 1701619200000000000)     # c1=-5
        tdSql.checkEqual(tdSql.queryResult[3][0], self.__get_today_ts() + 2)     # c1=1
        tdSql.checkEqual(tdSql.queryResult[4][0], 1701619200000002000)     # c1=2
        tdSql.checkEqual(tdSql.queryResult[5][0], self.__get_today_ts() + 2000000)     # c1=3
        tdSql.checkEqual(tdSql.queryResult[6][0], 1701792000001000000)     # c1=4
        tdSql.checkEqual(tdSql.queryResult[7][0], 1701921599003000000)     # c1=5
        tdSql.checkEqual(tdSql.queryResult[8][0], self.__get_today_ts() - 2000000000)     # c1=6
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[9][0]), str(datetime.date.today()))    # c1=7
        tdSql.checkEqual(tdSql.queryResult[10][0], self.__get_today_ts() - 7200000000000)    # c1=8
        tdSql.checkEqual(tdSql.queryResult[11][0], 1701705599998000000)    # c1=9
        tdSql.checkEqual(tdSql.queryResult[12][0], 1701668999998000000)    # c1=10
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[13][0]), str(datetime.date.today() + datetime.timedelta(days=2)))    # c1=11
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[14][0]), str(datetime.date.today() + datetime.timedelta(days=14)))    # c1=12
        tdSql.checkEqual(tdSql.queryResult[15][0], 1701705600000000000)    # c1=13
        tdSql.checkEqual(tdSql.queryResult[16][0], 1701629100000000000)    # c1=14
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[17][0]), str(datetime.date.today()))    # c1=15
        tdSql.checkEqual(tdSql.queryResult[18][0], 1701709260009999999)    # c1=16
        tdSql.checkEqual(tdSql.queryResult[19][0], 1701709259120000000)    # c1=17
        tdSql.checkEqual(tdSql.queryResult[20][0], 1701705600010000000)    # c1=18
        tdSql.checkEqual(tdSql.queryResult[21][0], 1701749532010000000)    # c1=19

        tdSql.execute("drop table if exists test_t ;")
        tdSql.execute("drop stable if exists st;")
        tdSql.execute("drop database if exists test_insert_timestamp;")
        tdLog.success("%s successfully executed" % __file__)

    def __convert_ts_to_date(self, ts: int) -> str:
        dt_object = datetime.datetime.fromtimestamp(ts / 1e9)

        formatted_date = dt_object.strftime('%Y-%m-%d')

        return formatted_date

    def __get_today_ts(self) -> int:
        return int(time.mktime(time.strptime(str(datetime.date.today()), "%Y-%m-%d"))) * 1000000000


    #
    # ---------------- insert/drop concurrency ---------------
    #
    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=tdSql.execute, args=(insert_sql,))
            tlist.append(t)
        return tlist

    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    # sub module
    def do_insert_drop_concurrent(self):
        tdSql.prepare()
        tdSql.execute('create database if not exists test;')
        tdSql.execute('create table test.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int );')
        tdSql.execute('create table test.tb using test.stb TAGS (1, 1);')        

        # double comma insert check error
        tdSql.error("insert into test.tb(ts, c11) values(now,,100)")

        sql_list = list()
        for i in range(5):
            sql = f'insert into test.tb values (now-{i}m, {i}, {i});'
            sql_list.append(sql)
        sql_list.append(f'drop database test;')
        tlist = self.genMultiThreadSeq(sql_list)
        self.multiThreadRun(tlist)
        tdSql.query(f'select * from information_schema.ins_databases')


    #
    # ---------------- test_insertFutures.py ---------------
    #

    def init_futurets(self):
        self.timestamp_ms = int(round(time.time()*1000))
        self.timestamp_us = int(round(time.time()*1000000))
        self.timestamp_ns = int(time.time_ns())
        self.ms_boundary = 31556995200000
        self.us_boundary = 31556995200000000
        self.ns_boundary = 9214646400000000000
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.ctbname = 'ctb'

    def insert_check(self,timestamp,tbname):
        tdSql.execute(f'insert into {tbname} values({timestamp},1)')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],1)
        tdSql.execute('flush database db')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],1)
        tdSql.execute(f'insert into {tbname} values({timestamp},2)')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],2)
        tdSql.execute('flush database db')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],2)
        tdSql.execute(f'delete from {tbname} where ts = {timestamp}')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkRows(0)
        tdSql.execute('flush database db')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkRows(0)
        
    def insert_ms(self):
        tdSql.prepare()
        tdSql.execute('use db')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        timestamp = random.randint(self.timestamp_ms,self.ms_boundary-1)
        self.insert_check(timestamp,self.ntbname)
        self.insert_check(self.ms_boundary,self.ntbname)
        tdSql.error(f'insert into {self.ntbname} values({self.ms_boundary+1},1)')
        tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        self.insert_check(timestamp,self.ctbname)
        self.insert_check(self.ms_boundary,self.ctbname)
        tdSql.error(f'insert into {self.ctbname} values({self.ms_boundary+1},1)')
    def insert_us(self):
        tdSql.execute('drop database if exists db1')
        tdSql.execute('create database db1 precision "us"')
        tdSql.execute('use db1')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        timestamp = random.randint(self.timestamp_us,self.us_boundary-1)
        self.insert_check(timestamp,self.ntbname)
        self.insert_check(self.us_boundary,self.ntbname)
        tdSql.error(f'insert into {self.ntbname} values({self.us_boundary+1},1)')
        tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        self.insert_check(timestamp,self.ctbname)
        self.insert_check(self.us_boundary,self.ctbname)
        tdSql.error(f'insert into {self.ctbname} values({self.us_boundary+1},1)')
    def insert_ns(self):
        tdSql.execute('drop database if exists db2')
        tdSql.execute('create database db2 precision "ns"')
        tdSql.execute('use db2')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        timestamp = random.randint(self.timestamp_ns,self.ns_boundary-1)
        self.insert_check(timestamp,self.ntbname)
        self.insert_check(self.ns_boundary,self.ntbname)
        tdSql.error(f'insert into {self.ntbname} values({self.ns_boundary+1},1)')
        tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        self.insert_check(timestamp,self.ctbname)
        self.insert_check(self.ns_boundary,self.ctbname)
        tdSql.error(f'insert into {self.ctbname} values({self.ns_boundary+1},1)')

    # sub module
    def do_write_futurets(self):
        self.init_futurets()

        self.insert_ms()
        self.insert_us()
        self.insert_ns()
        

    #
    # ---------------- test_insert_wide_column.py ---------------
    #

    def query_test(self, stbname):
        sql = f"select count(*) from {stbname}"
        tdSql.execute(sql)
        sql = f"select * from {stbname} order by ts desc;"
        tdSql.execute(sql)
        sql = f"select * from (select * from {stbname} where c1=c2 or c3=c4 or c5=c6) order by ts desc;"
        tdSql.execute(sql)

        tdLog.info(" test query ok!")


    def check_db(self, dbname, stbname, childname, tag_cnt, column_cnt, child_cnt, insert_rows, binary_len, nchar_len):    
        self.autoGen.create_db(dbname)
        self.autoGen.create_stable(stbname, tag_cnt, column_cnt, binary_len, nchar_len, type_set='varchar_preferred')
        self.autoGen.create_child(stbname, childname, child_cnt, tag_data_mode='old')
        self.autoGen.insert_data(insert_rows)
        self.autoGen.insert_samets(insert_rows)
        self.query_test(stbname)

    # sub module
    def do_write_wide_column(self):
        dbname = "test"
        stbname = "st"
        childname = "d"
        child_cnt = 2
        insert_rows = 10
        tag_cnt    = 15
        column_cnt = 20
        binary_len = 10240
        nchar_len =  1025
        self.autoGen = AutoGen(step=1, batch=100,genDataMode="fillone")
        self.autoGen.set_batch_size(1)
        
        # normal
        self.check_db(dbname, stbname, childname, tag_cnt, column_cnt, child_cnt, insert_rows, binary_len, nchar_len)        

        # max
        dbname = "test_max_col"
        child_cnt = 3
        insert_rows = 50
        tag_cnt = 128
        binary_len = 3
        nchar_len = 4
        column_cnt = 4096 - tag_cnt
        self.autoGen.set_batch_size(1)
        self.check_db(dbname, stbname, childname, tag_cnt, column_cnt, child_cnt, insert_rows, binary_len, nchar_len)

    #
    # ---------------- test_insert_wide_column.py ---------------
    #
    def initdb(self):
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0")
        tdSql.execute("use d0")
        tdSql.execute("create stable stb0 (ts timestamp, w_ts timestamp, opc nchar(100), quality int) tags(t0 int)")
        tdSql.execute("create table t0 using stb0 tags(1)")
        tdSql.execute("create table t1 using stb0 tags(2)")

    def multi_insert(self):
        for i in range(5):
            tdSql.execute(f"insert into t1 values(1721265436000, now() + {i + 1}s, '0', 12) t1(opc, quality, ts) values ('opc2', 192, now()+ {i + 2}s) t1(ts, opc, quality) values(now() + {i + 3}s, 'opc4', 10) t1 values(1721265436000, now() + {i + 4}s, '1', 191) t1(opc, quality, ts) values('opc5', 192, now() + {i + 5}s) t1 values(now(), now() + {i + 6}s, '2', 192)")
            tdSql.execute("insert into t0 values(1721265436000,now(),'0',192) t0(quality,w_ts,ts) values(192,now(),1721265326000) t0(quality,w_ts,ts) values(190,now()+1s,1721265326000) t0 values(1721265436000,now()+2s,'1',191) t0(quality,w_ts,ts) values(192,now()+3s,1721265326002) t0(ts,w_ts,opc,quality) values(1721265436003,now()+4s,'3',193) t0 values(now(), now() + 4s , '2', 192)")

    def do_multi_insert(self):
        self.initdb()
        self.multi_insert()

    #
    # main
    #
    def test_write(self):
        """ Write basic

        1. Write data with timestamp
        2. Write data with double
        3. Insert and drop database concurrently
        4. Write data with future timestamp
        5. Write data with wide column

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_timestamp.py
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_double.py
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_drop.py
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insertFuturets.py
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_wide_column.py
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_multi_insert.py

        """

        self.do_write_timestamp()
        self.do_write_double()
        self.do_insert_drop_concurrent()
        self.do_write_futurets()
        self.do_write_wide_column()
        self.do_multi_insert()