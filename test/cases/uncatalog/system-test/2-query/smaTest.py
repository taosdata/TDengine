###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import random
import time
# constant define
WAITS = 5 # wait seconds

class TestSmatest:
    #
    # --------------- main frame -------------------
    #
    # updatecfgDict = {'debugFlag': 135}
    # updatecfgDict = {'fqdn': 135}

    # init
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.ts = 1500000000000

    def check_TD_33336(self):
        sql = "flush database db"
        tdSql.execute(sql)
        time.sleep(5)
        sql = f'select last(ts) + 1d, last(ts) - 1d from db.t1'
        tdSql.query(sql, queryTimes=1)
        lastTs_add1d = tdSql.queryResult[0][0]
        lastTs_sub1d = tdSql.queryResult[0][1]

        sql = f'select count(*) from db.t1 where ts < "{lastTs_add1d}" and vc1 = 1'
        tdSql.query(sql, queryTimes=1)
        all_row_count = tdSql.queryResult[0][0]
        tdLog.debug(f"all rows: {all_row_count}")

        sql = f'select count(*) from db.t1 where ts < "{lastTs_sub1d}" and vc1 = 1'
        tdSql.query(sql, queryTimes=1)
        row_count_sub1d = tdSql.queryResult[0][0]
        tdLog.debug(f"row_count_sub1d: {row_count_sub1d}")

        if row_count_sub1d > all_row_count:
            tdLog.exit(f' err rows returned for sql: {sql} row_count_sub1d: {row_count_sub1d} > all_row_count: {all_row_count}')

    # run case
    def test_smaTest(self):
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

        # insert data
        dbname = "db"
        self.insert_data1(f"{dbname}.t1", self.ts, 10*10000)
        self.insert_data1(f"{dbname}.t4", self.ts, 10*10000)
        # test base case
        # self.test_case1()
        tdLog.debug(" LIMIT test_case1 ............ [OK]")
        # test advance case
        # self.test_case2()
        tdLog.debug(" LIMIT test_case2 ............ [OK]")

        self.check_TD_33336()
        self.ts5900()

    # stop
        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

    #
    # --------------- case  -------------------
    #

    # create table
    def create_tables(self, dbname="db"):
        # super table
        tdSql.execute(f"create table {dbname}.st(ts timestamp, i1 int,i2 int, vc1 varchar(255)) tags(area int)")
        # child table
        tdSql.execute(f"create table {dbname}.t1 using {dbname}.st tags(1)")

        tdSql.execute(f"create table {dbname}.st1(ts timestamp, i1 int ,i2 int, vc1 varchar(255)) tags(area int) sma(i2) ")
        tdSql.execute(f"create table {dbname}.t4 using {dbname}.st1 tags(1)")

        return

    # insert data1
    def insert_data(self, tbname, ts_start, count):
        pre_insert = "insert into %s values" % tbname
        sql = pre_insert
        tdLog.debug("insert table %s rows=%d ..." % (tbname, count))
        for i in range(count):
            sql += " (%d,%d)" % (ts_start + i*1000, i)
            if i > 0 and i % 20000 == 0:
                tdLog.info("%d rows inserted" % i)
                tdSql.execute(sql)
                sql = pre_insert
        # end sql
        tdLog.info("insert_data end")
        if sql != pre_insert:
            tdSql.execute(sql)

        tdLog.debug("INSERT TABLE DATA ............ [OK]")
        return

    def insert_data1(self, tbname, ts_start, count):
        pre_insert = "insert into %s values" % tbname
        sql = pre_insert
        tdLog.debug("insert table %s rows=%d ..." % (tbname, count))
        for i in range(count):
            if random.randint(0, 4) > 2:
                tail = ''
            else:
                tail = 'asd'
            sql += " (%d,%d,%d,%s)" % (ts_start + i*1000, i, i+1, '"' + str(random.randint(0,5)) + f'{tail}"')
            if i > 0 and i % 20000 == 0:
                tdLog.info("%d rows inserted" % i)
                tdSql.execute(sql)
                sql = pre_insert
        # end sql
        tdLog.info("insert_data1 end")
        if sql != pre_insert:
            tdSql.execute(sql)

        tdLog.debug("INSERT TABLE DATA ............ [OK]")
        return
    
    def ts5900query(self):
        sql = "select max(c0) from ts5900.tt1"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '99.0')
        sql = "select min(c0) from ts5900.tt1"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1.0')      

    def ts5900(self):
        tdSql.execute("drop database if exists ts5900;")
        tdSql.execute("create database ts5900;")
        
        tdSql.execute("create table ts5900.meters (ts timestamp, c0 varchar(64)) tags(t0 varchar(64));")
        
        sql = "CREATE TABLE ts5900.`tt1` USING ts5900.`meters` TAGS ('t11')"
        tdSql.execute(sql)
        for i in range(155):
            tdSql.query(f"insert into ts5900.tt1 values(now+{i*10}s, '{i+1}.0')")
        tdSql.query("insert into ts5900.tt1 values(now, '1.2')")
        tdSql.query("insert into ts5900.tt1 values(now+1s, '2.0')")
        tdSql.query("insert into ts5900.tt1 values(now+2s, '3.0')")
        tdSql.query("insert into ts5900.tt1 values(now+3s, '105.0')")
        tdSql.query("insert into ts5900.tt1 values(now+4s, '4.0')")
        
        sql = "select count(*) from ts5900.tt1"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '160')
        
        for i in range(10):
            tdSql.execute("flush database ts5900")
            time.sleep(1)
            self.ts5900query()
            tdSql.query(f"insert into ts5900.tt1 values(now, '23.0')")
            self.ts5900query()
            tdLog.info(f"ts5900 test {i} ............ [OK]")
            time.sleep(1)

    # test case1 base
    # def test_case1(self):
    #     #
    #     # limit base function
    #     #
    #     # base no where
    #     sql = "select * from t1 limit 10"
    #     tdSql.waitedQuery(sql, 10, WAITS)

#
# add case with filename
#
