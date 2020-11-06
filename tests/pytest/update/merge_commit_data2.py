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

import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import time


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
    
    
    def restart_taosd(self,db):
        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        tdSql.execute("use %s;" % db)

    def date_to_timestamp_microseconds(self, date):
        datetime_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
        obj_stamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
        return obj_stamp

    def timestamp_microseconds_to_date(self, timestamp):
        d = datetime.datetime.fromtimestamp(timestamp/1000)
        str1 = d.strftime("%Y-%m-%d %H:%M:%S.%f")
        return str1



    def run(self):
        print("==========step1")
        print("create table && insert data")
        sql = 'reset query cache'
        tdSql.execute(sql)
        sql = 'drop database if exists db'
        tdSql.execute(sql)
        sql = 'create database db update 1 days 30;'
        tdSql.execute(sql)
        sql = 'use db;'
        tdSql.execute(sql)
        tdSql.execute('create table t1 (ts timestamp, a int)')


        print("==================================1 start")
        insert_rows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % insert_rows)
        for i in range(insert_rows):
            tdSql.execute('insert into t1 values (%d , 1)' %(t0+i))
        print("==========step2")
        print("restart to commit ")
        self.restart_taosd('db')

        print('check query result after restart')
        tdSql.query('select * from db.t1;')
        for i in range(insert_rows):
            tdSql.checkData(i, 1, 1)

        print("==========step3")
        print('insert data')
        for i in range(insert_rows):
            tdSql.execute('insert into t1 values (%d , 1)' %(t0+i+5000))
        print('check query result before restart')
        tdSql.query('select * from db.t1;')
        for i in range(insert_rows, insert_rows*2):
            tdSql.checkData(i, 1, 1)

        self.restart_taosd('db')
        print('check query result after restart')
        tdSql.query('select * from db.t1;')
        for i in range(insert_rows, insert_rows*2):
            tdSql.checkData(i, 1, 1)

        print("==========step4")
        print('insert data')
        for i in range(insert_rows):
            tdSql.execute('insert into t1 values (%d , 2)' %(t0+i))
        for i in range(insert_rows):
            tdSql.execute('insert into t1 values (%d , 1)' %(t0+i+5000))

        print('check query result before restart')
        tdSql.query('select * from db.t1;')
        print(tdSql.queryResult)
        for i in range(insert_rows):
            tdSql.checkData(i, 1, 2)
        for i in range(insert_rows, insert_rows*2):
            tdSql.checkData(i, 1, 1)

        print('check query result after restart')
        self.restart_taosd('db')
        tdSql.query('select * from db.t1;')
        # print(tdSql.queryResult)
        for i in range(insert_rows):
            tdSql.checkData(i, 1, 2)
        for i in range(insert_rows, insert_rows*2):
            tdSql.checkData(i, 1, 1)

        print("==================================2 start")
        print("==========step1")
        print("create table && insert data")
        tdSql.execute('create table t2 (ts timestamp, a int)')
        insert_rows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % insert_rows)
        for i in range(insert_rows):
            tdSql.execute('insert into t2 values (%d , 1)' %(t0+i))
        print('restart to commit')
        self.restart_taosd('db')
        for i in range(insert_rows):
            tdSql.execute('insert into t2 values (%d , 1)' %(t0+i+5000))
        print('restart to commit')
        self.restart_taosd('db')

        for k in range(10):
            for i in range(10):
                tdSql.execute('insert into t2 values (%d , 1)' %(t0 + 200 + k * 10 + i))
                print('insert into t2 values (%d , 1)' %(t0 + 200 + k * 10 + i))


        print("==========step2")
        print('check query result before restart')
        tdSql.query('select * from db.t2;')
        for i in range(insert_rows*2+100):
            tdSql.checkData(i, 1, 1)
        # print(tdSql.queryResult)
        print('restart to commit')
        self.restart_taosd('db')
        print('check query result after restart')
        tdSql.query('select * from db.t2;')
        for i in range(insert_rows*2+100):
            tdSql.checkData(i, 1, 1)


        print("==================================3 start")
        print("==========step1")
        print("create table && insert data")
        tdSql.execute('create table t3 (ts timestamp, a int)')
        insert_rows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % insert_rows)
        for i in range(insert_rows):
            tdSql.execute('insert into t3 values (%d , 1)' %(t0+i))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(insert_rows):
            tdSql.execute('insert into t3 values (%d , 1)' %(t0+i+5000))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(5200):
            tdSql.execute('insert into t3 values (%d , 2)' %(t0+i))

        print("==========step2")
        print('check query result before restart')
        tdSql.query('select * from db.t3;')
        for i in range(5200):
            tdSql.checkData(i, 1, 2)
        # print(tdSql.queryResult)
        print('restart to commit')
        self.restart_taosd('db')
        print('check query result after restart')
        tdSql.query('select * from db.t3;')
        for i in range(5200):
            tdSql.checkData(i, 1, 2)

        print("==================================4 start")
        print("==========step1")
        print("create table && insert data")
        tdSql.execute('create table t4 (ts timestamp, a int)')
        insert_rows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % insert_rows)
        for i in range(insert_rows):
            tdSql.execute('insert into t4 values (%d , 1)' %(t0+i))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(insert_rows):
            tdSql.execute('insert into t4 values (%d , 1)' %(t0+i+5000))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(100):
            tdSql.execute('insert into t4 values (%d , 2)' %(t0+i))

        for i in range(200, 5000):
            tdSql.execute('insert into t4 values (%d , 2)' %(t0+i))

        for i in range(100):
            tdSql.execute('insert into t4 values (%d , 1)' %(t0+i+5000))

        print('check query result before restart')
        tdSql.query('select * from db.t4;')
        for i in range(100):
            tdSql.checkData(i, 1, 2)
        for i in range(100, 200):
            tdSql.checkData(i, 1, 1)
        for i in range(200, 5000):
            tdSql.checkData(i, 1, 2)
        for i in range(5000, 5200):
            tdSql.checkData(i, 1, 1)

        print('check query result after restart')
        self.restart_taosd('db')
        tdSql.query('select * from db.t4;')
        for i in range(100):
            tdSql.checkData(i, 1, 2)
        for i in range(100, 200):
            tdSql.checkData(i, 1, 1)
        for i in range(200, 5000):
            tdSql.checkData(i, 1, 2)
        for i in range(5000, 5200):
            tdSql.checkData(i, 1, 1)

        print("==================================5 start")
        print("==========step1")
        print("create table && insert data")
        tdSql.execute('create table t5 (ts timestamp, a int)')
        insert_rows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % insert_rows)
        for i in range(insert_rows):
            tdSql.execute('insert into t5 values (%d , 1)' %(t0+i))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(insert_rows):
            tdSql.execute('insert into t5 values (%d , 1)' %(t0+i+5000))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(100, 200):
            tdSql.execute('insert into t5 values (%d , 2)' %(t0+i))

        for i in range(200, 5000):
            tdSql.execute('insert into t5 values (%d , 2)' %(t0+i))

        for i in range(100, 200):
            tdSql.execute('insert into t5 values (%d , 2)' %(t0+i+5000))

        print('check query result before restart')
        tdSql.query('select * from db.t5;')
        for i in range(100):
            tdSql.checkData(i, 1, 1)
        for i in range(100, 5000):
            tdSql.checkData(i, 1, 2)
        for i in range(5000, 5100):
            tdSql.checkData(i, 1, 1)
        for i in range(5100, 5200):
            tdSql.checkData(i, 1, 2)

        print('check query result after restart')
        self.restart_taosd('db')
        tdSql.query('select * from db.t5;')
        for i in range(100):
            tdSql.checkData(i, 1, 1)
        for i in range(100, 5000):
            tdSql.checkData(i, 1, 2)
        for i in range(5000, 5100):
            tdSql.checkData(i, 1, 1)
        for i in range(5100, 5200):
            tdSql.checkData(i, 1, 2)

        print("==================================6 start")
        print("==========step1")
        print("create table && insert data")
        tdSql.execute('create table t6 (ts timestamp, a int)')
        insert_rows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % insert_rows)
        for i in range(insert_rows):
            tdSql.execute('insert into t6 values (%d , 1)' %(t0+i))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(insert_rows):
            tdSql.execute('insert into t6 values (%d , 1)' %(t0+i+5000))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(-1000, 10000):
            tdSql.execute('insert into t6 values (%d , 2)' %(t0+i))

        print('check query result before restart')
        tdSql.query('select * from db.t6;')
        tdSql.checkRows(11000)
        for i in range(11000):
            tdSql.checkData(i, 1, 2)

        print('check query result after restart')
        self.restart_taosd('db')
        tdSql.query('select * from db.t6;')
        tdSql.checkRows(11000)
        for i in range(11000):
            tdSql.checkData(i, 1, 2)


        print("==================================7 start")
        print("==========step1")
        print("create table && insert data")
        tdSql.execute('create table t7 (ts timestamp, a int)')
        insert_rows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % insert_rows)
        for i in range(insert_rows):
            tdSql.execute('insert into t7 values (%d , 1)' %(t0+i))

        for i in range(insert_rows):
            tdSql.execute('insert into t7 values (%d , 1)' %(t0+i+5000))
        print('restart to commit')
        self.restart_taosd('db')

        for i in range(-1000, 10000):
            tdSql.execute('insert into t7 values (%d , 2)' %(t0+i))

        print('check query result before restart')
        tdSql.query('select * from db.t7;')
        tdSql.checkRows(11000)
        for i in range(11000):
            tdSql.checkData(i, 1, 2)

        print('check query result after restart')
        self.restart_taosd('db')
        tdSql.query('select * from db.t7;')
        tdSql.checkRows(11000)
        for i in range(11000):
            tdSql.checkData(i, 1, 2)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
