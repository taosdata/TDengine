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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        print("==========step1")
        print("UPDATE THE WHOLE DATA BLOCK REPEATEDLY")
        s = 'reset query cache'
        tdSql.execute(s)
        s = 'drop database if exists db'
        tdSql.execute(s)
        s = 'create database db update 1 duration 30'
        tdSql.execute(s)
        s = 'use db'
        tdSql.execute(s)
        ret = tdSql.execute('create table t1 (ts timestamp, a int)')

        insertRows = 200
        t0 = 1603152000000
        tdLog.info("insert %d rows" % (insertRows))
        for i in range(0, insertRows):
          ret = tdSql.execute(
            'insert into t1 values (%d , 1)' %
            (t0 + i))
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t1")
        tdSql.checkRows(insertRows)

        for k in range(0,10):
          for i in range (0,insertRows):
            ret = tdSql.execute(
              'insert into t1 values(%d,1)' %
              (t0+i)
            )
          tdSql.query("select * from t1")
          tdSql.checkRows(insertRows)
          tdDnodes.stop(1)
          tdDnodes.start(1)
          tdSql.query("select * from t1")
          tdSql.checkRows(insertRows)
        print("==========step2")
        print("PREPEND DATA ")
        ret = tdSql.execute('create table t2 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t2 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t2")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t2")
        tdSql.checkRows(insertRows)
        for i in range(-100,0):
          ret = tdSql.execute(
                'insert into t2 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t2")
        tdSql.checkRows(insertRows+100)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t2")
        tdSql.checkRows(insertRows+100)
        print("==========step3")
        print("PREPEND MASSIVE DATA ")
        ret = tdSql.execute('create table t3 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t3 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t3")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t3")
        tdSql.checkRows(insertRows)
        for i in range(-6000,0):
          ret = tdSql.execute(
                'insert into t3 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t3")
        tdSql.checkRows(insertRows+6000)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t3")
        tdSql.checkRows(insertRows+6000)
        print("==========step4")
        print("APPEND DATA")
        ret = tdSql.execute('create table t4 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t4 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t4")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t4")
        tdSql.checkRows(insertRows)
        for i in range(0,100):
          ret = tdSql.execute(
                'insert into t4 values (%d , 1)' %
                (t0+200+i))
        tdSql.query("select * from t4")
        tdSql.checkRows(insertRows+100)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t4")
        tdSql.checkRows(insertRows+100)
        print("==========step5")
        print("APPEND DATA")
        ret = tdSql.execute('create table t5 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t5 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t5")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t5")
        tdSql.checkRows(insertRows)
        for i in range(0,6000):
          ret = tdSql.execute(
                'insert into t5 values (%d , 1)' %
                (t0+200+i))
        tdSql.query("select * from t5")
        tdSql.checkRows(insertRows+6000)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t5")
        tdSql.checkRows(insertRows+6000)
        print("==========step6")
        print("UPDATE BLOCK IN TWO STEP")
        ret = tdSql.execute('create table t6 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t6 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t6")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t6")
        tdSql.checkRows(insertRows)
        for i in range(0,100):
          ret = tdSql.execute(
                'insert into t6 values (%d , 2)' %
                (t0+i))
        tdSql.query("select * from t6")
        tdSql.checkRows(insertRows)
        tdSql.query("select sum(a) from t6")
        tdSql.checkData(0,0,'300')
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t6")
        tdSql.checkRows(insertRows)
        tdSql.query("select sum(a) from t6")
        tdSql.checkData(0,0,'300')
        for i in range(0,200):
          ret = tdSql.execute(
                'insert into t6 values (%d , 2)' %
                (t0+i))
        tdSql.query("select * from t6")
        tdSql.checkRows(insertRows)
        tdSql.query("select sum(a) from t6")
        tdSql.checkData(0,0,'400')
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t6")
        tdSql.checkRows(insertRows)
        tdSql.query("select sum(a) from t6")
        tdSql.checkData(0,0,'400')
        print("==========step7")
        print("UPDATE LAST HALF AND INSERT LITTLE DATA")
        ret = tdSql.execute('create table t7 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t7 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t7")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t7")
        tdSql.checkRows(insertRows)
        for i in range(100,300):
          ret = tdSql.execute(
                'insert into t7 values (%d , 2)' %
                (t0+i))
        tdSql.query("select * from t7")
        tdSql.checkRows(300)
        tdSql.query("select sum(a) from t7")
        tdSql.checkData(0,0,'500')
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t7")
        tdSql.checkRows(300)
        tdSql.query("select sum(a) from t7")
        tdSql.checkData(0,0,'500')
        print("==========step8")
        print("UPDATE LAST HALF AND INSERT MASSIVE DATA")
        ret = tdSql.execute('create table t8 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t8 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t8")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t8")
        tdSql.checkRows(insertRows)
        for i in range(6000):
          ret = tdSql.execute(
                'insert into t8 values (%d , 2)' %
                (t0+i))
        tdSql.query("select * from t8")
        tdSql.checkRows(6000)
        tdSql.query("select sum(a) from t8")
        tdSql.checkData(0,0,'12000')
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t8")
        tdSql.checkRows(6000)
        tdSql.query("select sum(a) from t8")
        tdSql.checkData(0,0,'12000')
        print("==========step9")
        print("UPDATE FIRST HALF AND PREPEND LITTLE DATA")
        ret = tdSql.execute('create table t9 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t9 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t9")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t9")
        tdSql.checkRows(insertRows)
        for i in range(-100,100):
          ret = tdSql.execute(
                'insert into t9 values (%d , 2)' %
                (t0+i))
        tdSql.query("select * from t9")
        tdSql.checkRows(300)
        tdSql.query("select sum(a) from t9")
        tdSql.checkData(0,0,'500')
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t9")
        tdSql.checkRows(300)
        tdSql.query("select sum(a) from t9")
        tdSql.checkData(0,0,'500')
        print("==========step10")
        print("UPDATE FIRST HALF AND PREPEND MASSIVE DATA")
        ret = tdSql.execute('create table t10 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t10 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t10")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t10")
        tdSql.checkRows(insertRows)
        for i in range(-6000,100):
          ret = tdSql.execute(
                'insert into t10 values (%d , 2)' %
                (t0+i))
        tdSql.query("select * from t10")
        tdSql.checkRows(6200)
        tdSql.query("select sum(a) from t10")
        tdSql.checkData(0,0,'12300')
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t10")
        tdSql.checkRows(6200)
        tdSql.query("select sum(a) from t10")
        tdSql.checkData(0,0,'12300')
        print("==========step11")
        print("UPDATE FIRST HALF AND APPEND MASSIVE DATA")
        ret = tdSql.execute('create table t11 (ts timestamp, a int)')
        insertRows = 200
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into t11 values (%d , 1)' %
                (t0+i))
        tdSql.query("select * from t11")
        tdSql.checkRows(insertRows)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t11")
        tdSql.checkRows(insertRows)
        for i in range(100):
          ret = tdSql.execute(
                'insert into t11 values (%d , 2)' %
                (t0+i))
        for i in range(200,6000):
          ret = tdSql.execute(
                'insert into t11 values (%d , 2)' %
                (t0+i))
        tdSql.query("select * from t11")
        tdSql.checkRows(6000)
        tdSql.query("select sum(a) from t11")
        tdSql.checkData(0,0,'11900')
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t11")
        tdSql.checkRows(6000)
        tdSql.query("select sum(a) from t11")
        tdSql.checkData(0,0,'11900')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
