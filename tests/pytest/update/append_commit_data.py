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
        print("create table && insert data")
        s = 'reset query cache'
        tdSql.execute(s)
        s = 'drop database if exists db'
        tdSql.execute(s)
        s = 'create database db  update 1'
        tdSql.execute(s)
        s = 'use db'
        tdSql.execute(s)
        ret = tdSql.execute('create table t1 (ts timestamp, a int)')

        insertRows = 200
        t0 = 1604298064000
        tdLog.info("insert %d rows" % (insertRows))
        for i in range(0, insertRows):
          ret = tdSql.execute(
              'insert into t1 values (%d , 1)' %
              (t0+i))
        print("==========step2")
        print("restart to commit ")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from db.t1")
        tdSql.checkRows(insertRows)
        for k in range(0,100):
          tdLog.info("insert %d rows" % (insertRows))
          for i in range (0,insertRows):
            ret = tdSql.execute(
                'insert into db.t1 values(%d,1)' %
                (t0+k*200+i)
            )
          tdDnodes.stop(1)
          tdDnodes.start(1)
          tdSql.query("select * from db.t1")
          tdSql.checkRows(insertRows+200*k)
        print("==========step2")
        print("insert into another table ")
        s = 'use db'
        tdSql.execute(s)
        ret = tdSql.execute('create table t2 (ts timestamp, a int)')
        insertRows = 20000
        for i in range(0, insertRows):
          ret = tdSql.execute(
              'insert into t2 values (%d, 1)' %
              (t0+i))
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from t2")
        tdSql.checkRows(insertRows)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
