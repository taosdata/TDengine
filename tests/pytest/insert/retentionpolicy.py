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
import os
import datetime

sys.path.insert(0, os.getcwd())
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def checkRows(self, expectRows, sql):
        if self.queryRows == expectRows:
            tdLog.info("sql:%s, queryRows:%d == expect:%d" %
                       (sql, self.queryRows, expectRows))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, self.queryRows,
                    expectRows)
            os.system("sudo timedatectl set-ntp true")
            os.system("date -s '%s'" %
                      (datetime.datetime.now() + datetime.timedelta(hours=1)))
            time.sleep(5)
            tdLog.exit("%s(%d) failed: sql:%s, queryRows:%d != expect:%d" %
                       args)

    def run(self):

        tdLog.info("=============== step1")
        tdSql.execute('create database test keep 3 days 1;')
        tdSql.execute('use test;')
        tdSql.execute('create table test(ts timestamp,i int);')

        cmd = 'insert into test values(now-2d,1)(now-1d,2)(now,3)(now+1d,4);'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from test')
        tdSql.checkRows(4)

        tdLog.info("=============== step2")
        tdDnodes.stopAll()
        os.system("sudo timedatectl set-ntp false")
        os.system("date -s '%s'" %
                  (datetime.datetime.now() + datetime.timedelta(hours=48)))
        tdDnodes.start()
        cmd = 'insert into test values(now,5);'
        tdDnodes.stopAll()
        tdDnodes.start()

        tdLog.info(cmd)
        ttime = datetime.datetime.now()
        tdSql.execute(cmd)
        self.queryRows = tdSql.query('select * from test')
        self.checkRows(3, cmd)
        tdLog.info("=============== step3")
        tdDnodes.stopAll()
        os.system("date -s '%s'" %
                  (datetime.datetime.now() + datetime.timedelta(hours=48)))
        tdDnodes.start()
        tdLog.info(cmd)
        tdSql.execute(cmd)
        self.queryRows = tdSql.query('select * from test')
        if self.queryRows == 4:
            self.checkRows(4, cmd)
            return 0
        cmd = 'insert into test values(now-1d,6);'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        self.queryRows = tdSql.query('select * from test')
        self.checkRows(3, cmd)
        tdLog.info("=============== step4")
        tdDnodes.stopAll()
        tdDnodes.start()
        cmd = 'insert into test values(now,7);'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        self.queryRows = tdSql.query('select * from test')
        self.checkRows(4, cmd)

        tdLog.info("=============== step5")
        tdDnodes.stopAll()
        tdDnodes.start()
        cmd = 'select * from test where ts > now-1d'
        self.queryRows = tdSql.query('select * from test where ts > now-1d')
        self.checkRows(2, cmd)

        tdLog.info("=============== step6")
        tdDnodes.stopAll()
        os.system("date -s '%s'" %
                  (ttime + datetime.timedelta(seconds=(72 * 60 * 60 - 7))))
        tdDnodes.start()
        while datetime.datetime.now() < (
                ttime + datetime.timedelta(seconds=(72 * 60 * 60 - 1))):
            time.sleep(0.001)
        cmd = 'select * from test'
        self.queryRows = tdSql.query(cmd)
        self.checkRows(4, cmd)
        while datetime.datetime.now() <= (ttime +
                                          datetime.timedelta(hours=72)):
            time.sleep(0.001)
        time.sleep(0.01)
        cmd = 'select * from test'
        self.queryRows = tdSql.query(cmd)
        print(tdSql.queryResult)
        self.checkRows(3, cmd)

    def stop(self):
        os.system("sudo timedatectl set-ntp true")
        os.system("date -s '%s'" %
                  (datetime.datetime.now() + datetime.timedelta(hours=1)))
        time.sleep(5)
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
