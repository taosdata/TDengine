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


class TDTestRetetion:
    def init(self):
        self.queryRows=0
        tdLog.debug("start to execute %s" % __file__)
        tdLog.info("prepare cluster")
        tdDnodes.init("")
        tdDnodes.setTestCluster(False)
        tdDnodes.setValgrind(False)
        tdDnodes.stopAll()
        tdDnodes.deploy(1)
        tdDnodes.start(1)
        print(tdDnodes.getDnodesRootDir())
        self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        tdSql.init(self.conn.cursor())
        tdSql.execute('reset query cache')
    def checkRows(self, expectRows,sql):
        if self.queryRows == expectRows:
            tdLog.info("sql:%s, queryRows:%d == expect:%d" % (sql, self.queryRows, expectRows))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, self.queryRows, expectRows)
            os.system("sudo timedatectl set-ntp true")
            os.system("date -s '%s'"%(datetime.datetime.now()+datetime.timedelta(hours=1)))
            time.sleep(5)
            tdLog.exit("%s(%d) failed: sql:%s, queryRows:%d != expect:%d" % args)

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
        tdDnodes.stop(1)
        os.system("sudo timedatectl set-ntp false")
        os.system("date -s '%s'"%(datetime.datetime.now()+datetime.timedelta(hours=48)))
        tdDnodes.start(1)
        cmd = 'insert into test values(now,5);'
        tdDnodes.stop(1)
        tdDnodes.start(1)
        
        tdLog.info(cmd)
        tdSql.execute(cmd)
        self.queryRows=tdSql.query('select * from test')
        if self.queryRows==4:
            self.checkRows(4,cmd)
            return 0
        else:
            self.checkRows(5,cmd)
        tdLog.info("=============== step3")
        tdDnodes.stop(1)
        os.system("date -s '%s'"%(datetime.datetime.now()+datetime.timedelta(hours=48)))
        tdDnodes.start(1)
        tdLog.info(cmd)
        tdSql.execute(cmd)
        self.queryRows=tdSql.query('select * from test')
        if self.queryRows==4:
            self.checkRows(4,cmd)
            return 0
        cmd = 'insert into test values(now-1d,6);'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        self.queryRows=tdSql.query('select * from test')
        self.checkRows(6,cmd)
        tdLog.info("=============== step4")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        cmd = 'insert into test values(now,7);'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        self.queryRows=tdSql.query('select * from test')
        self.checkRows(7,cmd)

        tdLog.info("=============== step5")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        cmd='select * from test where ts > now-1d'
        self.queryRows=tdSql.query('select * from test where ts > now-1d')
        self.checkRows(1,cmd)

    def stop(self):
        os.system("sudo timedatectl set-ntp true")
        os.system("date -s '%s'"%(datetime.datetime.now()+datetime.timedelta(hours=1)))
        time.sleep(5)
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


clients = TDTestRetetion()
clients.init()
clients.run()
clients.stop()

