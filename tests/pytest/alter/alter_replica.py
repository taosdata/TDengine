###################################################################
#       Copyright (c) 2016 by TAOS Technologies, Inc.
#             All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self):
        tdLog.debug("start to execute %s" % __file__)
        tdLog.info("prepare cluster")
        tdDnodes.stopAll()
        tdDnodes.deploy(1)
        tdDnodes.start(1)

        self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        tdSql.init(self.conn.cursor())
        tdSql.execute('reset query cache')
        tdSql.execute('create dnode 192.168.0.2')
        tdDnodes.deploy(2)
        tdDnodes.start(2)

        self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        tdSql.init(self.conn.cursor())
        tdSql.execute('reset query cache')
        tdSql.execute('create dnode 192.168.0.3')
        tdDnodes.deploy(3)
        tdDnodes.start(3)

    def run(self):
        tdSql.execute('create database db replica 3 days 7')
        tdSql.execute('use db')
        for tid in range(1, 11):
            tdSql.execute('create table tb%d(ts timestamp, i int)' % tid)
        tdLog.sleep(10)

        tdLog.info("================= step1")
        startTime = 1520000010000
        for rid in range(1, 11):
            for tid in range(1, 11):
                tdSql.execute(
                    'insert into tb%d values(%ld, %d)' %
                    (tid, startTime, rid))
            startTime += 1
        tdSql.query('select * from tb1')
        tdSql.checkRows(10)
        tdLog.sleep(5)

        tdLog.info("================= step2")
        tdSql.execute('alter database db replica 2')
        tdLog.sleep(10)

        tdLog.info("================= step3")
        for rid in range(1, 11):
            for tid in range(1, 11):
                tdSql.execute(
                    'insert into tb%d values(%ld, %d)' %
                    (tid, startTime, rid))
            startTime += 1
        tdSql.query('select * from tb1')
        tdSql.checkRows(20)
        tdLog.sleep(5)

        tdLog.info("================= step4")
        tdSql.execute('alter database db replica 1')
        tdLog.sleep(10)

        tdLog.info("================= step5")
        for rid in range(1, 11):
            for tid in range(1, 11):
                tdSql.execute(
                    'insert into tb%d values(%ld, %d)' %
                    (tid, startTime, rid))
            startTime += 1
        tdSql.query('select * from tb1')
        tdSql.checkRows(30)
        tdLog.sleep(5)

        tdLog.info("================= step6")
        tdSql.execute('alter database db replica 2')
        tdLog.sleep(10)

        tdLog.info("================= step7")
        for rid in range(1, 11):
            for tid in range(1, 11):
                tdSql.execute(
                    'insert into tb%d values(%ld, %d)' %
                    (tid, startTime, rid))
            startTime += 1
        tdSql.query('select * from tb1')
        tdSql.checkRows(40)
        tdLog.sleep(5)

        tdLog.info("================= step8")
        tdSql.execute('alter database db replica 3')
        tdLog.sleep(10)

        tdLog.info("================= step9")
        for rid in range(1, 11):
            for tid in range(1, 11):
                tdSql.execute(
                    'insert into tb%d values(%ld, %d)' %
                    (tid, startTime, rid))
            startTime += 1
        tdSql.query('select * from tb1')
        tdSql.checkRows(50)
        tdLog.sleep(5)

    def stop(self):
        tdSql.close()
        self.conn.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addCluster(__file__, TDTestCase())
