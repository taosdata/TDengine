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
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
import random


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use d")
        
    def run(self):
        tdSql.execute("drop database if exists d");
        tdSql.execute("create database d");
        tdSql.execute("use d");
        tdSql.execute("create table st(ts timestamp, f int) tags (t int)")
        
        for i in range(-2048, 2047):
            ts = 1626624000000 + i;
            tdSql.execute(f"insert into ct1 using st tags(1) values({ts}, {i})")
            
        tdSql.execute("flush database d")
        for i in range(1638):
            ts = 1648758398208 + i
            tdSql.execute(f"insert into ct1 using st tags(1) values({ts}, {i})")
        tdSql.execute("insert into ct1 using st tags(1) values(1648970742528, 1638)")
        tdSql.execute("flush database d")
        
        tdSql.query("select count(ts) from ct1 interval(17n, 5n)")           
        self.restartTaosd()
        tdSql.query("select count(ts) from ct1 interval(17n, 5n)")           

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())