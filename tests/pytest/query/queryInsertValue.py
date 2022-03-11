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
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.numOfRecords = 10
        self.ts = 1537146000000    
    
    def restartTaosd(self):
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.execute("use db")

    def run(self):
        tdSql.prepare()

        print("==============step1")

        tdSql.execute(
            "create table st (ts timestamp, speed int) tags(areaid int, loc nchar(20))")
        tdSql.execute("create table t1 using st tags(1, 'beijing')")
        tdSql.execute("insert into t1 values(now, 1)")
        tdSql.query("select * from st")
        tdSql.checkRows(1)

        tdSql.execute("alter table st add column len int")
        tdSql.execute("insert into t1 values(now, 1, 2)")
        tdSql.query("select last(*) from st")
        tdSql.checkData(0, 2, 2);

        self.restartTaosd();

        tdSql.query("select last(*) from st")
        tdSql.checkData(0, 2, 2);


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
