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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()
        tdSql.execute("create table t(ts timestamp, k int)")
        tdSql.execute("insert into t values('2021-1-1 1:1:1', 12);")
        
        tdSql.query("select interp(*) from t where ts='2021-1-1 1:1:1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 12)

        tdSql.error("select interp(*) from t where ts >'2021-1-1 1:1:1' and ts < now interval(1s) fill(next)")
 
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
