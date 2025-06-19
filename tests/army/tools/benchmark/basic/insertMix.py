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
import os
import subprocess
import time

import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        [TD-13823] taosBenchmark test cases
        """
        return
        
    def checkDataCorrect(self):
        sql = "select count(*) from meters"
        tdSql.query(sql)
        allCnt = tdSql.getData(0, 0)
        if allCnt < 200000:
            tdLog.exit(f"taosbenchmark insert row small. row count={allCnt} sql={sql}")
            return 
        
        # group by 10 child table
        rowCnt = tdSql.query("select count(*),tbname from meters group by tbname")
        tdSql.checkRows(10)

        # interval
        sql = "select count(*),max(ic),min(dc),last(*) from meters interval(1s)"
        rowCnt = tdSql.query(sql)
        if rowCnt < 10:
            tdLog.exit(f"taosbenchmark interval(1s) count small. row cout={rowCnt} sql={sql}")
            return

        # nest query
        tdSql.query("select count(*) from (select * from meters order by ts desc)")
        tdSql.checkData(0, 0, allCnt)

        rowCnt = tdSql.query("select tbname, count(*) from meters partition by tbname slimit 11")
        if rowCnt != 10:
            tdLog.exit("partition by tbname should return 10 rows of table data which is " + str(rowCnt))
            return

    def insert(self, cmd):
        tdLog.info("%s" % cmd)
        errcode = os.system("%s" % cmd)
        if errcode != 0:
            tdLog.exit(f"execute taosBenchmark ret error code={errcode}")
            return 

        tdSql.execute("use mixdb")
        self.checkDataCorrect()   

    def run(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/insertMix.json" % binPath
        self.insert(cmd)
        cmd = "%s -f ./tools/benchmark/basic/json/insertMixOldRule.json" % binPath
        self.insert(cmd)
        cmd = "%s -f ./tools/benchmark/basic/json/insertMixAutoCreateTable.json" % binPath
        self.insert(cmd)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
