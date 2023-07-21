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

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        """
        [TD-13823] taosBenchmark test cases
        """
        return

    def init(self, conn, logSql, replicaVar=1):
        # comment off by Shuduo for CI self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        else:
            projPath = selfPath[: selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]
        
    def checkDataCorrect(self):
        sql = "select count(*) from meters"
        tdSql.query(sql)
        allCnt = tdSql.getData(0, 0)
        if allCnt < 2000000:
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


    def run(self):
        binPath = self.getPath()
        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/insertMix.json" % binPath
        tdLog.info("%s" % cmd)
        errcode = os.system("%s" % cmd)
        if errcode != 0:
            tdLog.exit(f"execute taosBenchmark ret error code={errcode}")
            return 

        tdSql.execute("use mixdb")
        self.checkDataCorrect()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
