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
from util.dnodes import tdDnodes
from datetime import datetime

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):
        tdSql.prepare()
        tdSql.query('show databases')
        tdSql.checkData(0,15,0)
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        os.system("%staosdemo -f tools/taosdemoAllTest/insert_100M_rows.json -y " % binPath)
        # tdSql.query('select * from stb')
        # tdSql.checkRows(1000*10000)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.execute('use db')
        # tdSql.query('select * from stb')
        # tdSql.checkRows(1000*10000)
        tableName = []
        for i in range(500):
            tableName.append(f"stb_{i}")
        
        tdSql.execute('use db')
        lastRow_Off_start = datetime.now()

        slow = 0
        for i in range(5):        
            for i in range(100):
                for i in range(500):
                    tdSql.execute(f'SELECT LAST_ROW(*) FROM {tableName[i]}')
            lastRow_Off_end = datetime.now()

            tdLog.debug(f'time used:{lastRow_Off_end-lastRow_Off_start}')

            tdSql.execute('alter database db cachelast 1')
            tdSql.query('show databases')
            tdSql.checkData(0,15,1)

            # tdDnodes.stop(1)
            # tdDnodes.start(1)
        
            tdSql.execute('use db')
            lastRow_On_start = datetime.now()
            for i in range(100):
                for i in range(500):
                    tdSql.execute(f'SELECT LAST_ROW(*) FROM {tableName[i]}')
            lastRow_On_end = datetime.now()
                
            tdLog.debug(f'time used:{lastRow_On_end-lastRow_On_start}')

            if (lastRow_Off_end-lastRow_Off_start > lastRow_On_end-lastRow_On_start):
                pass
            else:
                slow += 1
            tdLog.debug(slow)
        if slow > 1:
            tdLog.exit('lastRow hot alter failed')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
