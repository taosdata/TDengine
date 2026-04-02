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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1625068800000000000   # this is timestamp  "2021-07-01 00:00:00"
        self.numberOfTables = 10
        self.numberOfRecords = 100

    def checkCommunity(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            return False
        else:
            return True

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosdump" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath
    
    

    def run(self):

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        # basic test for alter tags
        tdSql.execute("create database tagdb ")
        tdSql.execute(" use tagdb")
        tdSql.execute("create table st (ts timestamp , a int) tags (tg1 binary(20), tg2 binary(20), tg3 binary(20))")
        tdSql.execute("insert into t using st (tg3, tg2, tg1) tags ('tg3', 'tg2', 'tg1') values (now, 1)")
        tdSql.execute("alter table t set tag tg1='newtg1'")
        res = tdSql.getResult("select tg1,tg2,tg3 from t")

        if res == [('newtg1', 'tg2', 'tg3')]:
            tdLog.info(" alter tag check has pass!")
        else:
            tdLog.info(" alter tag failed , please check !")

        tdSql.error("alter stable st modify tag tg2 binary(2)")
        tdSql.execute("alter stable st modify tag tg2 binary(30) ")
        tdSql.execute("alter table t set tag tg2 = 'abcdefghijklmnopqrstuvwxyz1234'")
        res = tdSql.getResult("select tg1,tg2,tg3 from t")
        if res == [('newtg1', 'abcdefghijklmnopqrstuvwxyz1234', 'tg3')]:
            tdLog.info(" alter tag check has pass!")
        else:
            tdLog.info(" alter tag failed , please check !")

        # test boundary about tags
        tdSql.execute("create stable stb1 (ts timestamp , a int) tags (tg1 binary(16374))")
        tdSql.error("create stable stb1 (ts timestamp , a int) tags (tg1 binary(16375))")
        bound_sql = "create stable stb2 (ts timestamp , a int) tags (tg1 binary(10),"
        for i in range(127):
            bound_sql+="tag"+str(i)+" binary(10),"
        sql1 = bound_sql[:-1]+")"
        tdSql.execute(sql1)
        sql2 = bound_sql[:-1]+"tag127 binary(10))"
        tdSql.error(sql2)
        tdSql.execute("create stable stb3 (ts timestamp , a int) tags (tg1 nchar(4093))")
        tdSql.error("create stable stb3 (ts timestamp , a int) tags (tg1 nchar(4094))")
        tdSql.execute("create stable stb4 (ts timestamp , a int) tags (tg1 nchar(4093),tag2 binary(8))")
        tdSql.error("create stable stb4 (ts timestamp , a int) tags (tg1 nchar(4093),tag2 binary(9))")
        tdSql.execute("create stable stb5 (ts timestamp , a int) tags (tg1 nchar(4093),tag2 binary(4),tag3 binary(2))")
        tdSql.error("create stable stb5 (ts timestamp , a int) tags (tg1 nchar(4093),tag2 binary(4),tag3 binary(3))")

        tdSql.execute("create table stt (ts timestamp , a binary(100)) tags (tg1 binary(20), tg2 binary(20), tg3 binary(20))")
        tdSql.execute("insert into tt using stt (tg3, tg2, tg1) tags ('tg3', 'tg2', 'tg1') values (now, 1)")
        tags = "t"*16337
        sql3 = "alter table tt set tag tg1=" +"'"+tags+"'"
        tdSql.error(sql3)
        tdSql.execute("alter stable stt modify tag tg1 binary(16337)")
        tdSql.execute(sql3)
        res = tdSql.getResult("select tg1,tg2,tg3 from tt")
        if res == [(tags, 'tg2', 'tg3')]:
            tdLog.info(" alter tag check has pass!")
        else:
            tdLog.info(" alter tag failed , please check !")
        
        os.system("rm -rf ./tag_lite/TestModifyTag.py.sql")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())