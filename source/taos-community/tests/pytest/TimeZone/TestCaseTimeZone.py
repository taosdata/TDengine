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
import subprocess
import time
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import datetime


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
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath
        
    def run(self):
        tdSql.prepare()
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        tdSql.execute("create database timezone")
        tdSql.execute("use timezone")
        tdSql.execute("create stable st (ts timestamp, id int ) tags (index int)")

        tdSql.execute("insert into tb0 using st tags (1) values ('2021-07-01 00:00:00.000',0)")
        tdSql.query("select ts from tb0")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.000")

        tdSql.execute("insert into tb1 using st tags (1) values ('2021-07-01T00:00:00.000+07:50',1)")
        tdSql.query("select ts from tb1")
        tdSql.checkData(0, 0, "2021-07-01 00:10:00.000")

        tdSql.execute("insert into tb2 using st tags (1) values ('2021-07-01T00:00:00.000+08:00',2)")
        tdSql.query("select ts from tb2")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.000")
        
        tdSql.execute("insert into tb3 using st tags (1) values ('2021-07-01T00:00:00.000Z',3)")
        tdSql.query("select ts from tb3")
        tdSql.checkData(0, 0, "2021-07-01 08:00:00.000")
        
        tdSql.execute("insert into tb4 using st tags (1) values ('2021-07-01 00:00:00.000+07:50',4)")
        tdSql.query("select ts from tb4")
        tdSql.checkData(0, 0, "2021-07-01 00:10:00.000")

        tdSql.execute("insert into tb5 using st tags (1) values ('2021-07-01 00:00:00.000Z',5)")
        tdSql.query("select ts from tb5")
        tdSql.checkData(0, 0, "2021-07-01 08:00:00.000")

        tdSql.execute("insert into tb6 using st tags (1) values ('2021-07-01T00:00:00.000+0800',6)")
        tdSql.query("select ts from tb6")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.000")
        
        tdSql.execute("insert into tb7 using st tags (1) values ('2021-07-01 00:00:00.000+0800',7)")
        tdSql.query("select ts from tb7")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.000")

        tdSql.execute("insert into tb8 using st tags (1) values ('2021-07-0100:00:00.000',8)")
        tdSql.query("select ts from tb8")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.000")

        tdSql.execute("insert into tb9 using st tags (1) values ('2021-07-0100:00:00.000+0800',9)")
        tdSql.query("select ts from tb9")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.000")

        tdSql.execute("insert into tb10 using st tags (1) values ('2021-07-0100:00:00.000+08:00',10)")
        tdSql.query("select ts from tb10")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.000")
        
        tdSql.execute("insert into tb11 using st tags (1) values ('2021-07-0100:00:00.000+07:00',11)")
        tdSql.query("select ts from tb11")
        tdSql.checkData(0, 0, "2021-07-01 01:00:00.000")

        tdSql.execute("insert into tb12 using st tags (1) values ('2021-07-0100:00:00.000+0700',12)")
        tdSql.query("select ts from tb12")
        tdSql.checkData(0, 0, "2021-07-01 01:00:00.000")
        
        tdSql.execute("insert into tb13 using st tags (1) values ('2021-07-0100:00:00.000+07:12',13)")
        tdSql.query("select ts from tb13")
        tdSql.checkData(0, 0, "2021-07-01 00:48:00.000")
        
        tdSql.execute("insert into tb14 using st tags (1) values ('2021-07-0100:00:00.000+712',14)")
        tdSql.query("select ts from tb14")
        tdSql.checkData(0, 0, "2021-06-28 08:58:00.000")
        
        tdSql.execute("insert into tb15 using st tags (1) values ('2021-07-0100:00:00.000Z',15)")
        tdSql.query("select ts from tb15")
        tdSql.checkData(0, 0, "2021-07-01 08:00:00.000")

        tdSql.execute("insert into tb16 using st tags (1) values ('2021-7-1 00:00:00.000Z',16)")
        tdSql.query("select ts from tb16")
        tdSql.checkData(0, 0, "2021-07-01 08:00:00.000")
        
        tdSql.execute("insert into tb17 using st tags (1) values ('2021-07-0100:00:00.000+0750',17)")
        tdSql.query("select ts from tb17")
        tdSql.checkData(0, 0, "2021-07-01 00:10:00.000")
        
        tdSql.execute("insert into tb18 using st tags (1) values ('2021-07-0100:00:00.000+0752',18)")
        tdSql.query("select ts from tb18")
        tdSql.checkData(0, 0, "2021-07-01 00:08:00.000")

        tdSql.execute("insert into tb19 using st tags (1) values ('2021-07-0100:00:00.000+075',19)")
        tdSql.query("select ts from tb19")
        tdSql.checkData(0, 0, "2021-07-01 00:55:00.000")
        
        tdSql.execute("insert into tb20 using st tags (1) values ('2021-07-0100:00:00.000+75',20)")
        tdSql.query("select ts from tb20")
        tdSql.checkData(0, 0, "2021-06-28 05:00:00.000")

        tdSql.execute("insert into tb21 using st tags (1) values ('2021-7-1 1:1:1.234+075',21)")
        tdSql.query("select ts from tb21")
        tdSql.checkData(0, 0, "2021-07-01 01:56:01.234")

        tdSql.execute("insert into tb22 using st tags (1) values ('2021-7-1T1:1:1.234+075',22)")
        tdSql.query("select ts from tb22")
        tdSql.checkData(0, 0, "2021-07-01 01:56:01.234")

        tdSql.execute("insert into tb23 using st tags (1) values ('2021-7-131:1:1.234+075',22)")
        tdSql.query("select ts from tb23")
        tdSql.checkData(0, 0, "2021-07-13 01:56:01.234")

        
        tdSql.error("insert into tberror using st tags (1) values ('20210701 00:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('2021070100:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('202171 00:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('2021 07 01 00:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('2021 -07-0100:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('2021-7-11:1:1.234+075',0)")

        os.system("rm -rf ./TimeZone/*.py.sql")
        
        


        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
