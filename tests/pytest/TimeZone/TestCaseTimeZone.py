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
        res = tdSql.getResult("select * from tb0")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 0), 0)]:
            tdLog.info("time format is check pass : '2021-07-01 00:00:00.000' ")
        else:
            tdLog.info("  '2021-07-01 00:00:00.000'  failed   ")

        tdSql.execute("insert into tb1 using st tags (1) values ('2021-07-01T00:00:00.000+07:50',1)")
        res = tdSql.getResult("select * from tb1")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 10), 1)]:
            tdLog.info("time format is check pass : '2021-07-01T00:00:00.000+07:50' ")
        else:
            tdLog.info("  '2021-07-01T00:00:00.000+07:50'  failed   ")
        
        tdSql.execute("insert into tb2 using st tags (1) values ('2021-07-01T00:00:00.000+08:00',2)")
        res = tdSql.getResult("select * from tb2")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 0), 2)]:
            tdLog.info("time format is check pass : '2021-07-01T00:00:00.000+08:00' ")
        else:
            tdLog.info("  '2021-07-01T00:00:00.000+08:00'  failed   ")

        tdSql.execute("insert into tb3 using st tags (1) values ('2021-07-01T00:00:00.000Z',3)")
        res = tdSql.getResult("select * from tb3")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 8, 0), 3)]:
            tdLog.info("time format is check pass : '2021-07-01T00:00:00.000Z' ")
        else:
            tdLog.info("  '2021-07-01T00:00:00.000Z'  failed   ")
        
        tdSql.execute("insert into tb4 using st tags (1) values ('2021-07-01 00:00:00.000+07:50',4)")
        res = tdSql.getResult("select * from tb4")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 10), 4)]:
            tdLog.info("time format is check pass : '2021-07-01 00:00:00.000+07:50' ")
        else:
            tdLog.info("  '2021-07-01 00:00:00.000+07:50'  failed   ")

        tdSql.execute("insert into tb5 using st tags (1) values ('2021-07-01 00:00:00.000Z',5)")
        res = tdSql.getResult("select * from tb5")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 8, 0), 5)]:
            tdLog.info("time format is check pass : '2021-07-01 00:00:00.000Z' ")
        else:
            tdLog.info("  '2021-07-01 00:00:00.000Z'  failed   ")

        tdSql.execute("insert into tb6 using st tags (1) values ('2021-07-01T00:00:00.000+0800',6)")
        res = tdSql.getResult("select * from tb6")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 0), 6)]:
            tdLog.info("time format is check pass : '2021-07-01T00:00:00.000+0800' ")
        else:
            tdLog.info("  '2021-07-01T00:00:00.000+0800'  failed   ")
        
        tdSql.execute("insert into tb7 using st tags (1) values ('2021-07-01 00:00:00.000+0800',7)")
        res = tdSql.getResult("select * from tb7")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 0), 7)]:
            tdLog.info("time format is check pass : '2021-07-01 00:00:00.000+0800' ")
        else:
            tdLog.info("  '2021-07-01 00:00:00.000+0800'  failed   ")

        tdSql.execute("insert into tb8 using st tags (1) values ('2021-07-0100:00:00.000',8)")
        res = tdSql.getResult("select * from tb8")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 0), 8)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000'  failed   ")

        tdSql.execute("insert into tb9 using st tags (1) values ('2021-07-0100:00:00.000+0800',9)")
        res = tdSql.getResult("select * from tb9")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 0), 9)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+0800' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+0800'  failed   ")

        tdSql.execute("insert into tb10 using st tags (1) values ('2021-07-0100:00:00.000+08:00',10)")
        res = tdSql.getResult("select * from tb10")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 0), 10)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+08:00' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+08:00'  failed   ")
        
        tdSql.execute("insert into tb11 using st tags (1) values ('2021-07-0100:00:00.000+07:00',11)")
        res = tdSql.getResult("select * from tb11")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 1, 0), 11)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+07:00' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+07:00'  failed   ")

        tdSql.execute("insert into tb12 using st tags (1) values ('2021-07-0100:00:00.000+07:00',12)")
        res = tdSql.getResult("select * from tb12")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 1, 0), 12)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+07' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+07'  failed   ")
        
        tdSql.execute("insert into tb13 using st tags (1) values ('2021-07-0100:00:00.000+07:12',13)")
        res = tdSql.getResult("select * from tb13")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 48), 13)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+07:12' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+07:12'  failed   ")
        
        tdSql.execute("insert into tb14 using st tags (1) values ('2021-07-0100:00:00.000+712',14)")
        res = tdSql.getResult("select * from tb14")
        print(res)
        if res == [(datetime.datetime(2021, 6, 28, 8, 58), 14)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+712' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+712'  failed   ")
        
        tdSql.execute("insert into tb15 using st tags (1) values ('2021-07-0100:00:00.000Z',15)")
        res = tdSql.getResult("select * from tb15")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 8, 0), 15)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000Z' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000Z'  failed   ")

        tdSql.execute("insert into tb16 using st tags (1) values ('2021-7-1 00:00:00.000Z',16)")
        res = tdSql.getResult("select * from tb16")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 8, 0), 16)]:
            tdLog.info("time format is check pass : '2021-7-1 00:00:00.000Z' ")
        else:
            tdLog.info("  '2021-7-1 00:00:00.000Z'  failed   ")
        
        tdSql.execute("insert into tb17 using st tags (1) values ('2021-07-0100:00:00.000+0750',17)")
        res = tdSql.getResult("select * from tb17")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 10), 17)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+0750' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+0750'  failed   ")
        
        tdSql.execute("insert into tb18 using st tags (1) values ('2021-07-0100:00:00.000+0752',18)")
        res = tdSql.getResult("select * from tb18")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 8), 18)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+0752' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+0752'  failed   ")

        tdSql.execute("insert into tb19 using st tags (1) values ('2021-07-0100:00:00.000+075',19)")
        res = tdSql.getResult("select * from tb19")
        print(res)
        if res == [(datetime.datetime(2021, 7, 1, 0, 55), 19)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+075' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+075'  failed   ")
        
        tdSql.execute("insert into tb20 using st tags (1) values ('2021-07-0100:00:00.000+75',20)")
        res = tdSql.getResult("select * from tb20")
        print(res)
        if res == [(datetime.datetime(2021, 6, 28, 5, 0), 20)]:
            tdLog.info("time format is check pass : '2021-07-0100:00:00.000+75' ")
        else:
            tdLog.info("  '2021-07-0100:00:00.000+75'  failed   ")

        
        tdSql.error("insert into tberror using st tags (1) values ('20210701 00:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('2021070100:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('202171 00:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('2021 07 01 00:00:00.000+0800',0)")
        tdSql.error("insert into tberror using st tags (1) values ('2021 -07-0100:00:00.000+0800',0)")
        
        

        
        
        

        

        


        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
