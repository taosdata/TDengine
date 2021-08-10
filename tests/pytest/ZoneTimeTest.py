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
import datetime

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

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

        # clear envs

        tdSql.execute(" create database ZoneTime precision 'us' ")
        tdSql.execute(" use ZoneTime ")
        tdSql.execute(" create stable st (ts timestamp ,  id int , val float) tags (tag1 timestamp ,tag2 int) ")

        # standard case for Timestamp

        tdSql.execute(" insert into tb1 using st tags (\"2021-07-01 00:00:00.000\" , 2) values( \"2021-07-01 00:00:00.000\" , 1 , 1.0 ) ")
        case1 = (tdSql.getResult("select * from tb1"))
        print(case1)
        if case1 == [(datetime.datetime(2021, 7, 1, 0, 0), 1, 1.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01 00:00:00.000' ")

        # RCF-3339 : it allows "T" is replaced by " "

        tdSql.execute(" insert into tb2 using st tags (\"2021-07-01T00:00:00.000+07:50\" , 2) values( \"2021-07-01T00:00:00.000+07:50\" , 2 , 2.0 ) ")
        case2 = (tdSql.getResult("select * from tb2"))
        print(case2)
        if case2 == [(datetime.datetime(2021, 7, 1, 0, 10), 2, 2.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01T00:00:00.000+07:50'! ")

        tdSql.execute(" insert into tb3 using st tags (\"2021-07-01T00:00:00.000+08:00\" , 3) values( \"2021-07-01T00:00:00.000+08:00\" , 3 , 3.0 ) ")
        case3 = (tdSql.getResult("select * from tb3"))
        print(case3)
        if case3 == [(datetime.datetime(2021, 7, 1, 0, 0), 3, 3.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01T00:00:00.000+08:00'! ")

        tdSql.execute(" insert into tb4 using st tags (\"2021-07-01T00:00:00.000Z\" , 4) values( \"2021-07-01T00:00:00.000Z\" , 4 , 4.0 ) ")
        case4 = (tdSql.getResult("select * from tb4"))
        print(case4)
        if case4 == [(datetime.datetime(2021, 7, 1, 8, 0), 4, 4.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01T00:00:00.000Z'! ")

        tdSql.execute(" insert into tb5 using st tags (\"2021-07-01 00:00:00.000+07:50\" , 5) values( \"2021-07-01 00:00:00.000+07:50\" , 5 , 5.0 ) ")
        case5 = (tdSql.getResult("select * from tb5"))
        print(case5)
        if case5 == [(datetime.datetime(2021, 7, 1, 0, 10), 5, 5.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01 00:00:00.000+08:00 ")

        tdSql.execute(" insert into tb6 using st tags (\"2021-07-01 00:00:00.000Z\" , 6) values( \"2021-07-01 00:00:00.000Z\" , 6 , 6.0 ) ")
        case6 = (tdSql.getResult("select * from tb6"))
        print(case6)
        if case6 == [(datetime.datetime(2021, 7, 1, 8, 0), 6, 6.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01 00:00:00.000Z'! ")
        
        # ISO 8610 timestamp format , time days and hours must be split by "T"

        tdSql.execute(" insert into tb7 using st tags (\"2021-07-01T00:00:00.000+0800\" , 7) values( \"2021-07-01T00:00:00.000+0800\" , 7 , 7.0 ) ")
        case7 = (tdSql.getResult("select * from tb7"))
        print(case7)
        if case7 == [(datetime.datetime(2021, 7, 1, 0, 0), 7, 7.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01T00:00:00.000+0800'! ")
        
        tdSql.execute(" insert into tb8 using st tags (\"2021-07-01T00:00:00.000+08\" , 8) values( \"2021-07-01T00:00:00.000+08\" , 8 , 8.0 ) ")
        case8 = (tdSql.getResult("select * from tb8"))
        print(case8)
        if case8 == [(datetime.datetime(2021, 7, 1, 0, 0), 8, 8.0)]:
            print ("check pass! ")
        else: 
            print ("check failed about timestamp '2021-07-01T00:00:00.000+08'! ")

        # Non-standard case for Timestamp

        tdSql.execute(" insert into tb9 using st tags (\"2021-07-01 00:00:00.000+0800\" , 9) values( \"2021-07-01 00:00:00.000+0800\" , 9 , 9.0 ) ")
        case9 = (tdSql.getResult("select * from tb9"))
        print(case9)

        tdSql.execute(" insert into tb10 using st tags (\"2021-07-0100:00:00.000\" , 10) values( \"2021-07-0100:00:00.000\" , 10 , 10.0 ) ")
        case10 = (tdSql.getResult("select * from tb10"))
        print(case10)

        tdSql.execute(" insert into tb11 using st tags (\"2021-07-0100:00:00.000+0800\" , 11) values( \"2021-07-0100:00:00.000+0800\" , 11 , 11.0 ) ")
        case11 = (tdSql.getResult("select * from tb11"))
        print(case11)

        tdSql.execute(" insert into tb12 using st tags (\"2021-07-0100:00:00.000+08:00\" , 12) values( \"2021-07-0100:00:00.000+08:00\" , 12 , 12.0 ) ")
        case12 = (tdSql.getResult("select * from tb12"))
        print(case12)

        tdSql.execute(" insert into tb13 using st tags (\"2021-07-0100:00:00.000Z\" , 13) values( \"2021-07-0100:00:00.000Z\" , 13 , 13.0 ) ")
        case13 = (tdSql.getResult("select * from tb13"))
        print(case13)

        tdSql.execute(" insert into tb14 using st tags (\"2021-07-0100:00:00.000Z\" , 14) values( \"2021-07-0100:00:00.000Z\" , 14 , 14.0 ) ")
        case14 = (tdSql.getResult("select * from tb14"))
        print(case14)

        tdSql.execute(" insert into tb15 using st tags (\"2021-07-0100:00:00.000+08\" , 15) values( \"2021-07-0100:00:00.000+08\" , 15 , 15.0 ) ")
        case15 = (tdSql.getResult("select * from tb15"))
        print(case15)

        tdSql.execute(" insert into tb16 using st tags (\"2021-07-0100:00:00.000+07:50\" , 16) values( \"2021-07-0100:00:00.000+07:50\" , 16 , 16.0 ) ")
        case16 = (tdSql.getResult("select * from tb16"))
        print(case16)

        os.system("rm -rf *.py.sql")
        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
