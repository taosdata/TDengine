###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from posixpath import split
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
        
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10

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


    def caseDescription(self):

        '''
        case1 <wenzhouwww>: [TD-11389] : 
            this test case is an test case for cache error , it will let  the cached data obtained by the client that has connected to taosd incorrect，
            root cause : table schema is changed, tag hostname size is increased through schema-less insertion. The schema cache of client taos is not refreshed.

        ''' 
        return 

   
    def run(self):
        tdSql.prepare()
        tdSql.execute("use db")

        functions_list = ["count","avg","twa","irate","sum","stddev","LEASTSQUARES","min","max","first","last","last_row()","top","bottom",
        "spread","ceil","floor","round"]
        os.system("taosBenchmark -t 10 -n 5 -y ")

        params_list = ["(,)","(,,)","(current)","(current,,,)*tbname","(*),","(abc,,)"]
        for func in functions_list:
            for params in params_list:
                sql = "select %s%s from test.meters;"%(func,params)
                tdSql.error(sql)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


