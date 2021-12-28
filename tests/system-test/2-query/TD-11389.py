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
        tdSql.execute("create database if not exists testdb keep 36500;")
        tdSql.execute("use testdb;")
        tdSql.execute("create stable st (ts timestamp , id int , value double) tags(hostname binary(10) ,ind int);")
        for i in range(self.num):
            tdSql.execute("insert into sub_%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+10000*i,i*2,i+10.00))
        tdSql.query("select distinct(hostname) from st;")
        tdSql.checkRows(10)

        binPath = self.getBuildPath() + "/build/bin/"
        os.system( "taos -s ' ALTER STABLE testdb.st MODIFY TAG hostname binary(100); '" )
        os.system("taos -s ' insert into testdb.sub_test using testdb.st tags(\"host_10000000000000000000\" , 100) values (now , 100 , 100.0 ); '")
        
    
        tdLog.info (" ===============The correct result should be 11 rows ,there is error query result ====================")

        os.system("taos -s ' select distinct(hostname) from testdb.st '")

        # this bug will occor at this connect  ,it should get 11 rows ,but return 10 rows ,this error is caused by cache 

        for i in range(10):
            
            tdSql.checkRows(11)     # query 10 times every 10 second , test cache refresh
            sleep(10)              
    

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


