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
import subprocess

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
        case1 <wenzhouwww>:[TD-12593] : 
            this test case is an value error about nest query and inner query sort .
        ''' 
        return 

   
    def run(self):
        tdSql.prepare()
        tdSql.execute("create database if not exists testdb keep 36500;")
        tdSql.execute("use testdb;")
        tdSql.execute("create stable st (ts timestamp , id int , value double) tags(hostname binary(10) ,ind int);")
        for i in range(self.num):
            tdSql.execute("insert into tb%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+100*i,i*2,i+10.00))
            tdSql.execute("insert into tb%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+200*i,i*2,i+10.00))
            tdSql.execute("insert into tb%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+300*i,i*2,i+10.00))
            tdSql.execute("insert into tb%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+10000*i,i*2,i+10.00))

        tdSql.query('select * from (select csum(value) from tb1 order by ts  asc );')
        tdSql.checkRows(4)
        tdSql.checkData(0,1,11.000000000)
        tdSql.checkData(1,1,22.000000000)
        tdSql.checkData(2,1,33.000000000)
        tdSql.checkData(3,1,44.000000000)
        
        tdSql.query('select * from (select csum(value) from tb1 order by ts  desc );')
        tdSql.checkRows(4)
        tdSql.checkData(0,1,44.000000000)
        tdSql.checkData(1,1,33.000000000)
        tdSql.checkData(2,1,22.000000000)
        tdSql.checkData(3,1,11.000000000)



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())