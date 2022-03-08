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
        case1 <wenzhouwww>: [TD-12165]  
            this test case is an test case for unexpectd use way for alias _c0 ,it should be regarded as keywords ;
            
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

        # basic alis
        tdSql.error('select value ,value as _c0 from st;')  
        tdSql.error('select value _c0 from st;')
        tdSql.error('select ind ,ind as _c0 from st;')  
        tdSql.error('select ind _c0 from st;')
        tdSql.error('select ts ,ts as _c0 from st;')  
        tdSql.error('select ts _c0 from st;')
        tdSql.error('select value ,value as _c0 from tb1;')  
        tdSql.error('select value _c0 from tb2;')    
        tdSql.error('select ts ,ts as _c0 from tb1;')  
        tdSql.error('select ts _c0 from tb2;') 
       
        # nest query  alis name

        tdSql.error('select ts , ts _c0 from (select ts ,value from st);')  
        tdSql.error('select ts , ts as _c0 from (select ts ,value from tb1);')  
        tdSql.error('select ts , ts _c0 from (select * from st);')  
        tdSql.error('select ts , ts as _c0 from (select * from tb1);')  
        tdSql.error('select ts , _c0 from (select max(value) _c0 from st);')  
        tdSql.error('select ts , _c0 from (select max(value) _c0 from tb1);')
        tdSql.query('select _c0,data from (select max(value) data from tb1);')  
        tdSql.query('select _c0,data from (select max(value) data from st);')
        tdSql.query('select _c0,data from (select ts ,max(value) data from st);')
        tdSql.checkData(0,1,'19.0')
        tdSql.query('select _c0,data from (select ts ,max(value) data from tb1);')
        tdSql.checkData(0,1,'11.0')
        tdSql.query('select _c0,data from (select csum(value) data from tb1);')
        tdSql.checkData(0,1,'11.0')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


