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
        case1 <wenzhouwww>:[TD-12014] : 
            this test case is an test case for taoshell crash ， it will coredump when query such as "select 1*now from st "

        ''' 
        return 
   
    def run(self):
        tdSql.prepare()
        tdSql.execute("create database if not exists testdb keep 36500;")
        tdSql.execute("use testdb;")
        tdSql.execute("create stable st (ts timestamp , id int , value double) tags(hostname binary(10) ,ind int);")
        for i in range(self.num):
            tdSql.execute("insert into sub_%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+10000*i,i*2,i+10.00))

        tdSql.error('select 1*now+2d-3m from st;') 
        tdSql.error('select 1*now+2d-3m from sub_1;') 
        tdSql.error('select 1-now+2d-3m from st;') 
        tdSql.error('select 1*now+2d-3m from st;') 
        tdSql.error('select 1/now+2d-3m from st;') 
        tdSql.error('select 1%now+2d-3m from st;') 
        tdSql.error('select 1*now+2d-3m from sub_1;') 
        tdSql.error('select elapsed(ts)+now from st  group by tbname order by ts desc ;') 
        tdSql.error('select elapsed(ts)-now from st  group by tbname order by ts desc ;')
        tdSql.error('select elapsed(ts)*now from st  group by tbname order by ts desc ;')
        tdSql.error('select elapsed(ts)/now from st  group by tbname order by ts desc ;')
        tdSql.error('select elapsed(ts)%now from st  group by tbname order by ts desc ;')
        tdSql.error('select elapsed(ts)+now from sub_1 order by ts desc ;') 
        tdSql.error('select twa(value)+now from st order by ts desc ;') 
        tdSql.error('select max(value)*now from st ;') 
        tdSql.error('select max(value)*now from sub_1 ;') 
        tdSql.error('select max(value)*now+2d-3m from st;')

        tdSql.query('select max(value) from st where ts < now -2d +3m ;') 
        tdSql.checkRows(1)
        tdSql.query('select ts,value from st where ts < now -2d +3m ;') 
        tdSql.checkRows(10)
        tdSql.query('select max(value) from sub_1 where ts < now -2d +3m ;') 
        tdSql.checkRows(1)
        tdSql.query('select ts ,value from sub_1 where ts < now -2d +3m ;') 
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


