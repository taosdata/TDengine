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
        case1 <wenzhouwww>: [TD-12164] 
            this test case is an test case for key timestamp colum , such as elapsed function ,it will occur unexpected results ;
            Root Cause: elapse parameter column is checked that both the index and id is 0 
            
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

        # basic query
        tdSql.query("select elapsed(ts) from st group by tbname ;  ")
        tdSql.query("select elapsed(ts) from tb1 ;  ")
        tdSql.error("select elapsed(ts) from tb1 group by tbname ;  ")
        tdSql.query("select elapsed(ts) from st group by tbname  order by ts;  ")
        tdSql.checkRows(10)
        tdSql.checkData(0,0,0)
        tdSql.checkData(1,0,9900)
        tdSql.checkData(9,0,89100)
        
        # nest query  
        tdSql.error('select elapsed(ts00 ,1s) from (select elapsed(ts,1s) ts00 from tb1) ;')
        tdSql.error('select elapsed(ts00 ,1s) from (select elapsed(ts,1s) ts00 from st group by tbname ) ;')
        tdSql.error('select elapsed(ts00 ,1s) from (select elapsed(ts,1s) ts00 from tb1 group by tbname ) ;')

        tdSql.query('select max(ts00) from (select elapsed(ts,1s) ts00 from st group by tbname ) ;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,89.1)

        tdSql.error('select elapsed(data) from (select elapsed(ts,1s) data from st group by tbname ) ;')
        tdSql.error('select elapsed(data) from (select elapsed(ts,1s) data from tb2 ) ;')

        tdSql.error('select elapsed(data) from (select ts data from st group by tbname ) ;')
        tdSql.error('select elapsed(data) from (select ts data from tb2 ) ;')

        tdSql.error('select elapsed(data) from (select value data from st group by tbname ) ;')
        tdSql.error('select elapsed(data) from (select value data from tb2 ) ;')

        tdSql.query('select elapsed(ts) from (select csum(value) data from tb2 ) ;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,19800)

        tdSql.query('select elapsed(ts) from (select diff(value) data from tb2 ) ;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,19600.0)

        # another bug : it will be forbidden in the feature .
        # tdSql.error('select elapsed(ts) from (select csum(value) data from st group by tbname ) ;')
     

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


