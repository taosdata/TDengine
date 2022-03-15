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
import taos
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def __init__(self):
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10
        self.Loop = 100

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: this is an abnormal case for loop restart taosd
        and basic this query ,it will run 4 client to change schema ,run always ;
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def basic_insert(self):
        tdSql.execute("create database if not exists testdb")
        tdSql.execute("use testdb")
        tdSql.execute("create stable st (ts timestamp ,  value int) tags (ind int)")
        for i in range(self.num):
            tdSql.execute("insert into sub_%s using st tags(%d) values (%d , %d );"%(str(i),i,self.ts+10000*i,i))

    def basic_query(self):
        for i in range(self.num):
            tdSql.query("select count(*) from testdb.sub_%s"%str(i))
            tdSql.checkData(0,0,1)
        tdSql.query("select count(*) from testdb.st")
        tdSql.checkRows(1)

    
    def run(self):
        
        # Loop 

        for loop_step in range(self.Loop):

            # run basic query and insert
            # kill all 
            os.system("ps -aux |grep 'taosd'  |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1")
            tdDnodes.start(1)
            if loop_step ==0:
                self.basic_insert()
            else:
                tdSql.execute("insert into sub_10 using st tags(10) values(now ,10)")
            
            # another client 
            os.system('taos -s "insert into testdb.sub_100 using testdb.st tags(100) values(now ,100);"')
            os.system('taos -s "select count(*) from testdb.sub_100;"')
            
            self.basic_query()
            sleep(2)
            tdDnodes.stopAll()
            

            tdLog.info(" this is the %s_th loop restart taosd going " % loop_step)



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
