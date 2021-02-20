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
import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 100
        self.ts = 1537146000000
        self.clist1 = []
        self.clist2 = []
        self.clist3 = []
        self.clist4 = []
        self.clist5 = []
        self.clist6 = []
    
    def getData(self):
        for i in range(tdSql.queryRows):
            for j in range(6):
                exec('self.clist{}.append(tdSql.queryResult[i][j+1])'.format(j+1))
        
    def run(self):
        tdSql.prepare()

        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(cid int,gbid binary(20),loc nchar(20))''')
        tdSql.execute("create table test1 using test tags(1,'beijing','北京')")
        tdSql.execute("create table test2 using test tags(2,'shanghai','深圳')")
        tdSql.execute("create table test3 using test tags(2,'shenzhen','深圳')")
        tdSql.execute("create table test4 using test tags(1,'shanghai','上海')")
        for j in range(4):
            for i in range(self.rowNum):
                tdSql.execute("insert into test%d values(now-%dh, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                            % (j+1,i, i + 1, i + 1, i + 1, i + 1, i + i * 0.1, i * 1.5, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))                      

        # stddev verifacation 
        tdSql.error("select stddev(ts) from test")
        tdSql.error("select stddev(col7) from test")
        tdSql.error("select stddev(col8) from test")
        tdSql.error("select stddev(col9) from test")

        con_list = [
            ' where cid = 1 and ts >=now - 1d and ts <now',
            " where gbid = 'beijing' and ts >=now - 1d and ts <now",                       
            ' '
        ]
        for condition in con_list:
            tdSql.query("select * from test %s"%(condition))
            self.getData()
            for i in range(6):
                exec('tdSql.query("select stddev(col{}) from test {}")'.format(i+1,condition))
                exec('tdSql.checkData(0, 0, np.std(self.clist{}))'.format(i+1))
                exec('self.clist{}.clear()'.format(i+1))
        print('step 2')
        con_group_list = {
            ' cid = 2 and ts >=now - 1d and ts <now group by tbname':2,
             " loc = '深圳' and ts >=now - 1d and ts <now group by tbname " :2 ,
             " gbid = 'shanghai' and ts >=now - 1d and ts <now group by cid " :2
        }
        result = [6.922186552,6.922186552,6.922186552,6.922186552,7.614405212,10.383279829]
        for key,value in con_group_list.items():
            for i in range(6):
                    exec('tdSql.query("select stddev(col{}) from test where {}")'.format(i+1,key))
                    for j in range(value):
                        tdSql.checkData(j, 0, result[i])
                      
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
