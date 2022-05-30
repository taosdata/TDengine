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

from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.rowNum = 10
        self.ts = 1537146000000
        
    def check_apercentile(self,data,expect_data,param,percent,column):
        if param == "default":
            if abs((expect_data-data) <= expect_data * 0.2):
                tdLog.info(f"apercentile function values check success with col{column}, param = {param},percent = {percent}")
            else:
                tdLog.notice(f"apercentile function value has not as expected with col{column}, param = {param},percent = {percent}")
                sys.exit(1)
        elif param == "t-digest":
            if abs((expect_data-data) <= expect_data * 0.2):
                tdLog.info(f"apercentile function values check success with col{column}, param = {param},percent = {percent}")
            else:
                tdLog.notice(f"apercentile function value has not as expected with col{column}, param = {param},percent = {percent}")
                sys.exit(1)

    def run(self):
        tdSql.prepare()

        intData = []        
        floatData = []
        percent_list = [0,50,100]
        param_list = ['default','t-digest']
        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.rowNum):
            tdSql.execute("insert into test values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            intData.append(i + 1)            
            floatData.append(i + 0.1)                        

        # percentile verifacation 
        
        tdSql.error("select apercentile(ts ,20) from test")
        tdSql.error("select apercentile(col7 ,20) from test") 
        tdSql.error("select apercentile(col8 ,20) from test")
        tdSql.error("select apercentile(col9 ,20) from test")    

        column_list = [1,2,3,4,5,6,11,12,13,14]
        
        for i in column_list:
            for j in percent_list:
                for k in param_list:
                    tdSql.query(f"select apercentile(col{i},{j},'{k}') from test")
                    data = tdSql.getData(0, 0)
                    tdSql.query(f"select percentile(col{i},{j}) from test")
                    expect_data = tdSql.getData(0, 0)
                    self.check_apercentile(data,expect_data,k,j,i) 

        error_param_list = [-1,101,'"a"']
        for i in error_param_list:
            tdSql.error(f'select apercentile(col1,{i}) from test')

        tdSql.execute("create table meters (ts timestamp, voltage int) tags(loc nchar(20))")
        tdSql.execute("create table t0 using meters tags('beijing')")
        tdSql.execute("create table t1 using meters tags('shanghai')")
        for i in range(self.rowNum):
            tdSql.execute("insert into t0 values(%d, %d)" % (self.ts + i, i + 1))            
            tdSql.execute("insert into t1 values(%d, %d)" % (self.ts + i, i + 1))            
        
        column_list = ['voltage']
        for i in column_list:
            for j in percent_list:
                for k in param_list:
                    tdSql.query(f"select apercentile({i}, {j},'{k}') from t0")
                    data = tdSql.getData(0, 0)
                    tdSql.query(f"select percentile({i},{j}) from t0")
                    expect_data = tdSql.getData(0,0)
                    self.check_apercentile(data,expect_data,k,j,i)
                    tdSql.query(f"select apercentile({i}, {j},'{k}') from meters")
                    tdSql.checkRows(1) 
        table_list = ["meters","t0"]
        for i in error_param_list:
            for j in table_list:
                for k in column_list:
                    tdSql.error(f'select apercentile({k},{i}) from {j}')
 
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
