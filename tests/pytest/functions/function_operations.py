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

        self.rowNum = 10
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()

        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))                       

        # min verifacation 
        tdSql.error("select ts + col1 from test")
        tdSql.error("select ts + col1 from test1")
        tdSql.error("select col1 + col7 from test")
        tdSql.error("select col1 + col7 from test1")
        tdSql.error("select col1 + col8 from test")
        tdSql.error("select col1 + col8 from test1")
        tdSql.error("select col1 + col9 from test")
        tdSql.error("select col1 + col9 from test1")

        tdSql.query("select col1 + col2 from test1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 2.0)        

        tdSql.query("select col1 + col2 * col3 + col3 / col4 + col5 + col6 + col11 + col12 + col13 + col14 from test1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 7.2)

        tdSql.execute("insert into test1(ts, col1) values(%d, 11)" % (self.ts + 11))
        tdSql.query("select col1 + col2 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(10, 0, None)
        
        tdSql.query("select col1 + col2 * col3 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(10, 0, None)

        tdSql.query("select col1 + col2 * col3 + col3 / col4 + col5 + col6 + col11 + col12 + col13 + col14 from test1")
        tdSql.checkRows(11)
        tdSql.checkData(10, 0, None)

        # test for  tarithoperator.c coverage
        tdSql.execute("insert into test1 values(1537146000010,1,NULL,9,8,1.2,1.3,0,1,1,5,4,3,2)")
        tdSql.execute("insert into test1 values(1537146000011,2,1,NULL,9,1.2,1.3,1,2,2,6,5,4,3)")
        tdSql.execute("insert into test1 values(1537146000012,3,2,1,NULL,1.2,1.3,0,3,3,7,6,5,4)")
        tdSql.execute("insert into test1 values(1537146000013,4,3,2,1,1.2,1.3,1,4,4,8,7,6,5)")
        tdSql.execute("insert into test1 values(1537146000014,5,4,3,2,1.2,1.3,0,5,5,9,8,7,6)")
        tdSql.execute("insert into test1 values(1537146000015,6,5,4,3,1.2,1.3,1,6,6,NULL,9,8,7)")
        tdSql.execute("insert into test1 values(1537146000016,7,6,5,4,1.2,1.3,0,7,7,1,NULL,9,8)")
        tdSql.execute("insert into test1 values(1537146000017,8,7,6,5,1.2,1.3,1,8,8,2,1,NULL,9)")
        tdSql.execute("insert into test1 values(1537146000018,9,8,7,6,1.2,1.3,0,9,9,3,2,1,NULL)")
        tdSql.execute("insert into test1 values(1537146000019,NULL,9,8,7,1.2,1.3,1,10,10,4,3,2,1)")

        self.ts = self.ts + self.rowNum + 10

        tdSql.execute("insert into test1 values(%d, 1, 1, 1, 1, 1.1, 1.1, 1, NULL, '涛思数据3', 1, 1, 1, 1)"  % (  self.ts + self.rowNum + 1 ))
        tdSql.execute("insert into test1 values(%d, 1, 1, 1, 1, 1.1, 1.1, 1, 'taosdata', NULL, 1, 1, 1, 1)"  % (  self.ts + self.rowNum + 2 ))
        tdSql.execute("insert into test1 values(%d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"  % (  self.ts + self.rowNum + 3 ))
        tdSql.execute("insert into test1 values(%d, 1, 1, 1, 1, NULL, 1.1, 1, NULL, '涛思数据3', 1, 1, 1, 1)"  % (  self.ts + self.rowNum + 4 ))
        tdSql.execute("insert into test1 values(%d, 1, 1, 1, 1, 1.1, NULL, 1, 'taosdata', NULL, 1, 1, 1, 1)"  % (  self.ts + self.rowNum + 5 ))
        self.rowNum = self.rowNum + 5

        col_list = [ 'col1' , 'col2' , 'col3' , 'col4' , 'col5' , 'col6' , 'col7' , 'col8' , 'col9' , 'col11' , 'col12' , 'col13' , 'col14' , '1' , '1.1' , 'NULL' ]
        op_list = [ '+' , '-' , '*' , '/' , '%' ]
        err_list = [ 'col7' , 'col8' , 'col9' , 'NULL' ]
        for i in col_list :
            for j in col_list :
                for k in op_list :
                        sql = " select %s %s %s from test1" % ( i , k , j )
                        if i in err_list or j in err_list:
                            tdSql.error(sql)
                        else:
                            tdSql.query(sql)
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
