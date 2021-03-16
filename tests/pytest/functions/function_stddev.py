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
        self.stb_prefix = 's'
        self.subtb_prefix = 't'
        
    def run(self):
        tdSql.prepare()

        intData = []        
        floatData = []

        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            intData.append(i + 1)            
            floatData.append(i + 0.1)                        

        # stddev verifacation 
        tdSql.error("select stddev(ts) from test1")

        # stddev support super table now
        # tdSql.error("select stddev(col1) from test")
        # tdSql.error("select stddev(col2) from test")
        # tdSql.error("select stddev(col3) from test")
        # tdSql.error("select stddev(col4) from test")
        # tdSql.error("select stddev(col5) from test")
        # tdSql.error("select stddev(col6) from test")
        tdSql.error("select stddev(col7) from test1")
        tdSql.error("select stddev(col8) from test1")
        tdSql.error("select stddev(col9) from test1")

        tdSql.query("select stddev(col1) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col2) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col3) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col4) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col11) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col12) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col13) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col14) from test1")
        tdSql.checkData(0, 0, np.std(intData))

        tdSql.query("select stddev(col5) from test1")
        tdSql.checkData(0, 0, np.std(floatData))

        tdSql.query("select stddev(col6) from test1")
        tdSql.checkData(0, 0, np.std(floatData))

        #add for td-3276
        sql="create table s (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool,c8 binary(20),c9 nchar(20),c11 int unsigned,c12 smallint unsigned,c13 tinyint unsigned,c14 bigint unsigned) \
            tags(t1 int, t2 float, t3 bigint, t4 smallint, t5 tinyint, t6 double, t7 bool,t8 binary(20),t9 nchar(20), t10 int unsigned , t11 smallint unsigned , t12 tinyint unsigned , t13 bigint unsigned)"
        tdSql.execute(sql)
        for j in range(2):
            if j % 2 == 0:
                sql = "create table %s using %s tags(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)" % \
                        (self.subtb_prefix+str(j)+'_'+str(j),self.stb_prefix)
            else:
                sql = "create table %s using %s tags(%d,%d,%d,%d,%d,%d,%d,'%s','%s',%d,%d,%d,%d)" % \
                        (self.subtb_prefix+str(j)+'_'+str(j),self.stb_prefix,j,j/2.0,j%41,j%51,j%53,j*1.0,j%2,'taos'+str(j),'涛思'+str(j), j%43, j%23 , j%17 , j%3167)
            tdSql.execute(sql)
            for i in range(10):
                if i % 5 == 0 :
                    ret = tdSql.execute(
                    "insert into %s values (%d , NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)" %
                    (self.subtb_prefix+str(j)+'_'+str(j), self.ts+i))
                else:
                    ret = tdSql.execute(
                        "insert into %s values (%d , %d,%d,%d,%d,%d,%d,%d,'%s','%s',%d,%d,%d,%d)" %
                        (self.subtb_prefix+str(j)+'_'+str(j), self.ts+i, i%100, i/2.0, i%41, i%51, i%53, i*1.0, i%2,'taos'+str(i),'涛思'+str(i), i%43, i%23 , i%17 , i%3167))
        
        for i in range(13):
            tdSql.query('select stddev(c4) from s group by t%s' % str(i+1) )

        #add for td-3223
        for i in range(13):
            if i == 1 or i == 5 or i == 6 or i == 7 or i == 9 or i == 8 :continue
            tdSql.query('select stddev(c%d),stddev(c%d) from s group by c%d' %( i+1 , i+1 , i+1  ) )
        

        
            
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
