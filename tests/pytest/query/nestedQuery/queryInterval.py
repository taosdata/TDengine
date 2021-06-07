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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
import random

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts1 = 1593548685000    
        self.ts2 = 1593548785000    


    def run(self):
        # tdSql.execute("drop database db ")
        tdSql.prepare()
        tdSql.execute("create table st (ts timestamp, num int,  value int , t_instance int) tags (loc nchar(30))")
        number = 20
        for n in range(number):
            dt= n*300000   # collecting'frequency is 10s 
            args1=(self.ts1+dt,n,100+n,10+n)
            args2=(self.ts2+dt,n,120+n,15+n)
            tdSql.execute("insert into t0 using st tags('beijing')  values(%d, %d, %d, %d)" % args1)
            tdSql.execute("insert into t1 using st tags('shanghai') values(%d, %d, %d, %d)" % args2)             
                

        tdSql.query("select avg(value) from st interval(10m)")
        print(tdSql.queryResult)
        tdSql.checkRows(11)        
        tdSql.checkData(0, 0, "2020-07-01 04:20:00")
        tdSql.query("select avg_val from(select avg(value) as avg_val from st where loc='beijing' interval(10m));")
        # tdSql.query("select avg(avg_val) from(select avg(value) as avg_val from st where loc='beijing' interval(10m));")
        print(tdSql.queryResult)
        tdSql.checkData(0, 0, 109.5)        
        
        
        # tdSql.query("select avg(voltage) from st interval(1n, 15d)")

        # tdSql.query("select avg(voltage) from st interval(1n, 15d) group by loc")

        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        # tdSql.query("select last(*) from t interval(1s)")
        

        # tdSql.query("select first(ts),twa(c) from tb interval(14a)")
        # tdSql.checkRows(6)

        # tdSql.query("select twa(c) from tb group by c")
        # tdSql.checkRows(4)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
