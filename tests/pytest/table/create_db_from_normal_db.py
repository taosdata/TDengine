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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("test case for TS-783")
        tdSql.execute("drop table if exists db.state1;")
        tdSql.execute("create table db.state1 (ts timestamp, c1 int);")
        tdSql.error("create table db.test1 using db.state1 tags('tt');")

        tdSql.execute("drop table if exists db.state2;")
        tdSql.execute("create table db.state2 (ts timestamp, c1 int) tags (t binary(20));")
        tdSql.query("create table db.test2 using db.state2 tags('tt');")
        tdSql.error("create table db.test22 using db.test2 tags('tt');")  

         # test case for TS-1289
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table `metrics` (`ts` TIMESTAMP,`value` DOUBLE) TAGS (`labels` JSON)")
        tdSql.execute('''CREATE TABLE `t_eb22c740776471c56ed97eff4951eb41` USING `metrics` TAGS ('{"__name__":"node_exporter:memory:used:percent","datacenter":"cvte
            ","hostname":"p-tdengine-s-002","instance":"10.21.46.53:9100","ipaddress":"10.21.46.53","job":"node","product":"Prometheus","productline":"INFRA
            "}');''')
   
        tdSql.query("show create table t_eb22c740776471c56ed97eff4951eb41")
        sql = tdSql.getData(0, 1)
        tdSql.execute("drop table t_eb22c740776471c56ed97eff4951eb41")
        tdSql.query("show tables")
        tdSql.checkRows(0)
        
        tdSql.execute(sql)
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 't_eb22c740776471c56ed97eff4951eb41')     

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
