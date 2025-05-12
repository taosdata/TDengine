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

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_002 using st tags("dev_02")')

        print("==============step2")
        tdSql.error("select * from db.st where ts in ('2020-05-13 10:00:00.000')")

        tdSql.execute(
            """INSERT INTO dev_001(ts, tagtype) VALUES('2020-05-13 10:00:00.000', 1),
            ('2020-05-13 10:00:00.001', 1)
             dev_002 VALUES('2020-05-13 10:00:00.001', 1)""")

        # TAG nchar
        tdSql.query('select count(ts) from db.st where dev in ("dev_01")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query('select count(ts) from db.st where dev in ("dev_01", "dev_01")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query('select count(ts) from db.st where dev in ("dev_01", "dev_01", "dev_02")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        
        # colume int 
        tdSql.query("select count(ts) from db.st where tagtype in (1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query("select count(ts) from db.st where tagtype in (1, 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdSql.execute(
            """INSERT INTO dev_001(ts, tagtype) VALUES('2020-05-13 10:00:01.000', 2),
            ('2020-05-13 10:00:02.001', 2)
             dev_002 VALUES('2020-05-13 10:00:03.001', 2)""")

        tdSql.query("select count(ts) from db.st where tagtype in (1, 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.execute("create table tb(ts timestamp, c1 int, c2 binary(10), c3 nchar(10), c4 float, c5 bool)")
        for i in range(10):
            tdSql.execute("insert into tb values(%d, %d, 'binary%d', 'nchar%d', %f, %d)" % (self.ts + i, i, i, i, i + 0.1, i % 2))

        #binary 
        tdSql.query('select count(ts) from db.tb where c2 in ("binary1")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        
        tdSql.query('select count(ts) from db.tb where c2 in ("binary1", "binary2", "binary1")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        #bool 
        #tdSql.query('select count(ts) from db.tb where c5 in (true)')
        #tdSql.checkRows(1)
        #tdSql.checkData(0, 0, 5)

        #float
        #tdSql.query('select count(ts) from db.tb where c4 in (0.1)')
        #tdSql.checkRows(1)
        #tdSql.checkData(0, 0, 1)

        #nchar 
        tdSql.query('select count(ts) from db.tb where c3 in ("nchar0")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        
        
        #tdSql.query("select tbname, dev from dev_001") 
        #tdSql.checkRows(1)
        #tdSql.checkData(0, 0, 'dev_001')
        #tdSql.checkData(0, 1, 'dev_01')

        #tdSql.query("select tbname, dev, tagtype from dev_001") 
        #tdSql.checkRows(2)

        ### test case for https://jira.taosdata.com:18080/browse/TD-1930
        #tdSql.execute("create table tb(ts timestamp, c1 int, c2 binary(10), c3 nchar(10), c4 float, c5 bool)")
        #for i in range(10):
        #    tdSql.execute("insert into tb values(%d, %d, 'binary%d', 'nchar%d', %f, %d)" % (self.ts + i, i, i, i, i + 0.1, i % 2))
        #
        #tdSql.error("select * from tb where c2 = binary2")
        #tdSql.error("select * from tb where c3 = nchar2")

        #tdSql.query("select * from tb where c2 = 'binary2' ")
        #tdSql.checkRows(1)

        #tdSql.query("select * from tb where c3 = 'nchar2' ")
        #tdSql.checkRows(1)

        #tdSql.query("select * from tb where c1 = '2' ")
        #tdSql.checkRows(1)

        #tdSql.query("select * from tb where c1 = 2 ")
        #tdSql.checkRows(1)

        #tdSql.query("select * from tb where c4 = '0.1' ")
        #tdSql.checkRows(1)

        #tdSql.query("select * from tb where c4 = 0.1 ")
        #tdSql.checkRows(1)

        #tdSql.query("select * from tb where c5 = true ")
        #tdSql.checkRows(5)

        #tdSql.query("select * from tb where c5 = 'true' ")
        #tdSql.checkRows(5)

        ## For jira: https://jira.taosdata.com:18080/browse/TD-2850
        #tdSql.execute("create database 'Test' ")
        #tdSql.execute("use 'Test' ")
        #tdSql.execute("create table 'TB'(ts timestamp, 'Col1' int) tags('Tag1' int)")
        #tdSql.execute("insert into 'Tb0' using tb tags(1) values(now, 1)")
        #tdSql.query("select * from tb")
        #tdSql.checkRows(1)

        # For jira: https://jira.taosdata.com:18080/browse/TD-6038
        tdSql.query('select count(ts) from db.tb where c1 = 0 and c3 in ("nchar0")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select * from db.tb where c1 = 2 and c3 in ("nchar0")')
        tdSql.checkRows(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
