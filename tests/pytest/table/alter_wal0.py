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
    """
    remove last tow bytes of file 'wal0',then restart taosd and create new tables.
    """
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create database if not exists demo;");
        tdSql.execute("use demo;")
        tdSql.execute("create table if not exists meters(ts timestamp, f1 int) tags(t1 int);");
        for i in range(1,11):
            tdSql.execute("CREATE table if not exists test{num} using meters tags({num});".format(num=i))
        print("==============insert 10 tables")

        tdSql.query('show tables;')
        tdSql.checkRows(10)

        print("==============step2")
        tdDnodes.stopAll()
        filename = '/var/lib/taos/mnode/wal/wal0'

        with open(filename, 'rb') as f1:
            temp = f1.read()

        with open(filename, 'wb') as f2:
            f2.write(temp[:-2])

        tdDnodes.start(1)
        print("==============remove last tow bytes of file 'wal0' and restart taosd")
        
        print("==============step3")
        tdSql.execute("use demo;")
        tdSql.query('show tables;')
        tdSql.checkRows(10)
        for i in range(11,21):
            tdSql.execute("CREATE table if not exists test{num} using meters tags({num});".format(num=i))

        tdSql.query('show tables;')
        tdSql.checkRows(20)
        print("==============check table numbers and create 10 tables")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
