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
        case1 <wenzhouwww>: [TD-12909] :
            this test case is for illegal SQL in query ,it will crash taosd.
        '''
        return

    def run(self):
        tdSql.prepare()
        tdSql.execute("create database if not exists testdb keep 36500;")
        tdSql.execute("use testdb;")
        tdSql.execute("create stable st (ts timestamp , id int , value double) tags(hostname binary(10) ,ind int);")
        for i in range(self.num):
            tdSql.execute("insert into sub_%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+10000*i,i*2,i+10.00))
        tdSql.query("select distinct(hostname) from testdb.st")
        tdSql.checkRows(10)
        tdSql.query("select count(*) from st where hostname >1")
        tdSql.query("select count(*) from st where hostname >'1'")
        tdSql.query("select count(*) from st where hostname <=1")
        tdSql.query("select count(*) from st where hostname <='1'")
        tdSql.query("select count(*) from st where hostname !=1")
        tdSql.query("select count(*) from st where hostname !='1'")
        tdSql.query("select count(*) from st where hostname <>1")
        tdSql.query("select count(*) from st where hostname <>'1'")
        tdSql.query("select count(*) from st where hostname in ('1','2')")
        tdSql.query("select count(*) from st where hostname match '%'")
        tdSql.query("select count(*) from st where hostname match '%1'")
        tdSql.query("select count(*) from st where hostname between 1 and 2")
        tdSql.query("select count(*) from st where hostname between 'abc' and 'def'")

        tdSql.error("select count(*) from st where hostname between 1 and 2 or sum(1)")
        tdSql.error("select count(*) from st where hostname < max(123)")

        tdSql.error("select count(*) from st where hostname < max('abc')")
        tdSql.error("select count(*) from st where hostname < max(min(123))")

        tdSql.error("select count(*) from st where hostname < sum('abc')")
        tdSql.error("select count(*) from st where hostname < sum(min(123))")

        tdSql.error("select count(*) from st where hostname < diff('abc')")
        tdSql.error("select count(*) from st where hostname < diff(min(123))")

        tdSql.error("select count(*) from st where hostname < tbname")
        tdSql.error("select count(*) from st where ts > 0 and tbname in ('d1', 'd2') and tbname-2")

        tdSql.query("select count(*) from st where id > 10000000000000")

    def stop(self):
        tdSql.close()

tdLog.success("%s successfully executed" % __file__)
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())