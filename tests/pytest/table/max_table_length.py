###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
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

        print("==============step1")

        tdLog.info("check nchar")
        tdSql.error("create database anal (ts timestamp ,i nchar(4094))")
        tdSql.execute(
            "create table anal (ts timestamp ,i nchar(4093))")

        print("==============step2")
        tdLog.info("check binary")
        tdSql.error("create database anal (ts timestamp ,i binary(16375))")
        tdSql.execute(
            "create table anal1 (ts timestamp ,i binary(16374))")
        
        print("==============step3")
        tdLog.info("check int & binary")
        tdSql.error("create table anal2 (ts timestamp ,i binary(16371),j int)")
        tdSql.execute("create table anal2 (ts timestamp ,i binary(16370),j int)")
        tdSql.execute("create table anal3 (ts timestamp ,i binary(16366), j int, k int)")
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
