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
import time

import taos
import frame
import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.epath import *
from frame.srvCtl import *
from frame import *


class TDTestCase(TBase):
    def alterTtlConfig(self):
        """Add test case for altering ttl config
        """
        db_name = "db"
        tdSql.execute(f"create database {db_name};")
        tdSql.execute(f"use {db_name};")
        tdSql.execute("create table t1 (ts timestamp, a int);")

        ttl_min_value = 0
        ttl_max_value = 2147483647
        # verify ttl min value
        tdSql.execute(f"alter table t1 ttl {ttl_min_value}")
        tdSql.query("select `ttl` from information_schema.ins_tables where table_name='t1' and db_name='db';")
        tdSql.checkData(0, 0, ttl_min_value)
        # verify ttl max value
        tdSql.execute(f"alter table t1 ttl {ttl_max_value}")
        tdSql.query("select `ttl` from information_schema.ins_tables where table_name='t1' and db_name='db';")
        tdSql.checkData(0, 0, ttl_max_value)
        # verify abnormal ttl value
        tdSql.error(f"alter table t1 ttl {ttl_max_value + 1}", expectErrInfo="Value out of range")
        tdSql.error(f"alter table t1 ttl {ttl_min_value - 1}", expectErrInfo="syntax error")
        
        # TS-5291
        tdSql.execute("create database db5291 vgroups 1;")
        tdSql.execute("use db5291;")
        tdSql.execute("create table ttltb1(ts timestamp, ival int) ttl 1;")
        tdSql.execute("create table ttltb2(ts timestamp, ival int) ttl 1;")
        tdSql.execute("drop table ttltb1;")
        tdSql.execute("flush database db5291;")
        tdSql.execute("drop table ttltb2;")
        # restart taosd
        os.system("ps -ef | grep taosd | grep -v grep | awk '{print $2}' | xargs kill -9")
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        tdSql.execute("flush database db5291;")

    def alterSupportVnodes(self):
        tdLog.info(f"test function of altering supportVnodes")

        tdSql.execute("alter dnode 1 'supportVnodes' '128'")
        time.sleep(1)
        tdSql.query('show dnodes')
        tdSql.checkData(0, 3, "128")

        tdSql.execute("alter dnode 1 'supportVnodes' '64'")
        time.sleep(1)
        tdSql.query('show dnodes')
        tdSql.checkData(0, 3, "64")

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # TS-4721
        self.alterSupportVnodes()
        # TS-5191
        self.alterTtlConfig()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
