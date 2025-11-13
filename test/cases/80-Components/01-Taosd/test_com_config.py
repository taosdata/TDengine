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

from new_test_framework.utils import tdLog, tdSql, sc, tdDnodes
import platform
import re
import time

class TestAlterConfig:
    updatecfgDict = {
        'forceReadConfig':'1',
        'timezone':'Asia/Shanghai',
        'arbSetAssignedTimeoutSec':'10',
        'rpcQueueMemoryAllowed':'10485760'
    }
    
    #
    # ------------------- test_alter_config_refresh.py ----------------
    #    
    def alterCachemodel(self):
        """Add test case for altering cache model(TS-5390)
        """
        # drop database if exists
        tdSql.execute(f"drop database if exists db3590;")
        # create database with cachemodel 'none'
        tdSql.execute(f"create database db3590 vgroups 1 replica 1 cachemodel 'none';")
        tdSql.execute("use db3590;")
        tdSql.execute("create table ntb1 (ts timestamp, ival1 int);")
        tdSql.execute("insert into  ntb1 values(now, 1);")
        tdSql.execute("flush database db3590;")
        # alter table schema
        tdSql.execute("alter table ntb1 add column ival2 int;")
        tdSql.execute("insert into  ntb1 values(now, 2, NULL);")
        # alter table schema again
        tdSql.execute("alter table ntb1 add column ival3 int;")
        # alter database with cachemodel 'both‘
        tdSql.execute("alter database db3590 cachemodel 'both';")
        # wait for cachemodel avaiable
        time.sleep(5)
        tdSql.query("select last(*) from ntb1;")

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
        tdSql.error(f"alter table t1 ttl {ttl_max_value + 1}", expectErrInfo="Value out of range", fullMatched=False)
        tdSql.error(f"alter table t1 ttl {ttl_min_value - 1}", expectErrInfo="syntax error", fullMatched=False)
        
        # TS-5291
        tdSql.execute("create database db5291 vgroups 1;")
        tdSql.execute("use db5291;")
        tdSql.execute("create table ttltb1(ts timestamp, ival int) ttl 1;")
        tdSql.execute("create table ttltb2(ts timestamp, ival int) ttl 1;")
        tdSql.execute("drop table ttltb1;")
        tdSql.execute("flush database db5291;")
        tdSql.execute("drop table ttltb2;")
        # restart taosd
        sc.dnodeStopAll()
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

    def checkKeyValue(self, res, key, value, ikey = 0, ival = 1):
        result = False
        for row in res:
            if row[ikey] == key:
                if row[ival] != value:
                    raise Exception(f"key:{key} value:{row[ival]} != {value}")
                else:
                    tdLog.info(f"key:{key} value:{row[ival]} == {value}")
                    result = True
                    break
        if not result:
            raise Exception(f"key:{key} not found")

    def checkRows(self, sql, nExpect, nRetry):
        for i in range(nRetry):
            res = tdSql.getResult(sql)
            if len(res) == nExpect:
                break
            time.sleep(1)
        if len(res) != nExpect:
            raise Exception(f"rows:{len(res)} != {nExpect}")

    def alterBypassFlag(self):
        """Add test case for altering bypassFlag(TD-32907)
        """
        tdSql.execute(f"drop database if exists db")
        tdSql.execute(f"create database db")
        tdSql.execute("use db")
        self.checkKeyValue(tdSql.getResult("show local variables;"), "bypassFlag", "0")
        self.checkKeyValue(tdSql.getResult("show dnode 1 variables like 'bypassFlag'"), "bypassFlag", "0", 1, 2)
        tdSql.execute("alter local 'bypassFlag 1'")
        self.checkKeyValue(tdSql.getResult("show local variables;"), "bypassFlag", "1")
        self.checkKeyValue(tdSql.getResult("show dnode 1 variables like 'bypassFlag'"), "bypassFlag", "0", 1, 2)
        tdSql.execute("create table stb0(ts timestamp, c0 int) tags(t0 int)")
        tdSql.execute("create table ctb0 using stb0 tags(0)")
        tdSql.execute("insert into ctb0 values(now, 1)")
        tdSql.query("select * from stb0")
        tdSql.checkRows(0)
        tdSql.execute("alter local 'bypassFlag 0'")
        tdSql.execute("alter all dnodes 'bypassFlag 2'")
        self.checkKeyValue(tdSql.getResult("show local variables"), "bypassFlag", "0")
        self.checkKeyValue(tdSql.getResult("show dnode 1 variables like 'bypassFlag'"), "bypassFlag", "2", 1, 2)
        tdSql.execute("insert into ctb0 values(now, 2)")
        tdSql.query("select * from stb0")
        tdSql.checkRows(0)
        tdSql.execute("alter all dnodes 'bypassFlag 4'")
        self.checkKeyValue(tdSql.getResult("show dnode 1 variables like 'bypassFlag'"), "bypassFlag", "4", 1, 2)
        tdSql.execute("insert into ctb0 values(now, 4)")
        tdSql.execute("insert into ctb1 using stb0 tags(1) values(now, 10)")
        tdSql.query("select * from stb0")
        tdSql.checkRows(0)
        tdSql.query("show db.tables")
        tdSql.checkRows(2)
        tdSql.execute("alter all dnodes 'bypassFlag 8'")
        self.checkKeyValue(tdSql.getResult("show dnode 1 variables like 'bypassFlag'"), "bypassFlag", "8", 1, 2)
        tdSql.execute("insert into ctb0 values(now, 8)")
        tdSql.execute("insert into ctb1 values(now, 18)")
        tdSql.query("select * from stb0")
        tdSql.checkRows(2)
        tdSql.execute("flush database db")
        self.checkRows("select * from stb0", 0, 10)
        tdSql.execute("alter all dnodes 'bypassFlag 0'")
        self.checkKeyValue(tdSql.getResult("show local variables"), "bypassFlag", "0")
        self.checkKeyValue(tdSql.getResult("show dnode 1 variables like 'bypassFlag'"), "bypassFlag", "0", 1, 2)
        tdSql.execute("insert into ctb0 values(now, 80)")
        tdSql.execute("insert into ctb1 values(now, 180)")
        tdSql.query("select * from stb0")
        tdSql.checkRows(2)
        tdSql.execute("flush database db")
        for i in range(5):
            self.checkRows("select * from stb0", 2, 1)
            time.sleep(1)

    def alter_err_case(self):
        tdSql.error(f"alter local 'audit 0'",expectErrInfo="Config not found")
        tdSql.error(f"alter dnode 1 'audit 1'",expectErrInfo="Invalid config option")
    
    def alter_dnode_1_case(self):
        tdSql.execute("alter dnode 1 'numOfRpcThreads' '5'")
        tdSql.execute("alter dnode 1 'rpcQueueMemoryAllowed' '15242880'")
        tdSql.execute("alter dnode 1 'syncLogBufferMemoryAllowed' '115728640'")

    def alter_timezone_case(self):
        tdSql.execute("alter local 'timezone UTC'")
        tdSql.execute("alter all dnodes 'timezone UTC'")
        # forbidden to alter one dnode timezone
        tdSql.error("alter dnode 1 'timezone UTC'")
        
        tdSql.query("show local variables like 'timezone'")
        tdSql.checkData(0, 1, "UTC (UTC, +0000)")
        tdSql.query("show dnode 1 variables like 'timezone'")
        tdSql.checkData(0, 2, "UTC (UTC, +0000)")
        tdSql.query("show variables like 'timezone'")
        tdSql.checkData(0, 1, "UTC (UTC, +0000)")

    def alter_memPoolReservedSizeMB_case(self):
        tdSql.execute("alter all dnodes 'minReservedMemorySize 2048'")
        tdSql.query("show dnode 1 variables like 'minReservedMemorySize'")
        tdSql.checkData(0, 2, "2048")

        tdSql.error("alter all dnodes 'minReservedMemorySize 0'")
        tdSql.error("alter all dnodes 'minReservedMemorySize 100000000000'")

    # run
    def do_alter_config(self):
        # TS-4721
        self.alterSupportVnodes()
        # TS-5191
        self.alterTtlConfig()
        # TS-5390
        self.alterCachemodel()
        # TD-32907
        self.alterBypassFlag()
        # TS-5007
        self.alter_err_case()
        self.alter_dnode_1_case()

        #TS-6363
        self.alter_memPoolReservedSizeMB_case()

        self.alter_timezone_case()
        print("do alter config ....................... [passed]")

    #
    # ------------------- test_dismatch_config.py ----------------
    #
    def update_cfg_success(self):
        tdLog.info("start to update cfg")
        tdDnodes.stop(1)
        time.sleep(3)
        tdDnodes.cfg(1, 'timezone', 'UTC')
        tdDnodes.cfg(1, 'arbSetAssignedTimeoutSec', '17')
        tdDnodes.cfg(1, 'rpcQueueMemoryAllowed', '20971520')
        tdDnodes.start(1)
        time.sleep(10)
        # global cfg use values from cluster
        tdSql.query("show dnode 1 variables like 'timezone'")

        # It is hard to get the abbr. of the timezone on Windows, so TDengine behaves differently
        # on Windows, it is better to not include the abbr. of the timezone and always return
        # something like 'Asia/Shanghai (UTC+0800)' in the future, this could make things easier.
        if platform.system().lower() == 'windows':
            tdSql.checkData(0, 2, "Asia/Shanghai (UTC, +0800)")
        else:
            tdSql.checkData(0, 2, "Asia/Shanghai (CST, +0800)")

        tdSql.query("show dnode 1 variables like 'arbSetAssignedTimeoutSec'")
        tdSql.checkData(0, 2, "10")

        # dnode local cfg use values from cfg file while forceReadConfig is 1
        tdSql.query("show dnode 1 variables like 'rpcQueueMemoryAllowed'")
        tdSql.checkData(0, 2, "20971520")

    def do_dismatch_config(self):
        self.update_cfg_success()
        print("do dismatch config ....................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_alter_config(self):
        """Server config basic
        
        1. Verify alter support vnodes config
        2. Verify alter ttl config
        3. Verify alter bypass flag config
        4. Verify alter audit config
        5. Verify alter config on dnode 1
        6. Verify alter timezone config
        7. Verify alter memPoolReservedSizeMB config
        8. Verify dismatch config refresh

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/alter/test_alter_config_refresh.py
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_dismatch_config.py

        """
        self.do_dismatch_config()
        self.do_alter_config()