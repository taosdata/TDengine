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
from new_test_framework.utils import tdLog, tdSql
import time


class TestKeepBasic:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    #
    #  ---------------------  test keep time offset ---------------------
    #

    def check_create_db(self):
        hours = 8
        # create
        keep_str = f"KEEP_TIME_OFFSET {hours}"
        tdSql.execute(f"create database db {keep_str}")

        # check result
        tdSql.query("select `keep_time_offset` from  information_schema.ins_databases where name='db'")
        tdSql.checkData(0, 0, hours)

        # alter
        hours = 4
        keep_str = f"KEEP_TIME_OFFSET {hours}"
        tdSql.execute(f"alter database db {keep_str}")

        # check result
        tdSql.query("select `keep_time_offset` from  information_schema.ins_databases where name='db'")
        tdSql.checkData(0, 0, hours)
        tdLog.info("%s successfully executed" % __file__)

    def check_old_syntax(self):
        # old syntax would not support again
        tdSql.error("alter dnode 1 'keeptimeoffset 10';")
        tdLog.info("%s successfully executed" % __file__)


    #
    #  ---------------------  test keep expired ---------------------
    #

    def check_keep_expired(self):
        # init
        self.dbname = "test"
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.keep_value = "2d,2d,2d"
        self.duration_value = "16h"
        self.offset_time = 5
        self.sleep_time = self.offset_time*2

        # check keep expired
        tdSql.execute(f'create database if not exists {self.dbname} duration {self.duration_value} keep {self.keep_value};')
        tdSql.execute(f'create table {self.dbname}.{self.stbname} (ts timestamp, c11 int) TAGS(t11 int, t12 int );')
        tdSql.execute(f'create table {self.dbname}.{self.ctbname} using {self.dbname}.{self.stbname} TAGS (1, 1);')
        expired_row_ts = f'now-{int(self.keep_value.split(",")[0].replace("d", "")) * 86400 - self.offset_time}s'
        tdSql.execute(f'insert into {self.dbname}.{self.ctbname} values ({expired_row_ts}, 1);')
        tdSql.query(f'select * from {self.dbname}.{self.ctbname}')
        tdSql.checkEqual(tdSql.queryRows, 1)
        time.sleep(self.offset_time * 2)
        tdSql.query(f'select * from {self.dbname}.{self.ctbname}')
        tdSql.checkEqual(tdSql.queryRows, 0)
        tdSql.execute(f'TRIM DATABASE {self.dbname}')    


    #
    # main
    #
    def test_keep_basic(self):
        """Database Keep Option Basic

        1. Test create/alter database with keep time offset
        2. Test keep expired
    
        Since: v3.0.0.0

        Labels: common,ci

        History:
            - 2023-9-27 Alex Duan Created
            - 2025-5-13 Huo Hong Migrated to new test framework
            - 2025-9-22 Alex Duan Rename test_keep_time_offset.py to test_keep_basic.py
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_keep_expired.py

        """

        # keep time offset
        self.check_old_syntax()
        self.check_create_db()

        # keep expired
        self.check_keep_expired()