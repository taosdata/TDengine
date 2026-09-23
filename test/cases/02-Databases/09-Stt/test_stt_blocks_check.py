# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

from random import randint

from new_test_framework.utils import tdLog, tdSql
import random
from random import randint
import os
import time

class TestSttBlocksCheck:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        
    def stt_block_check(self):
        tdSql.prepare()
        tdSql.execute('use db')
        
        tdSql.execute('create table meters (ts timestamp, c1 int, c2 float) tags(t1 int)')
        tdSql.execute("create table d0 using meters tags(1)")
        
        ts = 1704261670000

        sql = "insert into d0 values "
        for i in range(100):
            sql = sql + f"({ts + i}, 1, 0.1)"
        tdSql.execute(sql)
        tdSql.execute("flush database db")
        
        ts = 1704261670099

        sql = "insert into d0 values "
        for i in range(100):
            sql = sql + f"({ts + i}, 1, 0.1)"
        tdSql.execute(sql)
        tdSql.execute("flush database db")
        
        tdSql.execute(f"insert into d0 values({ts + 100}, 2, 1.0)")
        tdSql.execute("flush database db")
        
        time.sleep(2)
        
        tdSql.query("show table distributed db.meters")
        tdSql.query("select count(*) from db.meters")
        tdSql.checkData(0, 0, 200)

    def test_stt_blocks_check(self):
        """STT Blocks Check

        1. Create database with default STT trigger
        2. Insert data and flush to generate smaller blocks
        3. Query insert data
        4. show table distributed to check blocks
        5. verify result is ok

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_stt_blocks_check.py
        """
        self.stt_block_check()
        
        #tdSql.close()

