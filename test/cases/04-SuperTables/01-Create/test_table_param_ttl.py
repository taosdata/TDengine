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

from new_test_framework.utils import tdLog, tdSql
import os
import time

class TestTableParamTtl:
    updatecfgDict = {'ttlUnit':5,'ttlPushInterval':3, 'mdebugflag':143}
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        cls.ntbname = 'ntb'
        cls.stbname = 'stb'
        cls.tbnum = 10
        cls.ttl_param = 1
        cls.default_ttl = 100
        cls.modify_ttl = 1

    def wait_query(self, sql: str, expected_row_num: int, timeout_in_seconds: float):
        # Under ASAN the TTL background thread and flush are 3-5x slower; extend
        # all waits proportionally so the test does not produce a false failure.
        if os.environ.get("CI_ASAN_BUILD") == "1":
            timeout_in_seconds = timeout_in_seconds * 4
        timeout = timeout_in_seconds
        tdSql.query(sql)
        while timeout > 0 and tdSql.getRows() != expected_row_num:
            tdLog.debug(f'start to wait query: {sql} to return {expected_row_num}, got: {tdSql.getRows()}, remain: {timeout_in_seconds - timeout}')
            time.sleep(1)
            timeout = timeout - 1
            tdSql.query(sql)
        if timeout <= 0:
            tdLog.exit(f'failed to wait query: {sql} to return {expected_row_num} rows timeout: {timeout_in_seconds}s')
        else:
            tdLog.debug(f'wait query succeed: {sql} to return {expected_row_num}, got: {tdSql.getRows()}')

    def ttl_check_ntb(self):
        tdSql.prepare()

        for i in range(self.tbnum):
            tdSql.execute(f'create table db.{self.ntbname}_{i} (ts timestamp,c0 int) ttl {self.ttl_param}')
        tdSql.query(f'show db.tables')
        tdSql.checkRows(self.tbnum)
        tdSql.execute(f'flush database db')
        timeout = self.updatecfgDict['ttlUnit']*self.ttl_param+self.updatecfgDict['ttlPushInterval']
        self.wait_query('show db.tables', 0, timeout + 5)
        for i in range(self.tbnum):
            tdSql.execute(f'create table db.{self.ntbname}_{i} (ts timestamp,c0 int) ttl {self.default_ttl}')
        for i in range(int(self.tbnum/2)):
            tdSql.execute(f'alter table db.{self.ntbname}_{i} ttl {self.modify_ttl}')
        tdSql.execute(f'flush database db')
        timeout = self.updatecfgDict['ttlUnit']*self.modify_ttl+self.updatecfgDict['ttlPushInterval']
        self.wait_query('show db.tables', self.tbnum - int(self.tbnum / 2), timeout + 10)
        tdSql.execute('drop database db')
    def ttl_check_ctb(self):
        tdSql.prepare()
        tdSql.execute(f'create table db.{self.stbname} (ts timestamp,c0 int) tags(t0 int)')

        for i in range(self.tbnum):
            tdSql.execute(f'create table db.{self.stbname}_{i} using db.{self.stbname} tags({i}) ttl {self.ttl_param}')
        tdSql.query(f'show db.tables')
        tdSql.checkRows(self.tbnum)
        tdSql.execute(f'flush database db')
        timeout = self.updatecfgDict['ttlUnit']*self.ttl_param+self.updatecfgDict['ttlPushInterval'];
        self.wait_query('show db.tables', 0, timeout + 5)
        for i in range(self.tbnum):
            tdSql.execute(f'create table db.{self.stbname}_{i} using db.{self.stbname} tags({i}) ttl {self.default_ttl}')
        tdSql.query(f'show db.tables')
        tdSql.checkRows(self.tbnum)
        for i in range(int(self.tbnum/2)):
            tdSql.execute(f'alter table db.{self.stbname}_{i} ttl {self.modify_ttl}')
        tdSql.execute(f'flush database db')
        timeout = self.updatecfgDict['ttlUnit']*self.modify_ttl+self.updatecfgDict['ttlPushInterval'];
        self.wait_query('show db.tables', self.tbnum - int(self.tbnum / 2), timeout + 5)
        tdSql.execute('drop database db')

    def ttl_check_insert(self):
        tdSql.prepare()
        tdSql.execute(f'create table db.{self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        for i in range(self.tbnum):
            tdSql.execute(f'insert into db.{self.stbname}_{i} using db.{self.stbname} tags({i}) ttl {self.ttl_param} values(now,1)')
        tdSql.query(f'show db.tables')
        tdSql.checkRows(self.tbnum)
        tdSql.execute(f'flush database db')
        timeout = self.updatecfgDict['ttlUnit']*self.ttl_param+self.updatecfgDict['ttlPushInterval'];
        self.wait_query('show db.tables', 0, timeout + 60)
        tdSql.execute('drop database db')
    def test_table_param_ttl(self):
        """Stable ttl options

        1. Create normal table with ttl param
        2. Create stable and child table with ttl param
        3. Insert data into child table with ttl param
        4. Verify ttl param is ok

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_table_param_ttl.py

        """
        self.ttl_check_ntb()
        self.ttl_check_ctb()
        self.ttl_check_insert()

