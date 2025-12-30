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
        self.wait_query('show db.tables', 0, timeout + 5)
        tdSql.execute('drop database db')
    def test_table_param_ttl(self):
        """Stable ttl options

        1. Create normal table with ttl param
        2. Create stable and child table with ttl param
        3. Insert data into child table with ttl param
        4. Verify ttl param is ok

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_table_param_ttl.py

        """
        self.ttl_check_ntb()
        self.ttl_check_ctb()
        self.ttl_check_insert()
        tdLog.success(f"{__file__} successfully executed")
