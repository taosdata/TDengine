
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

class TestDdlInSysdb:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


    def test_meta_sysdb(self):
        """Meta system database

        1. check meta system database

        Since: v3.3.6

        Labels: common,ci

        Jira: TS-7600

        History:
            - 2025-11-13 Tony Zhang created

        """
        self.check_table_type()

    def check_table_type(self):
        # prepare data
        tdSql.execute("create database if not exists test_meta_sysdb", show=1)
        tdSql.execute("use test_meta_sysdb")
        tdSql.execute("create table stb (ts timestamp, v1 int) tags (t1 int)", show=1)
        tdSql.execute("create table ctb using stb tags (1)", show=1)

        # check table type in information_schema
        tdSql.query('select type from information_schema.ins_tables where table_name="ctb" and create_time < now() + 1d')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'CHILD_TABLE')
