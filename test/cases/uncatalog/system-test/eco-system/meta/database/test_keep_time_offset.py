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

from new_test_framework.utils import tdLog, tdSql, TDSetSql
import re

class TestKeepTimeOffset:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()

    def create_db(self):
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


    def check_old_syntax(self):
        # old syntax would not support again
        tdSql.error("alter dnode 1 'keeptimeoffset 10';")


    def test_keep_time_offset(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        # check new syntax right
        self.create_db()
        # check old syntax error
        self.check_old_syntax()
        tdLog.success("%s successfully executed" % __file__)

