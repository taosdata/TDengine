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


class TestKeepTimeOffset:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def test_create_db(self):
        """create db using keep_time_offset

        1. create db using keep_time_offset
        2. query keep_time_offset
        3. alter keep_time_offset
        4. query keep_time_offset

        Since: v3.0.0.0

        History:
            - 2023-9-27 Alex Duan Created
            - 2025-5-13 Huo Hong Migrated to new test framework

        """
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


    def test_check_old_syntax(self):
        """alter keeptimeoffset using old syntax

        1. alter dnode 1 'keeptimeoffset 10'; return error

        Since: v3.0.0.0

        History:
            - 2023-9-27 Alex Duan Created
            - 2025-5-13 Huo Hong Migrated to new test framework

        """
        # old syntax would not support again
        tdSql.error("alter dnode 1 'keeptimeoffset 10';")
        tdLog.info("%s successfully executed" % __file__)

