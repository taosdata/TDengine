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

from new_test_framework.utils import tdLog, tdSql, tdDnodes
import re

class TestInsFilesets:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        
    def test_ins_filesets(self):
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
        tdSql.execute('create database db vgroups 1')
        tdSql.execute('use db')
        tdSql.execute('create table t1 (ts timestamp, a int, b int)')
        tdSql.execute('insert into t1 values(\'2024-12-27 14:00:00\', 1, 2)')
        tdSql.execute('flush database db')

        tdLog.sleep(5)

        rows = tdSql.query('select * from information_schema.ins_filesets')
        tdSql.checkRows(1)
        tdSql.checkEqual(tdSql.getData(0, 0), 'db')
        tdSql.checkEqual(tdSql.getData(0, 1), 2)
        tdSql.checkEqual(tdSql.getData(0, 2), 2008)
        # tdSql.CheckEqual(str(tdSql.getData(0, 3)), '2024-12-23 08:00:00.000')
        # tdSql.CheckEqual(str(tdSql.getData(0, 4)), '2025-01-02 07:59:59.999')
        # tdSql.CheckEqual(tdSql.getData(0, 6), '1970-01-01 08:00:00.000')
        # tdSql.CheckEqual(tdSql.getData(0, 7), False)

        tdDnodes.stopAll()
        tdLog.success("%s successfully executed" % __file__)

