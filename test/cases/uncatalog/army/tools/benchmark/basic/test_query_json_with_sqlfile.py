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
from new_test_framework.utils import tdLog, tdSql, etool
import os


class TestQueryJsonWithSqlfile:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def test_query_json_with_sqlfile(self):
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
        binPath = etool.benchMarkFile()
        os.system(
            "rm -f rest_query_specified-0 rest_query_super-0 taosc_query_specified-0 taosc_query_super-0"
        )
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")
        tdSql.execute("create table stb (ts timestamp, c0 int)  tags (t0 int)")
        tdSql.execute("insert into stb_0 using stb tags (0) values (now, 0)")
        tdSql.execute("insert into stb_1 using stb tags (1) values (now, 1)")
        tdSql.execute("insert into stb_2 using stb tags (2) values (now, 2)")
        cmd = "%s -f %s/json/taosc_query-sqlfile.json" % (binPath, os.path.dirname(__file__))
        rlist = self.benchmark(f"-f {cmd}")
        # check result
        self.checkListString(rlist, "completed total queries: 2")

        tdLog.success("%s successfully executed" % __file__)


