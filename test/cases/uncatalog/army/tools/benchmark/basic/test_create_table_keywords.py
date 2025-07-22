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

from new_test_framework.utils import tdLog, tdSql, etool, eutil
from new_test_framework.utils.autogen import AutoGen
import os

class TestCase:
    updatecfgDict = {
        'slowLogScope' : "others"
    }

    def setup_class(cls):
        tdLog.info(f"start to init {__file__}")

    # run
    def test_create_tbl_keywords(self):
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
        tdSql.prepare()
        tdSql.execute("DROP DATABASE IF EXISTS test;")
        autoGen = AutoGen()
        autoGen.create_db("test", 1, 1)
        tdSql.execute(f"use test")
        tdLog.info(f"start to excute {__file__}")
        tdSql.execute('''CREATE TABLE IF NOT EXISTS test.meters (time TIMESTAMP,`value` double, qulity bigint,flags bigint) TAGS (id nchar(32),station nchar(32),type nchar(8))''')
        
        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        templateFilePath = f"{os.path.dirname(os.path.realpath(__file__))}/json/create_table_keywords.json"
        os.system(f"{binPath} -f {templateFilePath} -y ")
        tdSql.query("SELECT COUNT(*) FROM test.meters;")
        tdSql.checkData(0, 0, 9)

        tdLog.success(f"{__file__} successfully executed")
