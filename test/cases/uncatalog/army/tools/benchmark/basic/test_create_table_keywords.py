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

    def init(self, conn, logSql, replicaVar=1):
        tdLog.info(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    # run
    def run(self):
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
        os.system("%s -f ./tools/benchmark/basic/json/create_table_keywords.json -y " % binPath)
        tdSql.query("SELECT COUNT(*) FROM test.meters;")
        tdSql.checkData(0, 0, 9)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
