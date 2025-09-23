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
import threading
from time import sleep
import json


class TestInsertTagOrderStmt:
    templateFilePath = f"{os.path.dirname(os.path.realpath(__file__))}/json/insert_tag_order.json"
    fileDirPath = f"{os.path.dirname(os.path.realpath(__file__))}/json/"
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def checkDataCorrect(self, dbname, count, table_count):
        sql = f"select count(*) from {dbname}.addcolumns"
        tdSql.query(sql)
        tdSql.checkData(0, 0, count)

        sql = f"select distinct groupid from {dbname}.addcolumns;"
        tdSql.query(sql)
        tdSql.checkRows(table_count)

        sql = f"select count(*) from {dbname}.addcolumns1"
        tdSql.query(sql)
        tdSql.checkData(0, 0, count)

        sql = f"select distinct groupid from {dbname}.addcolumns1;"
        tdSql.query(sql)
        tdSql.checkRows(table_count)

    def executeAndCheck(self, dbname):
        benchmark = etool.benchMarkFile()
        cmd = f"{benchmark} -f {self.templateFilePath}"
        tdLog.info(f"Executing command: {cmd}")
        os.system("%s" % cmd)
        self.checkDataCorrect(dbname, 1000, 100)
        tdSql.execute(f"drop database {dbname};")
    
    def test_insert_tag_order_stmt(self):
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
        self.executeAndCheck('insert_tag_order')

        tdLog.success("%s successfully executed" % __file__)


