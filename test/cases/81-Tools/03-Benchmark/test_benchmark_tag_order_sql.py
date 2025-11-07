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

class TestInsertTagOrderSql:
    templateFilePath = f"{os.path.dirname(os.path.realpath(__file__))}/json/insert_tag_order.json"
    fileDirPath = f"{os.path.dirname(os.path.realpath(__file__))}/json/"
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def configJsonFile(self, dbname, model, interlace=1, auto_create_table="no"):
        tdLog.debug(f"configJsonFile {self.templateFilePath} model={model}, interlace={interlace}, auto_create_table={auto_create_table}")
        with open(self.templateFilePath, 'r') as f:
            data = json.load(f)
        data['databases'][0]['dbinfo']['name'] = dbname
        data['databases'][0]['super_tables'][0]['insert_mode'] = model
        data['databases'][0]['super_tables'][0]['interlace_rows'] = interlace
        data['databases'][0]['super_tables'][1]['insert_mode'] = model
        data['databases'][0]['super_tables'][1]['interlace_rows'] = interlace
        json_data = json.dumps(data)
        filePath = self.fileDirPath + dbname + ".json"
        with open(filePath, "w") as file:
            file.write(json_data)

        tdLog.debug(f"configJsonFile {json_data}")

    def checkDataCorrect(self, dbname, count, table_count):
        sql = f"select count(*) from {dbname}.addcolumns"
        rows = tdSql.query(sql)
        tdSql.checkData(0, 0, count)

        sql = f"select distinct groupid from {dbname}.addcolumns;"
        rows = tdSql.query(sql)
        tdSql.checkRows(table_count)

        sql = f"select count(*) from {dbname}.addcolumns1"
        rows = tdSql.query(sql)
        tdSql.checkData(0, 0, count)

        sql = f"select distinct groupid from {dbname}.addcolumns1;"
        rows = tdSql.query(sql)
        tdSql.checkRows(table_count)

    def executeAndCheck(self, dbname, mode, interlace=1, auto_create_table="no"):
        self.configJsonFile(dbname, mode, interlace, auto_create_table)
        benchmark = etool.benchMarkFile()
        filePath = self.fileDirPath + dbname + ".json"
        cmd = f"{benchmark} -f {filePath}"
        tdLog.info(f"Executing command: {cmd}")
        os.system("%s" % cmd)
        self.checkDataCorrect(dbname, 1000, 100)
        tdSql.execute(f"drop database {dbname};")

    def test_benchmark_tag_order_sql(self):
        """taosBenchmark tag order sql

        1. Sql insert with tag order and interlace 1
        2. Sql insert with tag order and interlace 0 
        3. Check tag value is ordered after insert
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_tag_order_sql.py

        """
        self.executeAndCheck('taoc_tag_order_1', 'taosc', 1)
        self.executeAndCheck('taoc_tag_order_2', 'taosc', 0)

        tdLog.success("Successfully executed")

        tdLog.success("%s successfully executed" % __file__)


