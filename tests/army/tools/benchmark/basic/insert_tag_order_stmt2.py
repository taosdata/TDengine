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
import os
import threading
from time import sleep
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    templateFilePath = "./tools/benchmark/basic/json/insert_tag_order.json"
    fileDirPath = "./tools/benchmark/basic/json/"
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
        benchmark = frame.etool.benchMarkFile()
        filePath = self.fileDirPath + dbname + ".json"
        cmd = f"{benchmark} -f {filePath}"
        tdLog.info(f"Executing command: {cmd}")
        os.system("%s" % cmd)
        self.checkDataCorrect(dbname, 1000, 100)
        tdSql.execute(f"drop database {dbname};")
    
    def run(self):
        self.executeAndCheck('stmt2_tag_order_1', 'stmt2', 1)
        self.executeAndCheck('stmt2_tag_order_2', 'stmt2', 0)
        tdLog.success("Successfully executed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
