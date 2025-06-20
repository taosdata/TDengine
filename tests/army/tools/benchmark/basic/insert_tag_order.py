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
from time import sleep
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    filePath = "./tools/benchmark/basic/json/insert_tag_order.json"
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def configJsonFile(self, model, interlace=1, auto_create_table="no"):
        tdLog.debug(f"configJsonFile {self.filePath} model={model}, interlace={interlace}, auto_create_table={auto_create_table}")
        with open(self.filePath, 'r') as f:
            data = json.load(f)
        data['databases'][0]['super_tables'][0]['insert_mode'] = model
        data['databases'][0]['super_tables'][0]['interlace_rows'] = interlace
        data['databases'][0]['super_tables'][0]['auto_create_table'] = auto_create_table
        json_data = json.dumps(data)
        with open(self.filePath, "w") as file:
            file.write(json_data)

        tdLog.debug(f"configJsonFile {json_data}")

    def checkDataCorrect(self, count, table_count):
        sql = "select count(*) from functiontest.addcolumns"
        rows = tdSql.query(sql)
        tdSql.checkData(0, 0, count)

        sql = "select distinct groupid from functiontest.addcolumns;"
        rows = tdSql.query(sql)
        tdSql.checkRows(table_count)

    def executeAndCheck(self, mode, interlace=1, auto_create_table="no"):
        self.configJsonFile(mode, interlace, auto_create_table)
        benchmark = frame.etool.benchMarkFile()
        cmd = f"{benchmark} -f {self.filePath}"
        tdLog.info(f"Executing command: {cmd}")
        os.system("%s" % cmd)
        self.checkDataCorrect(10000, 1000)


    def run(self):

        self.executeAndCheck("stmt", 1)
        self.executeAndCheck("stmt", 0)
        self.executeAndCheck("stmt", 0, 'yes')
        
        self.executeAndCheck("stmt2", 1)
        self.executeAndCheck("stmt2", 1, 'yes')
        self.executeAndCheck("stmt2", 0)
        self.executeAndCheck("stmt2", 0, 'yes')

        self.executeAndCheck("sml", 1)
        self.executeAndCheck("sml", 1, 'yes')
        self.executeAndCheck("sml", 0)
        self.executeAndCheck("sml", 0, 'yes')

        self.executeAndCheck("taosc", 1)
        self.executeAndCheck("taosc", 1, 'yes')
        self.executeAndCheck("taosc", 0)
        self.executeAndCheck("taosc", 0, 'yes')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
