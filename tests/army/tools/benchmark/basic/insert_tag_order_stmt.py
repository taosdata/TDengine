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
        benchmark = frame.etool.benchMarkFile()
        cmd = f"{benchmark} -f {self.templateFilePath}"
        tdLog.info(f"Executing command: {cmd}")
        os.system("%s" % cmd)
        self.checkDataCorrect(dbname, 1000, 100)
        tdSql.execute(f"drop database {dbname};")
    
    def run(self):
        self.executeAndCheck('insert_tag_order')

        tdLog.success("Successfully executed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
