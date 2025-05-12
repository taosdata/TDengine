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
import json
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """



    def testBenchmarkJson(self, benchmark, jsonFile):
        # exe insert 
        cmd = f"{benchmark} -f {jsonFile}"
        os.system(cmd)
        
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        db  = data["databases"][0]["dbinfo"]["name"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(f"get json info: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
        tdSql.query(sql)
        tdSql.checkRows(0)
        

    def run(self):
        benchmark = etool.benchMarkFile()
        ''' stmt2 engine have some problem
        # batch - auto-create-table(yes or no)
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/stmt2_insert_batch_autoctb_yes.json")
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/stmt2_insert_batch_autoctb_no.json")
        # interlace - auto-create-table(yes or no)
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/stmt2_insert_interlace_autoctb_yes.json")
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/stmt2_insert_interlace_autoctb_no.json")
        # csv - (batch or interlace)
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/stmt2_insert_csv_interlace_autoctb_yes.json")
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/stmt2_insert_csv_batch_autoctb_no.json")
        '''


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
