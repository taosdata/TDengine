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
        taosBenchmark insert->Precision test cases
        """

    def testBenchmarkJson(self, benchmark, jsonFile, options = ""):
        # exe insert 
        cmd = f"{benchmark} {options} -f {jsonFile}"
        os.system(cmd)
        
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        db  = data["databases"][0]["dbinfo"]["name"]
        precison = data["databases"][0]["dbinfo"]["precision"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]
        start_timestamp = data["databases"][0]["super_tables"][0]["start_timestamp"]
        
        tdLog.info(f"get json info: db={db} precision={precison} stb={stb} child_count={child_count} insert_rows={insert_rows} "
                   f"start_timestamp={start_timestamp} timestamp_step={timestamp_step} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step    
        sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
        tdSql.query(sql)
        tdSql.checkRows(0)

        # check last ts
        lastTime = start_timestamp + timestamp_step * (insert_rows - 1)
        sql = f"select last(ts) from {db}.{stb}"
        tdSql.checkAgg(sql, lastTime)

    # bugs ts
    def checkBasic(self, benchmark):
        # MS
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/insertPrecisionMS.json", "")
        # US
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/insertPrecisionUS.json", "")
        # NS
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/insertPrecisionNS.json", "")

    def run(self):
        benchmark = etool.benchMarkFile()

        # vgroups
        self.checkBasic(benchmark)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
