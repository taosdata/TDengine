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
import frame.eos
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        taosBenchmark Insert->Basic test cases
        """

    def benchmarkQuery(self, benchmark, jsonFile,  keys, options=""):
        # exe insert 
        result = "query.log"
        os.system(f"rm -f {result}")
        cmd = f"{benchmark} {options} -f {jsonFile} >> {result}"
        os.system(cmd)
        tdLog.info(cmd)
        with open(result) as file:
            content = file.read()
            for key in keys:
                if content.find(key) == -1:
                    tdLog.exit(f"not found key: {key} in content={content}")
                else:
                    tdLog.info(f"found key:{key} successful.")            


    def testBenchmarkJson(self, benchmark, jsonFile, options = "", checkTimeStep = False):
        # exe insert 
        cmd = f"{benchmark} {options} -f {jsonFile}"
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
        
        # drop
        try:
            drop = data["databases"][0]["dbinfo"]["drop"]
        except:
            drop = "yes"

        # command is first
        if options.find("-Q") != -1:
            drop = "no"

        # only support 
        cmdVG = None
        pos = options.find("=")
        if pos != -1:
            arr = options.split("=")
            if arr[0] == "--vgroups":
                cmdVG = arr[1]

        # vgropus
        vgroups = None
        try:
            if cmdVG != None:
                # command special vgroups first priority
                vgroups = cmdVG
            else:
                dbinfo = data["databases"][0]["dbinfo"]
                for key,value in dbinfo.items():
                    if key.strip().lower() == "vgroups":
                        vgroups = value
        except:
            vgroups = None

        tdLog.info(f"get json info: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} cmdVG={cmdVG}\n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkTimeStep:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

        if drop.lower() == "yes":
            # check database optins 
            sql = f"select `vgroups` from information_schema.ins_databases where name='{db}';"
            tdSql.query(sql)
            if vgroups != None:
                tdLog.info(f" vgroups real={tdSql.getData(0,0)} expect={vgroups}")
                tdSql.checkData(0, 0, vgroups, True)

    # bugs ts
    def checkVGroups(self, benchmark):
        # vgroups with command line set
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/insertBasic.json", "--vgroups=3", True)
        # vgroups with json file
        self.testBenchmarkJson(benchmark, "./tools/benchmark/basic/json/insertBasic.json", "", True)


    def checkInsertManyStb(self):
        # many stb
        self.benchInsert("./tools/benchmark/basic/json/insertManyStb.json")
    
    def run(self):
        # check env
        cmd = f"pip3 list"
        output, error, code = eos.run(cmd)
        tdLog.info("output: >>>%s<<<" % output)

        benchmark = frame.etool.benchMarkFile()

        # vgroups
        self.checkVGroups(benchmark)

        # check many stable
        self.checkInsertManyStb()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
