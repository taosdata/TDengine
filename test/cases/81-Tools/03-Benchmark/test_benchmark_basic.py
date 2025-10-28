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
from new_test_framework.utils import tdLog, tdSql, etool, eos
import os
import json
import platform

class TestBenchmarkBasic:
    #
    # ------------------- test_insert_basic.py ----------------
    #
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
        if platform.system().lower() == "windows":
            # Windows只支持短参数
            if "-v" in options:
                arr = options.split()
                v_idx = arr.index("-v")
                if v_idx + 1 < len(arr):
                    cmdVG = arr[v_idx + 1]
        else:
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
        if platform.system().lower() == "windows":
            self.testBenchmarkJson(benchmark, f"{os.path.dirname(__file__)}\json\insertBasic.json", "-v 3", True)
            self.testBenchmarkJson(benchmark, f"{os.path.dirname(__file__)}\json\insertBasic.json", "", True)
        else:
            self.testBenchmarkJson(benchmark, f"{os.path.dirname(__file__)}/json/insertBasic.json", "--vgroups=3", True)
            # vgroups with json file
            self.testBenchmarkJson(benchmark, f"{os.path.dirname(__file__)}/json/insertBasic.json", "", True)


    def checkInsertManyStb(self):
        # many stb
        self.benchInsert(f"{os.path.dirname(__file__)}/json/insertManyStb.json")


    def checkCompress(self):
        sql = "describe test.meters"
        results = [
            ["ts"    ,"TIMESTAMP"        ,  8 ,"",    "delta-i"   ,"lz4"       ,"medium"],
            ["bc"    ,"BOOL"             ,  1 ,"",    "disabled"  ,"disabled"  ,"medium"],
            ["fc"    ,"FLOAT"            ,  4 ,"",    "delta-d"   ,"zlib"      ,"medium"],
            ["dc"    ,"DOUBLE"           ,  8 ,"",    "delta-d"   ,"xz"        ,"low" ],
            ["ti"    ,"TINYINT"          ,  1 ,"",    "simple8b"  ,"zstd"      ,"high" ],
            ["si"    ,"SMALLINT"         ,  2 ,"",    "simple8b"  ,"zlib"      ,"medium"],
            ["ic"    ,"INT"              ,  4 ,"",    "simple8b"  ,"zstd"      ,"medium"],
            ["bi"    ,"BIGINT"           ,  8 ,"",    "delta-i"   ,"lz4"       ,"medium"],
            ["uti"   ,"TINYINT UNSIGNED" ,  1 ,"",    "simple8b"  ,"zlib"      ,"high" ],
            ["usi"   ,"SMALLINT UNSIGNED",  2 ,"",    "simple8b"  ,"zlib"      ,"medium"],
            ["ui"    ,"INT UNSIGNED"     ,  4 ,"",    "simple8b"  ,"lz4"       ,"low" ],
            ["ubi"   ,"BIGINT UNSIGNED"  ,  8 ,"",    "simple8b"  ,"xz"        ,"medium"],
            ["bin"   ,"VARCHAR"          ,  4 ,"",    "disabled"  ,"zstd"      ,"medium"],
            ["nch"   ,"NCHAR"            ,  8 ,"",    "disabled"  ,"xz"        ,"medium"],
            ["dec64" ,"DECIMAL(10, 6)"   ,  8 ,"",    "disabled"  ,"zstd"      ,"medium"],
            ["dec128","DECIMAL(20, 8)"   , 16 ,"",    "disabled"  ,"zstd"      ,"medium"],
            ["tbc"   ,"BOOL"             ,  1 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tfc"   ,"FLOAT"            ,  4 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tdc"   ,"DOUBLE"           ,  8 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tti"   ,"TINYINT"          ,  1 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tsi"   ,"SMALLINT"         ,  2 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tic"   ,"INT"              ,  4 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tbi"   ,"BIGINT"           ,  8 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tuti"  ,"TINYINT UNSIGNED" ,  1 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tusi"  ,"SMALLINT UNSIGNED",  2 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tui"   ,"INT UNSIGNED"     ,  4 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tubi"  ,"BIGINT UNSIGNED"  ,  8 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tbin"  ,"VARCHAR"          ,  4 ,"TAG", "disabled"  ,"disabled"  ,"disabled"],
            ["tnch"  ,"NCHAR"            ,  8 ,"TAG", "disabled"  ,"disabled"  ,"disabled"]
        ]        
        tdSql.checkDataMem(sql, results)

    def cbRetry(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]

    def doRetry(self, json):
        rlist = self.benchmark(f"-f {json}")
        results = [
            "retry"
        ]
        self.checkManyString(results)

    def checkOther(self):
        
        # tempalte
        template = f"{os.path.dirname(__file__)}/json/insertBasicTemplate.json"

        # retry
        jsonRetry = self.genNewJson(template, self.cbRetry)



    def do_insert_basic(self):
        # check env
        cmd = f"pip3 list"
        output, error = eos.run(cmd)
        tdLog.info("output: >>>%s<<<" % output)

        benchmark = etool.benchMarkFile()

        # vgroups
        self.checkVGroups(benchmark)

        # check many stable
        self.checkInsertManyStb()

        # check compress
        self.checkCompress()

        # other
        #self.checkOther()

        print("do insert basic ..................... [passed]")

    #
    # ------------------- test_custom_col_tag.py ----------------
    #
    def do_custom_col_tag(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/custom_col_tag.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "first_col")
        tdSql.checkData(2, 0, "second_col")
        tdSql.checkData(3, 0, "second_col_1")
        tdSql.checkData(4, 0, "second_col_2")
        tdSql.checkData(5, 0, "second_col_3")
        tdSql.checkData(6, 0, "second_col_4")
        tdSql.checkData(7, 0, "third_col")
        tdSql.checkData(8, 0, "forth_col")
        tdSql.checkData(9, 0, "forth_col_1")
        tdSql.checkData(10, 0, "forth_col_2")
        tdSql.checkData(11, 0, "single")
        tdSql.checkData(12, 0, "multiple")
        tdSql.checkData(13, 0, "multiple_1")
        tdSql.checkData(14, 0, "multiple_2")
        tdSql.checkData(15, 0, "multiple_3")
        tdSql.checkData(16, 0, "multiple_4")
        tdSql.checkData(17, 0, "thensingle")
        tdSql.checkData(18, 0, "thenmultiple")
        tdSql.checkData(19, 0, "thenmultiple_1")
        tdSql.checkData(20, 0, "thenmultiple_2")

        print("do custom col tag ..................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_basic(self):
        """taosBenchmark basic

        1. 
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_basic.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_custom_col_tag.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/

        """
        self.do_insert_basic()
        self.do_custom_col_tag()