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
    # ------------------- test_default_json.py ----------------
    #
    def do_default_json(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/default.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(10)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 100)

        print("do default json ....................... [passed]")

    #
    # ------------------- test_demo.py ----------------
    #
    def do_demo(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -n 100 -t 100 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("describe meters")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "current")
        tdSql.checkData(1, 1, "FLOAT")
        tdSql.checkData(2, 0, "voltage")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "phase")
        tdSql.checkData(3, 1, "FLOAT")
        tdSql.checkData(4, 0, "groupid")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(4, 3, "TAG")
        tdSql.checkData(5, 0, "location")
        # binary for 2.x and varchar for 3.x
        # tdSql.checkData(5, 1, "BINARY")
        tdSql.checkData(5, 2, 24)
        tdSql.checkData(5, 3, "TAG")

        tdSql.query("select count(*) from test.meters where groupid >= 0")
        tdSql.checkData(0, 0, 10000)

        tdSql.query(
            "select count(*) from test.meters where location = 'California.SanFrancisco' or location = 'California.LosAngles' or location = 'California.SanDiego' or location = 'California.SanJose' or \
            location = 'California.PaloAlto' or location = 'California.Campbell' or location = 'California.MountainView' or location = 'California.Sunnyvale' or location = 'California.SantaClara' or location = 'California.Cupertino' "
        )
        tdSql.checkData(0, 0, 10000)

        print("do demo ............................... [passed]")

    #
    # ------------------- test_from_to_continue.py ----------------
    #
    def do_from_to_continue(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -t 6 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        cmd = "%s -f %s/json/insert-from-to-continue-no.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 6 + 3)

        cmd = "%s -t 5 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.execute("create table d5 using meters tags (4, 'd5')")

        cmd = "%s -f %s/json/insert-from-to-continue-yes.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 5 + 3)

        cmd = "%s -t 4 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.execute("create table d4 using meters tags (4, 'd4')")

        cmd = "%s -f %s/json/insert-from-to-continue-smart.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 4 + 1)

        print("do from to continue ................... [passed]")

    #
    # ------------------- test_from_to.py ----------------
    #
    def do_from_to(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()

        cmd = "%s -f %s/json/from-to.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 50)

        if major_ver == "3":
            for i in range(0, 5):
                tdSql.error("select count(*) from db.d%d" % i)
        else:
            for i in range(0, 5):
                tdSql.error("select count(*) from db.d%d" % i)
        for i in range(5, 10):
            tdSql.query("select count(*) from db.d%d" % i)
            tdSql.checkData(0, 0, 10)

        print("do from to ............................ [passed]")

    #
    # ------------------- test_insert_auto_create_table_json.py ----------------
    #
    def do_insert_auto_create_table_json(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        # test case for https://jira.taosdata.com:18080/browse/TD-4985
        os.system("%s -f %s/json/auto_create_table.json -y " % (binPath, os.path.dirname(__file__)))

        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 12)

        print("do insert auto create table json ...... [passed]")

    #
    # ------------------- test_insert_bind_vgroup.py ----------------
    #
    def do_insert_bind_vgroup(self):
        # thread equal vgroups
        self.insertBenchJson(f"{os.path.dirname(__file__)}/json/insertBindVGroup.json", "-g", True)
        # thread is limited
        self.insertBenchJson(f"{os.path.dirname(__file__)}/json/insertBindVGroup.json", "-T 2", True)

        print("do insert bind vgroup ................. [passed]")

    #
    # ------------------- test_insert_data.py ----------------
    #
    def do_insert_data(self):
        # insert
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/dmeters.json" % (binPath, os.path.dirname(__file__))
        errcode = os.system("%s" % cmd)
        if errcode != 0:
            tdLog.exit(f"execute taosBenchmark ret error code={errcode}")
           
        # check data
        sql = "select count(*) from dmeters.meters where current > 5 and current < 65 and voltage > 119 and voltage < 2181 and phase > 29 and phase < 571;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 3)

        print("do insert data ........................ [passed]")

    #
    # ------------------- test_json_tag.py ----------------
    #
    def do_json_tag(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_json_tag.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkData(2, 0, "jtag")
        tdSql.checkData(2, 1, "JSON")
        tdSql.checkData(2, 3, "TAG")

        print("do json tag ........................ [passed]")

    #
    # ------------------- test_limit_offset_json.py ----------------
    #
    def do_limit_offset_json(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_only_create_table.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)

        tdSql.query("describe db.stb")
        tdSql.checkData(9, 1, "NCHAR")
        tdSql.checkData(23, 1, "NCHAR")
        tdSql.checkData(9, 2, 64)
        tdSql.checkData(14, 2, 64)
        tdSql.checkData(23, 2, 64)
        tdSql.checkData(28, 2, 64)

        cmd = "%s -f %s/json/taosc_limit_offset.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 40)
        tdSql.query("select distinct(c3) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c4) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c5) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c13) from db.stb")
        tdSql.checkData(0, 0, None)

        print("do limit offset json .................. [passed]")

    #
    # ------------------- test_insert_precision.py ----------------
    #
    def run_benchmark_json(self, benchmark, jsonFile, options = ""):
        # exe insert 
        cmd = f"{benchmark} {options} -f {jsonFile}"
        os.system(cmd)
        precision = None
        
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        db  = data["databases"][0]["dbinfo"]["name"]
        dbinfo = data["databases"][0]["dbinfo"]
        for key,value in dbinfo.items():
            if key.strip().lower() == "precision":
                precision = value
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]
        start_timestamp = data["databases"][0]["super_tables"][0]["start_timestamp"]
        
        tdLog.info(f"get json info: db={db} precision={precision} stb={stb} child_count={child_count} insert_rows={insert_rows} "
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
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/insertPrecisionMS.json", "")
        # US
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/insertPrecisionUS.json", "")
        # NS
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/insertPrecisionNS.json", "")

    def do_insert_precision(self):
        benchmark = etool.benchMarkFile()

        # vgroups
        self.checkBasic(benchmark)
        print("do insert precision ................... [passed]")

    #
    # ------------------- test_insert_mix.py ----------------
    #
    def checkDataCorrect(self):
        sql = "select count(*) from meters"
        tdSql.query(sql)
        allCnt = tdSql.getData(0, 0)
        if allCnt < 200000:
            tdLog.exit(f"taosbenchmark insert row small. row count={allCnt} sql={sql}")
            return 
        
        # group by 10 child table
        rowCnt = tdSql.query("select count(*),tbname from meters group by tbname")
        tdSql.checkRows(10)

        # interval
        sql = "select count(*),max(ic),min(dc),last(*) from meters interval(1s)"
        rowCnt = tdSql.query(sql)
        if rowCnt < 10:
            tdLog.exit(f"taosbenchmark interval(1s) count small. row cout={rowCnt} sql={sql}")
            return

        # nest query
        tdSql.query("select count(*) from (select * from meters order by ts desc)")
        tdSql.checkData(0, 0, allCnt)

        rowCnt = tdSql.query("select tbname, count(*) from meters partition by tbname slimit 11")
        if rowCnt != 10:
            tdLog.exit("partition by tbname should return 10 rows of table data which is " + str(rowCnt))
            return

    def insert(self, cmd):
        tdLog.info("%s" % cmd)
        errcode = os.system("%s" % cmd)
        if errcode != 0:
            tdLog.exit(f"execute taosBenchmark ret error code={errcode}")
            return 

        tdSql.execute("use mixdb")
        self.checkDataCorrect()   

    def do_insert_mix(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/insertMix.json" % (binPath, os.path.dirname(__file__))
        self.insert(cmd)
        cmd = "%s -f %s/json/insertMixOldRule.json" % (binPath, os.path.dirname(__file__))
        self.insert(cmd)
        cmd = "%s -f %s/json/insertMixAutoCreateTable.json" % (binPath, os.path.dirname(__file__))
        self.insert(cmd)

        print("do insert mix ......................... [passed]")

    #
    # ------------------- test_stt.py ----------------
    #

    def checkDataCorrectStt(self):
        sql = "select count(*) from meters"
        tdSql.query(sql)
        allCnt = tdSql.getData(0, 0)
        if allCnt < 2000000:
            tdLog.exit(f"taosbenchmark insert row small. row count={allCnt} sql={sql}")
            return 
        
        # group by 10 child table
        rowCnt = tdSql.query("select count(*),tbname from meters group by tbname")
        tdSql.checkRows(1000)

        # interval
        sql = "select count(*),max(ic),min(dc),last(*) from meters interval(1s)"
        rowCnt = tdSql.query(sql)
        if rowCnt < 10:
            tdLog.exit(f"taosbenchmark interval(1s) count small. row cout={rowCnt} sql={sql}")
            return

        # nest query
        tdSql.query("select count(*) from (select * from meters order by ts desc)")
        tdSql.checkData(0, 0, allCnt)
    
    def do_stt(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/stt.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        errcode = os.system("%s" % cmd)
        if errcode != 0:
            tdLog.exit(f"execute taosBenchmark ret error code={errcode}")
            return 

        tdSql.execute("use db")
        self.checkDataCorrectStt()
        print("do stt ................................ [passed]")

    #
    # ------------------- test_taos_config_json.py ----------------
    #
    def do_taos_config_json(self):    
        cmd = f"-f {os.path.dirname(__file__)}/json/taos_config.json"
        rlist = self.benchmark(cmd, checkRun=True)
        self.checkListString(rlist, f"Set engine cfgdir successfully, dir:./cases/81-Tools/03-Benchmark/config")    

        print("do taos config set .................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_basic(self):
        """taosBenchmark basic

        1. Insert data with different json files
        2. Check data correct depends on json file
        3. Check taosBenchmark options
        4. Check custom column and tags
        5. Check default json file
        6. Check demo mode
        7. Check from to continue insert
        8. Check from to insert
        9. Check insert auto create table json
        10.Check insert bind vgroup
        11.Check json tag
        12.Check limit offset json
        13.Check insert precision
        14. Check insert with mix mode()
        15. Check insert correctly with different stt options
        16. Check taos config is correctedly set
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_basic.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_custom_col_tag.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_default_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_demo.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_from_to_continue.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_from_to.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_auto_create_table_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_bind_vgroup.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_data.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_json_tag.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_limit_offset_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_precision.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_mix.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stt.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taos_config_json.py


        """
        self.do_stt()
        self.do_insert_basic()
        self.do_custom_col_tag()
        self.do_default_json()
        self.do_demo()
        self.do_from_to_continue()
        self.do_from_to()
        self.do_insert_auto_create_table_json()
        self.do_insert_bind_vgroup()
        self.do_insert_data()
        self.do_json_tag()
        self.do_limit_offset_json()
        self.do_insert_precision()