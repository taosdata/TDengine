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
import subprocess
import time
from faker import Faker
from new_test_framework.utils import tdLog, tdSql, etool, eutil, sc

class TestBugs:

    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    #
    # ------------------- test_bugs.py ----------------
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


    def run_benchmark_json(self, benchmark, jsonFile, options="", checkStep=False):
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


        # cachemodel
        try:
            cachemode = data["databases"][0]["dbinfo"]["cachemodel"]
        except:
            cachemode = None

        # vgropus
        try:
            vgroups   = data["databases"][0]["dbinfo"]["vgroups"]
        except:
            vgroups = None

        tdLog.info(f"get json info: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkStep:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

        if drop.lower() == "yes":
            # check database optins 
            sql = f"select `vgroups`,`cachemodel` from information_schema.ins_databases where name='{db}';"
            tdSql.query(sql)

            if cachemode != None:
                value = eutil.removeQuota(cachemode)
                tdLog.info(f" deal both origin={cachemode} after={value}")
                tdSql.checkData(0, 1, value)

            if vgroups != None:
                tdSql.checkData(0, 0, vgroups)


    # bugs ts
    def bugsTS(self, benchmark):
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TS-5002.json")
        # TS-5234
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TS-5234-1.json")
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TS-5234-2.json")
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TS-5234-3.json")
        # TS-5846
        keys = ["completed total queries: 40"]
        self.benchmarkQuery(benchmark, f"{os.path.dirname(__file__)}/json/TS-5846-Query.json", keys)
        keys = ["completed total queries: 20"]
        self.benchmarkQuery(benchmark, f"{os.path.dirname(__file__)}/json/TS-5846-Mixed-Query.json", keys)

    # bugs td
    def bugsTD(self, benchmark):
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TD-31490.json", checkStep = False)
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TD-31575.json")
        # self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TD-32846.json")
        
        # no drop
        db      = "td32913db"
        vgroups = 4
        tdSql.execute(f"create database {db} vgroups {vgroups}")
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TD-32913.json", options="-Q")
        tdSql.query(f"select `vgroups` from information_schema.ins_databases where name='{db}';")
        tdSql.checkData(0, 0, vgroups)

        # other
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TD-32913-1.json")
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TD-32913-2.json", options="-T 6")
        self.run_benchmark_json(benchmark, f"{os.path.dirname(__file__)}/json/TD-32913-3.json")


    #
    # ------------------- test_reuse_exist_stb.py ----------------
    #




    """
    [TD-22190] taosBenchmark reuse exist stb test cases
    """
    def do_reuse_exist_stb(self):
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
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")
        tdSql.execute("create table stb (ts timestamp, c0 int)  tags (t0 int)")
        tdSql.execute("insert into stb_0 using stb tags (0) values (now, 0)")
        #        sys.exit(0)
        cmd = "%s -f %s/json/reuse-exist-stb.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.new_0")
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count(*) from db.stb_0")
        tdSql.checkData(0, 0, 1)

        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb)")
        else:
            tdSql.query("select count(tbname) from db.stb")
        tdSql.checkData(0, 0, 2)

        print("do jira TD-22190 ...................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_bugs(self):
        """taosBenchmark bugs

        1. Verify bug TD-32913
        2. Verify bug TD-31575
        3. Verify bug TD-31490
        4. Verify bug TS-5846
        5. Verify bug TS-5234
        6. Verify bug TD-22190

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_bugs.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_reuse_exist_stb.py

        """

        benchmark = etool.benchMarkFile()
        # ts
        self.bugsTS(benchmark)

        # td
        self.bugsTD(benchmark)
        self.do_reuse_exist_stb()

