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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


# reomve single and double quotation
def removeQuotation(origin):
    value = ""
    for c in origin:
        if c != '\'' and c != '"':
            value += c

    return value

class TDTestCase:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        elif "src" in selfPath:
            projPath = selfPath[: selfPath.find("src")]
        elif "/tools/" in selfPath:
            projPath = selfPath[: selfPath.find("/tools/")]
        else:
            projPath = selfPath[: selfPath.find("tests")]

        paths = []
        for root, dummy, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def testBenchmarkJson(self, benchmark, jsonFile, options="", checkStep=False):
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
                value = removeQuotation(cachemode)
                tdLog.info(f" deal both origin={cachemode} after={value}")
                tdSql.checkData(0, 1, value)

            if vgroups != None:
                tdSql.checkData(0, 0, vgroups)


    # bugs ts
    def bugsTS(self, benchmark):
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TS-5002.json")
        # TS-5234
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TS-5234-1.json")
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TS-5234-2.json")
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TS-5234-3.json")

    # bugs td
    def bugsTD(self, benchmark):
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TD-31490.json", checkStep = False)
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TD-31575.json")
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TD-32846.json")
        
        # no drop
        db      = "td32913db"
        vgroups = 4
        tdSql.execute(f"create database {db} vgroups {vgroups}")
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TD-32913.json", options="-Q")
        tdSql.query(f"select `vgroups` from information_schema.ins_databases where name='{db}';")
        tdSql.checkData(0, 0, vgroups)

        # other
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TD-32913-1.json")
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TD-32913-2.json", options="-T 6")
        self.testBenchmarkJson(benchmark, "./taosbenchmark/json/TD-32913-3.json")

    def run(self):
        benchmark = self.getPath()

        # ts
        self.bugsTS(benchmark)

        # td
        self.bugsTD(benchmark)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
