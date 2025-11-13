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
from new_test_framework.utils import tdLog, tdSql, etool
import os
import json
import subprocess


class TestBenchmarkQueryMain:

    def runSeconds(self, command, timeout = 180):
        tdLog.info(f"runSeconds {command} ...")
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait(timeout)

        # get output
        output = process.stdout.read().decode(encoding="gbk")
        error  = process.stderr.read().decode(encoding="gbk")
        return output, error

    def getKeyValue(self, content, key, end):
        # find key
        s = content.find(key)
        if s == -1:
            return False,""
        
        # skip self
        s += len(key)
        # skip blank
        while s < len(content):
            if content[s] != " ":
                break
            s += 1

        # end check
        if s + 1 == len(content):
            return False, ""
        
        # find end
        if len(end) == 0:
            e = -1
        else:
            e = content.find(end, s)
        
        # get value
        if e == -1:
            value = content[s : ]
        else:
            value = content[s : e]

        return True, value    

    def getDbRows(self, times):
        sql = f"select count(*) from test.meters"
        tdSql.waitedQuery(sql, 1, times)
        dbRows = tdSql.getData(0, 0)
        return dbRows
    
    def checkItem(self, output, key, end, expect, equal):
        ret, value = self.getKeyValue(output, key, end)
        if ret == False:
            tdLog.exit(f"not found key:{key}. end:{end} output:\n{output}")

        tdLog.info(f"get key:{key} value:{value} end:{end}, output:\n{output}")
        cleaned_value = value.split("\n")[0]
        fval = float(cleaned_value)
        # compare
        if equal and fval != expect:
            tdLog.exit(f"check not expect. expect:{expect} real:{fval}, key:'{key}' end:'{end}' output:\n{output}")
        elif equal == False and fval <= expect:
            tdLog.exit(f"failed because {fval} <= {expect}, key:'{key}' end:'{end}' output:\n{output}")
        else:
            # succ
            if equal:
                tdLog.info(f"check successfully. key:'{key}' expect:{expect} real:{fval}")
            else:
                tdLog.info(f"check successfully. key:'{key}' {fval} > {expect}")

    
    def checkAfterRun(self, benchmark, jsonFile, specMode, tbCnt):
        # run
        cmd = f"{benchmark} -f {jsonFile}"
        output, error = self.runSeconds(cmd)

        if specMode :
            label = "specified_table_query"
        else:
            label = "super_table_query"

        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        queryTimes = data["query_times"]
        # contineIfFail
        try:
            continueIfFail = data["continue_if_fail"]
        except:
            continueIfFail = "no"

        threads    = data[label]["threads"]
        sqls       = data[label]["sqls"]


        # batch_query
        try:
            batchQuery = data[label]["batch_query"]
        except:
            batchQuery = "no"

        # mixed_query
        try:
            mixedQuery = data[label]["mixed_query"]
        except:
            mixedQuery = "no"

        tdLog.info(f"queryTimes={queryTimes} threads={threads} mixedQuery={mixedQuery} "
                   f"batchQuery={batchQuery} len(sqls)={len(sqls)} label={label}\n")

        totalQueries  = 0
        threadQueries = 0

        if continueIfFail.lower() == "yes":
            allEnd = " "
        else:
            allEnd = "\n"
        
        if specMode and mixedQuery.lower() != "yes":
            # spec
            threadQueries = queryTimes * threads
            totalQueries  = queryTimes * threads * len(sqls)
            threadKey     = f"complete query with {threads} threads and " 
            avgKey = "query delay avg: "
            minKey = "min:"
        else:
            # spec mixed or super 
            
            if specMode:
                totalQueries  = queryTimes * len(sqls)
                # spec mixed
                if batchQuery.lower() == "yes":
                    # batch
                    threadQueries = len(sqls)
                else:
                    threadQueries = totalQueries
            else:
                # super
                totalQueries  = queryTimes * len(sqls) * tbCnt
                threadQueries = totalQueries            

            nSql = len(sqls)
            if specMode and nSql < threads :
                tdLog.info(f"set threads = {nSql} because len(sqls) < threads")
                threads = nSql
            threadKey     = f"using {threads} threads complete query "
            avgKey = "avg delay:"
            minKey = "min delay:"

        items = [
            [threadKey, " ", threadQueries, True],
            [avgKey, "s",  0, False],
            [minKey, "s",  0, False],
            ["max: ", "s", 0, False],
            ["p90: ", "s", 0, False],
            ["p95: ", "s", 0, False],
            ["p99: ", "s", 0, False],
            ["INFO: Spend ", " ", 0, False],
            ["completed total queries: ", ",", totalQueries, True],
        ]
    
        # check
        for item in items:
            if len(item[0]) > 0:
                self.checkItem(output, item[0], item[1], item[2], item[3])

    # native
    def threeQueryMode(self, benchmark, tbCnt, tbRow):
        # json
        args = [
            [f"{os.path.dirname(__file__)}/json/queryModeSpec", True],
            [f"{os.path.dirname(__file__)}/json/queryModeSpecMix", True],
            [f"{os.path.dirname(__file__)}/json/queryModeSpecMixBatch", True],
            [f"{os.path.dirname(__file__)}/json/queryModeSuper", False]
        ]

        # native
        for arg in args:
            self.checkAfterRun(benchmark, arg[0] + ".json", arg[1], tbCnt)

    def expectFailed(self, command):
        ret = os.system(command)
        if ret == 0:
            tdLog.exit(f" expect failed but success. command={command}")
        else:
            tdLog.info(f" expect failed is ok. command={command}")


    # check excption
    def exceptTest(self, benchmark, tbCnt, tbRow):
        # 'specified_table_query' and 'super_table_query' error
        self.expectFailed(f"{benchmark} -f  {os.path.dirname(__file__)}/json/queryErrorNoSpecSuper.json")
        self.expectFailed(f"{benchmark} -f  {os.path.dirname(__file__)}/json/queryErrorBothSpecSuper.json")
        # json format error
        self.expectFailed(f"{benchmark} -f  {os.path.dirname(__file__)}/json/queryErrorFormat.json")
        # batch query
        self.expectFailed(f"{benchmark} -f  {os.path.dirname(__file__)}/json/queryErrorBatchNoMix.json")
        self.expectFailed(f"{benchmark} -f  {os.path.dirname(__file__)}/json/queryErrorBatchRest.json")

    def test_query_main(self):
        """taosBenchmark query basic

        1. Insert test data into benchmark tables.
        2. Run taosBenchmark in three query modes:
            1) specified table query
            2) specified table mixed query
            3) super table query
        3. Validate the output of each query mode to ensure correct execution.
        4. Perform exception tests to verify error handling.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_query_main.py

        """
        tbCnt = 10
        tbRow = 1000
        benchmark = etool.benchMarkFile()

        # insert 
        command = f"{benchmark} -d test -t {tbCnt} -n {tbRow} -I stmt2 -r 100 -y"
        ret = os.system(command)
        if ret !=0 :
            tdLog.exit(f"exec failed. command={command}")

        # query mode test
        self.threeQueryMode(benchmark, tbCnt, tbRow)

        # exception test
        self.exceptTest(benchmark, tbCnt, tbRow); 

        tdLog.success("%s successfully executed" % __file__)


