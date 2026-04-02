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
from new_test_framework.utils import tdLog, tdSql, etool, sc, eos
import os
import time
import subprocess
import json


# reomve single and double quotation
def removeQuotation(origin):
    value = ""
    for c in origin:
        if c != '\'' and c != '"':
            value += c

    return value

class TestBenchmarkWebsite:

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

        try:
            fval = float(value)
        except Exception as e:
            tdLog.error(f"value:'{value}' is not float, key:'{key}' end:'{end}' output:\n{output}")
            # Remove the line breaks and try again
            value = value.splitlines()[0]
            fval = float(value)

        # compare
        if equal and fval != expect:
            tdLog.exit(f"check not expect. expect:{expect} real:{fval}, key:'{key}' end:'{end}' output:\n{output}")
        elif equal == False and fval <= expect:
            tdLog.error(f"value:'{value}' < expect, key:'{key}' end:'{end}' output:\n{output}")
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
        output, error = etool.run(cmd)
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

        threads = data[label]["threads"]
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
        QPS           = 1

        if continueIfFail.lower() == "yes":
            allEnd = " "
        else:
            allEnd = "\n"

        if specMode and mixedQuery.lower() != "yes":
            # spec
            threadQueries = queryTimes * threads
            totalQueries  = queryTimes * threads * len(sqls)
            threadKey     = f"complete query with {threads} threads and "
            qpsKey = "s QPS: "
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
                    QPS           = 1
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
            qpsKey = ""
            avgKey = "avg delay:"
            minKey = "min delay:"

        items = [
            [threadKey, " ", threadQueries, True],
            [qpsKey, " ",  0, False],
            [avgKey, "s",  0, False],
            [minKey, "s",  0, False],
            ["max: ", "s", 0, False],
            ["p90: ", "s", 0, False],
            ["p95: ", "s", 0, False],
            ["p99: ", "s", 0, False],
            ["INFO: Spend ", " ", 0, False],
            ["completed total queries: ", ",", totalQueries, True],
            ["the QPS of all threads:", allEnd, QPS        , False]  # all qps need > 5
        ]

        # check
        for item in items:
            if len(item[0]) > 0:
                self.checkItem(output, item[0], item[1], item[2], item[3])



    # tmq check
    def checkTmqJson(self, benchmark, json):
        OK_RESULT = "Consumed total msgs: 30, total rows: 300000"
        cmd =  benchmark + " -f " + json
        output, error = eos.run(cmd, 600)
        if output.find(OK_RESULT) != -1:
            tdLog.info(f"succ: {cmd} found '{OK_RESULT}'")
        else:
            tdLog.exit(f"failed: {cmd} not found {OK_RESULT} in:\n{output} \nerror:{error}")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_website(self):
        """taosBenchmark official website

        1. Check official website example case run successfully
        2. example files: insert.json/query.json/queryStb.json/tmq.json

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_website_case.py

        """
        tbCnt = 10
        benchmark = etool.benchMarkFile()

        # insert
        json = "../tools/taos-tools/example/insert.json"
        self.insertBenchJson(json, checkStep=True)

        # query
        json = "../tools/taos-tools/example/query.json"
        self.checkAfterRun(benchmark, json, True, tbCnt)
        json = "../tools/taos-tools/example/queryStb.json"
        self.checkAfterRun(benchmark, json, False, tbCnt)

        # tmq
        json = "../tools/taos-tools/example/tmq.json"
        self.checkTmqJson(benchmark, json)


