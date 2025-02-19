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
import sys
import os
import time
import datetime
import platform
import subprocess

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
        taosBenchmark query->Basic test cases
        """

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

        fval = float(value)
        # compare
        if equal and fval != expect:
            tdLog.exit(f"check not expect. expect:{expect} real:{fval}, key:{key} end:{end} output:\n{output}")
        elif equal == False and fval <= expect:
            tdLog.exit(f"failed because {fval} <= {expect}, key:{key} end:{end} output:\n{output}")
        else:
            # succ
            if equal:
                tdLog.info(f"check successfully. key:{key} expect:{expect} real:{fval}")
            else:
                tdLog.info(f"check successfully. key:{key} {fval} > {expect}")

    
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

        concurrent = data[label]["concurrent"]
        sqls       = data[label]["sqls"]


        # mix
        try:
            mixedQuery = data[label]["mixed_query"]
        except:
            mixedQuery = "no"

        tdLog.info(f"queryTimes={queryTimes} concurrent={concurrent} mixedQuery={mixedQuery} len(sqls)={len(sqls)} label={label}\n")

        totalQueries = 0
        threadQueries = 0

        if continueIfFail.lower() == "yes":
            allEnd = " "
        else:
            allEnd = "\n"
        
        if specMode and mixedQuery.lower() != "yes":
            # spec
            threadQueries = queryTimes * concurrent
            totalQueries  = queryTimes * concurrent * len(sqls)
            threadKey     = f"complete query with {concurrent} threads and " 
            qpsKey = "QPS: "
            avgKey = "query delay avg: "
            minKey = "min:"
        else:
            # spec mixed or super 
            if specMode:
                # spec
                totalQueries  = queryTimes * len(sqls)
            else:
                # super
                totalQueries  = queryTimes * len(sqls) * tbCnt
            threadQueries = totalQueries

            nSql = len(sqls)
            if specMode and nSql < concurrent :
                tdLog.info(f"set concurrent = {nSql} because len(sqls) < concurrent")
                concurrent = nSql
            threadKey     = f"using {concurrent} threads complete query "
            qpsKey = ""
            avgKey = "avg delay:"
            minKey = "min delay:"

        items = [
            [threadKey, " ", threadQueries, True],
            [qpsKey, " ",  5, False],  # qps need > 1
            [avgKey, "s",  0, False],
            [minKey, "s",  0, False],
            ["max: ", "s", 0, False],
            ["p90: ", "s", 0, False],
            ["p95: ", "s", 0, False],
            ["p99: ", "s", 0, False],
            ["INFO: Spend ", " ", 0, False],
            ["completed total queries: ", ",", totalQueries, True],
            ["the QPS of all threads:", allEnd, 10         , False]  # all qps need > 5
        ]
    
        # check
        for item in items:
            if len(item[0]) > 0:
                self.checkItem(output, item[0], item[1], item[2], item[3])    

    # native
    def threeQueryMode(self, benchmark, tbCnt, tbRow):
        # json
        args = [
            ["./tools/benchmark/basic/json/queryModeSpec", True],
            ["./tools/benchmark/basic/json/queryModeSpecMix", True],
            ["./tools/benchmark/basic/json/queryModeSuper", False]
        ]

        # native
        for arg in args:
            self.checkAfterRun(benchmark, arg[0] + ".json", arg[1], tbCnt)

        # rest
        for arg in args:
            self.checkAfterRun(benchmark, arg[0] + "Rest.json", arg[1], tbCnt)

    def expectFailed(self, command):
        ret = os.system(command)
        if ret == 0:
            tdLog.exit(f" expect failed but success. command={command}")
        else:
            tdLog.info(f" expect failed is ok. command={command}")


    # check excption
    def exceptTest(self, benchmark, tbCnt, tbRow):
        # 'specified_table_query' and 'super_table_query' error
        self.expectFailed(f"{benchmark} -f  ./tools/benchmark/basic/json/queryErrorNoSpecSuper.json")
        self.expectFailed(f"{benchmark} -f  ./tools/benchmark/basic/json/queryErrorBothSpecSuper.json")
        # json format error
        self.expectFailed(f"{benchmark} -f  ./tools/benchmark/basic/json/queryErrorFormat.json")



    def run(self):
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

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
