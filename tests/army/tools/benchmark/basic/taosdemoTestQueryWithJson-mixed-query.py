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
import ast
import re

import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *

# from assertpy import assert_that
import subprocess


class TDTestCase(TBase):
    # 获取taosc接口查询的结果文件中的内容,返回每行数据,并断言数据的第一列内容。
    def assertfileDataTaosc(self, filename, expectResult):
        self.filename = filename
        self.expectResult = expectResult
        with open("%s" % filename, "r+") as f1:
            for line in f1.readlines():
                queryResultTaosc = line.strip().split()[0]
                self.assertCheck(filename, queryResultTaosc, expectResult)

    # 获取taosc接口查询次数
    def queryTimesTaosc(self, filename):
        self.filename = filename
        command = "cat %s |wc -l" % filename
        times = int(subprocess.getstatusoutput(command)[1])
        return times

    # 定义断言结果是否正确。不正确返回错误结果，正确即通过。
    def assertCheck(self, filename, queryResult, expectResult):
        self.filename = filename
        self.queryResult = queryResult
        self.expectResult = expectResult
        args0 = (filename, queryResult, expectResult)
        assert queryResult == expectResult, (
            "Queryfile:%s ,result is %s != expect: %s" % args0
        )

    def run(self):
        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark use %s" % binPath)

        # delete useless files
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")

        # taosc query: query specified  table  and query  super table
        os.system("%s -f ./tools/benchmark/basic/json/queryInsertdata.json" % binPath)
        os.system("%s -f ./tools/benchmark/basic/json/queryTaosc-mixed-query.json" % binPath)
        os.system("%s -f ./tools/benchmark/basic/json/queryTaosc-mixed-query1.json" % binPath)
        os.system("cat query_res2.txt* > all_query_res2_taosc.txt")

        # correct Times testcases

        queryTimes2Taosc = self.queryTimesTaosc("all_query_res2_taosc.txt")
        self.assertCheck("all_query_res2_taosc.txt", queryTimes2Taosc, 20)

        # correct data testcase
        self.assertfileDataTaosc("all_query_res2_taosc.txt", "1604160000199")

        # delete useless files
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")

        # query times less than or equal to 100
        assert (
            os.system("%s -f ./tools/benchmark/basic/json/queryInsertdata.json" % binPath) == 0
        )
        assert (
            os.system("%s -f ./tools/benchmark/basic/json/querySpeciMutisql100.json" % binPath)
            != 0
        )
        assert (
            os.system("%s -f ./tools/benchmark/basic/json/querySuperMutisql100.json" % binPath)
            == 0
        )

        # query result print QPS
        os.system("%s -f ./tools/benchmark/basic/json/queryInsertdata.json" % binPath)
        exceptcode = os.system("%s -f ./tools/benchmark/basic/json/queryQps.json" % binPath)
        assert exceptcode == 0

        # 2021.02.09 need modify taosBenchmakr code
        # use illegal or out of range parameters query json file
        os.system("%s -f ./tools/benchmark/basic/json/queryInsertdata.json" % binPath)
        # 2021.02.09 need modify taosBenchmakr code
        # exceptcode = os.system(
        #     "%s -f ./tools/benchmark/basic/json/queryTimes0.json" %
        #     binPath)
        # assert exceptcode != 0

        # 2021.02.09 need modify taosBenchmakr code
        # exceptcode0 = os.system(
        #     "%s -f ./tools/benchmark/basic/json/queryTimesless0.json" %
        #     binPath)
        # assert exceptcode0 != 0

        # exceptcode1 = os.system(
        #     "%s -f ./tools/benchmark/basic/json/queryConcurrent0.json" %
        #     binPath)
        # assert exceptcode2 != 0

        # exceptcode3 = os.system(
        #     "%s -f ./tools/benchmark/basic/json/querrThreadsless0.json" %
        #     binPath)
        # assert exceptcode3 != 0

        # exceptcode4 = os.system(
        #     "%s -f ./tools/benchmark/basic/json/querrThreads0.json" %
        #     binPath)
        # assert exceptcode4 != 0

        # delete useless files
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf 5-taos-tools/taosbenchmark/*.py.sql")
        os.system("rm -rf ./querySystemInfo*")
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")

    # pylint: disable=R0201
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
