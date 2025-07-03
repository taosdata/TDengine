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
import re
import ast
import subprocess

# from assertpy import assert_that


class TestTaosdemoTestQueryWithJsonMixedQuery:
    # 获取taosc接口查询的结果文件中的内容,返回每行数据,并断言数据的第一列内容。
    def assertfileDataTaosc(self, filename, expectResult):
        self.filename = filename
        self.expectResult = expectResult
        with open("%s" % filename, "r+") as f1:
            for line in f1.readlines():
                queryResultTaosc = line.strip().split()[0]
                self.assertCheck(filename, queryResultTaosc, expectResult)

    # 获取restful接口查询的结果文件中的关键内容,目前的关键内容找到第一个key就跳出循，所以就只有一个数据。后续再修改多个结果文件。
    def getfileDataRestful(self, filename):
        self.filename = filename
        with open("%s" % filename, "r+") as f1:
            for line in f1.readlines():
                contents = line.strip()
                if contents.find("data") != -1:
                    pattern = re.compile("{.*}")
                    contents = pattern.search(contents).group()
                    contentsDict = ast.literal_eval(contents)  # 字符串转换为字典
                    queryResultRest = contentsDict["data"][0][0]
                    break
                else:
                    queryResultRest = ""
        return queryResultRest

    # 获取taosc接口查询次数
    def queryTimesTaosc(self, filename):
        self.filename = filename
        command = "cat %s |wc -l" % filename
        times = int(subprocess.getstatusoutput(command)[1])
        return times

    # 获取restful接口查询次数
    def queryTimesRestful(self, filename):
        self.filename = filename
        command = 'cat %s |grep "200 OK" |wc -l' % filename
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

    def test_taosdemo_test_query_with_json_mixed_query(self):
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

        # use restful api to query
        os.system("%s -f ./tools/benchmark/basic/json/queryInsertrestdata.json" % binPath)
        os.system("%s -f ./tools/benchmark/basic/json/queryRestful.json" % binPath)
        os.system("%s -f ./tools/benchmark/basic/json/queryRestful1.json" % binPath)
        os.system("cat query_res2.txt*  > all_query_res2_rest.txt")

        # correct Times testcases

        queryTimes2Restful = self.queryTimesRestful("all_query_res2_rest.txt")
        self.assertCheck("all_query_res2_rest.txt", queryTimes2Restful, 4)

        # correct data testcase

        data2 = self.getfileDataRestful("all_query_res2_rest.txt")
        print(data2)
        if data2 != "2020-11-01 00:00:00.004" and data2 != "2020-10-31T16:00:00.004Z":
            tdLog.exit(
                "data2 is not 2020-11-01 00:00:00.004 and 2020-10-31T16:00:00.004Z"
            )

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
        tdLog.success("%s successfully executed" % __file__)


