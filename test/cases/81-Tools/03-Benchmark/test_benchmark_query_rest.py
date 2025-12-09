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
import ast
import os
import re
import subprocess

class TestBenchmarkQueryRest:
    #
    # ------------------- test_query_json.py ----------------
    #    
    def do_benchmark_query_json(self):
        binPath = etool.benchMarkFile()
        os.system(
            "rm -f rest_query_specified-0 rest_query_super-0 taosc_query_specified-0 taosc_query_super-0"
        )
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")
        tdSql.execute("create table stb (ts timestamp, c0 int)  tags (t0 int)")
        tdSql.execute("insert into stb_0 using stb tags (0) values (now, 0)")
        tdSql.execute("insert into stb_1 using stb tags (1) values (now, 1)")
        tdSql.execute("insert into stb_2 using stb tags (2) values (now, 2)")
        cmd = "%s -f %s/json/taosc_query.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        cmd = "%s -f %s/json/taosc_query1.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        with open("%s" % "taosc_query_specified-0", "r+") as f1:
            for line in f1.readlines():
                queryTaosc = line.strip().split()[0]
                assert queryTaosc == "3", "result is %s != expect: 3" % queryTaosc

        with open("%s" % "taosc_query_super-0", "r+") as f1:
            for line in f1.readlines():
                queryTaosc = line.strip().split()[0]
                assert queryTaosc == "1", "result is %s != expect: 1" % queryTaosc

        # split two
        cmd = "%s -f %s/json/rest_query.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        cmd = "%s -f %s/json/rest_query1.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        times = 0
        with open("rest_query_super-0", "r+") as f1:
            for line in f1.readlines():
                contents = line.strip()
                if contents.find("data") != -1:
                    pattern = re.compile("{.*}")
                    contents = pattern.search(contents).group()
                    contentsDict = ast.literal_eval(contents)
                    queryResultRest = contentsDict["data"][0][0]
                    assert queryResultRest == 1, (
                        "result is %s != expect: 1" % queryResultRest
                    )
                    times += 1

        assert times == 3, "result is %s != expect: 3" % times

        times = 0
        with open("rest_query_specified-0", "r+") as f1:
            for line in f1.readlines():
                contents = line.strip()
                if contents.find("data") != -1:
                    pattern = re.compile("{.*}")
                    contents = pattern.search(contents).group()
                    contentsDict = ast.literal_eval(contents)
                    queryResultRest = contentsDict["data"][0][0]
                    assert queryResultRest == 3, (
                        "result is %s != expect: 3" % queryResultRest
                    )
                    times += 1

        assert times == 1, "result is %s != expect: 1" % times

        print("do query json ......................... [passed]")

    #
    # ------------------- test_taosdemo_test_query_with_json.py ----------------
    #

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
        
    def do_taosdemo_test_query_with_json(self):
        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark use %s" % binPath)

        # delete useless files
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")

        # taosc query: query specified  table  and query  super table
        os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__)))
        os.system("%s -f %s/json/queryTaosc.json" % (binPath, os.path.dirname(__file__)))
        # forbid parallel spec query with super query
        os.system("%s -f %s/json/queryTaosc1.json" % (binPath, os.path.dirname(__file__)))
        os.system("cat query_res0.txt* > all_query_res0_taosc.txt")
        os.system("cat query_res1.txt* > all_query_res1_taosc.txt")
        os.system("cat query_res2.txt* > all_query_res2_taosc.txt")

        # correct Times testcases
        queryTimes0Taosc = self.queryTimesTaosc("all_query_res0_taosc.txt")
        self.assertCheck("all_query_res0_taosc.txt", queryTimes0Taosc, 6)

        queryTimes1Taosc = self.queryTimesTaosc("all_query_res1_taosc.txt")
        self.assertCheck("all_query_res1_taosc.txt", queryTimes1Taosc, 6)

        queryTimes2Taosc = self.queryTimesTaosc("all_query_res2_taosc.txt")
        self.assertCheck("all_query_res2_taosc.txt", queryTimes2Taosc, 20)

        # correct data testcase
        self.assertfileDataTaosc("all_query_res0_taosc.txt", "1604160000099")
        self.assertfileDataTaosc("all_query_res1_taosc.txt", "100")
        self.assertfileDataTaosc("all_query_res2_taosc.txt", "1604160000199")

        # delete useless files
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")

        # use restful api to query
        os.system("%s -f %s/json/queryInsertrestdata.json" % (binPath, os.path.dirname(__file__)))
        os.system("%s -f %s/json/queryRestful.json" % (binPath, os.path.dirname(__file__)))
        os.system("%s -f %s/json/queryRestful1.json" % (binPath, os.path.dirname(__file__)))
        os.system("cat query_res0.txt*  > all_query_res0_rest.txt")
        os.system("cat query_res1.txt*  > all_query_res1_rest.txt")
        os.system("cat query_res2.txt*  > all_query_res2_rest.txt")

        # correct Times testcases
        queryTimes0Restful = self.queryTimesRestful("all_query_res0_rest.txt")
        self.assertCheck("all_query_res0_rest.txt", queryTimes0Restful, 6)

        queryTimes1Restful = self.queryTimesRestful("all_query_res1_rest.txt")
        self.assertCheck("all_query_res1_rest.txt", queryTimes1Restful, 6)

        queryTimes2Restful = self.queryTimesRestful("all_query_res2_rest.txt")
        self.assertCheck("all_query_res2_rest.txt", queryTimes2Restful, 4)

        # correct data testcase
        data0 = self.getfileDataRestful("all_query_res0_rest.txt")
        if data0 != "2020-11-01 00:00:00.009" and data0 != "2020-10-31T16:00:00.009Z":
            tdLog.exit(
                "data0 is not 2020-11-01 00:00:00.009 and 2020-10-31T16:00:00.009Z"
            )

        data1 = self.getfileDataRestful("all_query_res1_rest.txt")
        self.assertCheck("all_query_res1_rest.txt", data1, 10)

        data2 = self.getfileDataRestful("all_query_res2_rest.txt")
        print(data2)
        if data2 != "2020-11-01 00:00:00.004" and data2 != "2020-10-31T16:00:00.004Z":
            tdLog.exit(
                "data2 is not 2020-11-01 00:00:00.004 and 2020-10-31T16:00:00.004Z"
            )

        # query times less than or equal to 100
        assert (
            os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__))) == 0
        )
        assert (
            os.system("%s -f %s/json/querySpeciMutisql100.json" % (binPath, os.path.dirname(__file__)))
            != 0
        )
        assert (
            os.system("%s -f %s/json/querySuperMutisql100.json" % (binPath, os.path.dirname(__file__)))
            == 0
        )

        # query result print QPS
        os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__)))
        exceptcode = os.system("%s -f %s/json/queryQps.json" % (binPath, os.path.dirname(__file__)))
        assert exceptcode == 0
        exceptcode = os.system("%s -f %s/json/queryQps1.json" % (binPath, os.path.dirname(__file__)))
        assert exceptcode == 0

        # 2021.02.09 need modify taosBenchmakr code
        # use illegal or out of range parameters query json file
        os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__)))

        # delete useless files
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf %s/*.py.sql" % os.path.dirname(__file__))
        os.system("rm -rf ./querySystemInfo*")
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")
        os.system("rm -rf ./test_query_res0.txt")        

        print("do query taosdemo ......................... [passed]")


    #
    # ------------------- test_taosdemo_test_query_with_json_mixed_query.py ----------------
    #
    def do_taosdemo_test_query_with_json_mixed_query(self):
        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark use %s" % binPath)

        # delete useless files
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")

        # taosc query: query specified  table  and query  super table
        os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__)))
        os.system("%s -f %s/json/queryTaosc-mixed-query.json" % (binPath, os.path.dirname(__file__)))
        os.system("%s -f %s/json/queryTaosc-mixed-query1.json" % (binPath, os.path.dirname(__file__)))
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
        os.system("%s -f %s/json/queryInsertrestdata.json" % (binPath, os.path.dirname(__file__)))
        os.system("%s -f %s/json/queryRestful.json" % (binPath, os.path.dirname(__file__)))
        os.system("%s -f %s/json/queryRestful1.json" % (binPath, os.path.dirname(__file__)))
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
            os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__))) == 0
        )
        assert (
            os.system("%s -f %s/json/querySpeciMutisql100.json" % (binPath, os.path.dirname(__file__)))
            != 0
        )
        assert (
            os.system("%s -f %s/json/querySuperMutisql100.json" % (binPath, os.path.dirname(__file__)))
            == 0
        )

        # query result print QPS
        os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__)))
        exceptcode = os.system("%s -f %s/json/queryQps.json" % (binPath, os.path.dirname(__file__)))
        assert exceptcode == 0

        # 2021.02.09 need modify taosBenchmakr code
        # use illegal or out of range parameters query json file
        os.system("%s -f %s/json/queryInsertdata.json" % (binPath, os.path.dirname(__file__)))
        # 2021.02.09 need modify taosBenchmakr code
        # exceptcode = os.system(
        #     "%s -f %s/json/queryTimes0.json" %
        #     binPath)
        # assert exceptcode != 0

        # 2021.02.09 need modify taosBenchmakr code
        # exceptcode0 = os.system(
        #     "%s -f ./81-Tools/03-Benchmark/json/queryTimesless0.json" %
        #     binPath)
        # assert exceptcode0 != 0

        # exceptcode1 = os.system(
        #     "%s -f ./81-Tools/03-Benchmark/json/queryConcurrent0.json" %
        #     binPath)
        # assert exceptcode2 != 0

        # exceptcode3 = os.system(
        #     "%s -f ./81-Tools/03-Benchmark/json/querrThreadsless0.json" %
        #     binPath)
        # assert exceptcode3 != 0

        # exceptcode4 = os.system(
        #     "%s -f ./81-Tools/03-Benchmark/json/querrThreads0.json" %
        #     binPath)
        # assert exceptcode4 != 0

        # delete useless files
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf 5-taos-tools/taosbenchmark/*.py.sql")
        os.system("rm -rf ./querySystemInfo*")
        os.system("rm -rf ./query_res*")
        os.system("rm -rf ./all_query*")

        print("do query taosdemo mix ................. [passed]")
    
    #
    # ------------------- main ----------------
    #
    def test_benchmark_basic(self):
        """taosBenchmark query with json

        1. Create database and super table
        2. Create sub-tables and insert data
        3. taosBenchmark run with taosc_query.json
        4. taosBenchmark run with rest_query.json
        5. taosBenchmark run with taosc_query_mixed_query.json
        6. taosBenchmark run with rest_query_mixed_query.json
        7. Verify query result correct
        8. Verify query times correct
        9. Verify query with times less than or equal to 100
        10. Verify query result print QPS
        11. Use illegal or out of range parameters query json file


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_query_json.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosdemo_test_query_with_json_mixed_query.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosdemo_test_query_with_json.py

        """
        self.do_benchmark_query_json()
        self.do_taosdemo_test_query_with_json()
        self.do_taosdemo_test_query_with_json_mixed_query()