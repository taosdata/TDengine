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

import sys
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1625068800000000000   # this is timestamp  "2021-07-01 00:00:00"
        self.numberOfTables = 10
        self.numberOfRecords = 100

    def checkCommunity(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            return False
        else:
            return True

    def getPath(self, tool="taosdump"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            return ""
        return paths[0]

    def createdb(self, precision="ns"):
        tb_nums = self.numberOfTables
        per_tb_rows = self.numberOfRecords

        def build_db(precision, start_time):
            tdSql.execute("drop database if exists timedb1")
            tdSql.execute(
                "create database timedb1 duration 10 keep 36500 precision " +
                "\"" +
                precision +
                "\"")

            tdSql.execute("use timedb1")
            tdSql.execute(
                "create stable st(ts timestamp, c1 int, c2 nchar(10),c3 timestamp) tags(t1 int, t2 binary(10))")
            for tb in range(tb_nums):
                tbname = "t" + str(tb)
                tdSql.execute("create table " + tbname +
                              " using st tags(1, 'beijing')")
                sql = "insert into " + tbname + " values"
                currts = start_time
                if precision == "ns":
                    ts_seed = 1000000000
                elif precision == "us":
                    ts_seed = 1000000
                else:
                    ts_seed = 1000

                for i in range(per_tb_rows):
                    sql += "(%d, %d, 'nchar%d',%d)" % (currts + i * ts_seed, i %
                                                       100, i % 100, currts + i * 100)  # currts +1000ms (1000000000ns)
                tdSql.execute(sql)

        if precision == "ns":
            start_time = 1625068800000000000
            build_db(precision, start_time)

        elif precision == "us":
            start_time = 1625068800000000
            build_db(precision, start_time)

        elif precision == "ms":
            start_time = 1625068800000
            build_db(precision, start_time)

        else:
            print("other time precision not valid , please check! ")

    def run(self):

        # clear envs
        os.system("rm -rf ./taosdumptest/")
        tdSql.execute("drop database if exists dumptmp1")
        tdSql.execute("drop database if exists dumptmp2")
        tdSql.execute("drop database if exists dumptmp3")

        if not os.path.exists("./taosdumptest/tmp1"):
            os.makedirs("./taosdumptest/dumptmp1")
        else:
            print("path exist!")

        if not os.path.exists("./taosdumptest/dumptmp2"):
            os.makedirs("./taosdumptest/dumptmp2")

        if not os.path.exists("./taosdumptest/dumptmp3"):
            os.makedirs("./taosdumptest/dumptmp3")

        binPath = self.getPath("taosdump")
        if (binPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found: %s" % binPath)

        # create nano second database

        self.createdb(precision="ns")

        # dump all data

        os.system(
            "%s -g --databases timedb1 -o ./taosdumptest/dumptmp1" %
            binPath)

        # dump part data with -S  -E
        os.system(
            '%s -g --databases timedb1 -S 1625068810000000000 -E 1625068860000000000  -o ./taosdumptest/dumptmp2 ' %
            binPath)
        os.system(
            '%s -g --databases timedb1 -S 1625068810000000000  -o ./taosdumptest/dumptmp3  ' %
            binPath)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp2" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 510)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp3" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 900)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 1000)

        # check data
        origin_res = tdSql.getResult("select * from timedb1.st")
        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        # dump data and check for taosdump
        dump_res = tdSql.getResult("select * from timedb1.st")
        if origin_res == dump_res:
            tdLog.info("test nano second : dump check data pass for all data!")
        else:
            tdLog.info(
                "test nano second : dump check data failed for all data!")

        # us second support test case

        os.system("rm -rf ./taosdumptest/")
        tdSql.execute("drop database if exists timedb1")

        if not os.path.exists("./taosdumptest/tmp1"):
            os.makedirs("./taosdumptest/dumptmp1")
        else:
            print("path exits!")

        if not os.path.exists("./taosdumptest/dumptmp2"):
            os.makedirs("./taosdumptest/dumptmp2")

        if not os.path.exists("./taosdumptest/dumptmp3"):
            os.makedirs("./taosdumptest/dumptmp3")

        binPath = self.getPath()
        if (binPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found: %s" % binPath)

        self.createdb(precision="us")

        os.system(
            "%s -g --databases timedb1 -o ./taosdumptest/dumptmp1" %
            binPath)

        os.system(
            '%s -g --databases timedb1 -S 1625068810000000 -E 1625068860000000  -o ./taosdumptest/dumptmp2 ' %
            binPath)
        os.system(
            '%s -g --databases timedb1 -S 1625068810000000  -o ./taosdumptest/dumptmp3  ' %
            binPath)

        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        os.system("%s -i ./taosdumptest/dumptmp2" % binPath)
        os.system("%s -i ./taosdumptest/dumptmp3" % binPath)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp2" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 510)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp3" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 900)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 1000)

        # check data
        origin_res = tdSql.getResult("select * from timedb1.st")
        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        # dump data and check for taosdump
        dump_res = tdSql.getResult("select * from timedb1.st")
        if origin_res == dump_res:
            tdLog.info("test micro second : dump check data pass for all data!")
        else:
            tdLog.info(
                "test micro second : dump check data failed for all data!")

        # ms second support test case

        os.system("rm -rf ./taosdumptest/")
        tdSql.execute("drop database if exists timedb1")

        if not os.path.exists("./taosdumptest/tmp1"):
            os.makedirs("./taosdumptest/dumptmp1")
        else:
            print("path exits!")

        if not os.path.exists("./taosdumptest/dumptmp2"):
            os.makedirs("./taosdumptest/dumptmp2")

        if not os.path.exists("./taosdumptest/dumptmp3"):
            os.makedirs("./taosdumptest/dumptmp3")

        binPath = self.getPath()
        if (binPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found: %s" % binPath)

        self.createdb(precision="ms")

        os.system(
            "%s -g --databases timedb1 -o ./taosdumptest/dumptmp1" %
            binPath)

        os.system(
            '%s -g --databases timedb1 -S 1625068810000 -E 1625068860000  -o ./taosdumptest/dumptmp2 ' %
            binPath)
        os.system(
            '%s -g --databases timedb1 -S 1625068810000  -o ./taosdumptest/dumptmp3  ' %
            binPath)

        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        os.system("%s -i ./taosdumptest/dumptmp2" % binPath)
        os.system("%s -i ./taosdumptest/dumptmp3" % binPath)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp2" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 510)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp3" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 900)

        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        # dump data and check for taosdump
        tdSql.query("select count(*) from timedb1.st")
        tdSql.checkData(0, 0, 1000)

        # check data
        origin_res = tdSql.getResult("select * from timedb1.st")
        tdSql.execute("drop database timedb1")
        os.system("%s -i ./taosdumptest/dumptmp1" % binPath)
        # dump data and check for taosdump
        dump_res = tdSql.getResult("select * from timedb1.st")
        if origin_res == dump_res:
            tdLog.info(
                "test million second : dump check data pass for all data!")
        else:
            tdLog.info(
                "test million second : dump check data failed for all data!")

        os.system("rm -rf ./taosdumptest/")
        os.system("rm -rf ./dump_result.txt")
        os.system("rm -rf *.py.sql")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
