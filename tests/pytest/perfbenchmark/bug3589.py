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
import json

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def __init__(self):
        self.path = ""

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/debug/build/bin")]
                    break
        return buildPath

    def getCfgDir(self):
        return self.getBuildPath() + "/sim/psim/cfg"

    def querycfg(self):
        cfgdir = self.getCfgDir()
        querycfg={
            "filetype": "query",
            "cfgdir": cfgdir,
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "confirm_parameter_prompt": "yes",
            "databases": "db",
            "specified_table_query": {
                "query_interval": 0,
                "concurrent": 1,
                "sqls": [
                    {
                        "sql": "select * from t10, t11 where t10.ts=t11.ts"
                    }
                ]
            }
        }

        return querycfg

    def querycfgfile(self):
        querycfg = self.querycfg()
        date = datetime.datetime.now().strftime("%Y%m%d%H%M")
        querycfg.get("specified_table_query").get("sqls")[0]["result"] = f"/tmp/query_{date}.log"
        file_query_table = f"/tmp/query_{date}.json"
        with open(file_query_table, "w") as f:
            json.dump(querycfg, f)

        return [file_query_table, querycfg]

    def querytable(self, filepath):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info(f"taosd found in {buildPath}")
        binPath = buildPath + "/debug/build/bin/"

        query_table_cmd = f"yes | {binPath}taosdemo -f {filepath}"
        _ = subprocess.check_output(query_table_cmd, shell=True).decode("utf-8")

    def checkqueryresult(self, expectrows):
        querycfg = self.querycfgfile()[1]
        result_file = querycfg.get("specified_table_query").get("sqls")[0].get("result") + "-0"
        if result_file:
            check_cmd = f"wc -l {result_file}"
            check_data_init = subprocess.check_output(check_cmd, shell=True).decode("utf-8")
            check_data = int(check_data_init[0])
            if check_data == expectrows:
                tdLog.info(f"queryResultRows:{check_data} == expect:{expectrows}")
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, check_data, expectrows)
                tdLog.exit(f"{args[0]}({args[1]}) failed: result:{args[2]} != expect:{args[3]}")

    def droptmpfile(self):
        drop_file_cmd = "rm -f /tmp/query_* "
        _ = subprocess.check_output(drop_file_cmd, shell=True).decode("utf-8")
        drop_file_cmd = "rm -f querySystemInfo-*"
        _ = subprocess.check_output(drop_file_cmd, shell=True).decode("utf-8")

    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table && insert data")
        tdSql.execute("alter database db keep 36500")
        tdSql.execute(
            "create table stb1 (ts timestamp, c1 int) TAGS(t1 int)"
        )
        tdSql.execute("create table t10 using stb1 tags(1)")
        tdSql.execute("create table t11 using stb1 tags(2)")

        tdSql.execute("insert into t10 values (-865000000, 1)")
        tdSql.execute("insert into t11 values (-865000000, 2)")
        tdSql.execute("insert into t10 values ('1969-12-31 23:59:59.000', 2)")
        tdSql.execute("insert into t11 values ('1969-12-31 23:59:59.000', 3)")
        tdSql.execute("insert into t10 values ('1970-01-01 00:00:00.000', 3)")
        tdSql.execute("insert into t11 values ('1970-01-01 00:00:00.000', 4)")
        tdSql.execute("insert into t10 values (-15230000, 4)")
        tdSql.execute("insert into t11 values (-15230000, 5)")
        tdSql.execute("insert into t10 values (-15220000, 5)")
        tdSql.execute("insert into t11 values (-15220000, 6)")
        tdSql.execute("insert into t10 values (-15210000, 6)")
        tdSql.execute("insert into t11 values (-15210000, 7)")
        tdSql.execute("insert into t10 values (0, 7)")
        tdSql.execute("insert into t11 values (0, 8)")
        tdSql.execute("insert into t10 values ('2020-10-01 00:00:00.000', 8)")
        tdSql.execute("insert into t11 values ('2020-10-01 00:00:00.000', 9)")

        tdLog.printNoPrefix("==========step2:query")
        query_file = self.querycfgfile()[0]
        self.querytable(query_file)
        self.checkqueryresult(8)

        self.droptmpfile()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())