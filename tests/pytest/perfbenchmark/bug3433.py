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
        tdLog.debug(f"start to execute {__file__}")
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
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def getcfgPath(self, path):
        binPath = os.path.dirname(os.path.realpath(__file__))
        binPath = binPath + "/../../../debug/"
        tdLog.debug(f"binPath {binPath}")
        binPath = os.path.realpath(binPath)
        tdLog.debug(f"binPath real path {binPath}")
        if path == "":
            self.path = os.path.abspath(binPath + "../../")
        else:
            self.path = os.path.realpath(path)
        return self.path

    def getCfgDir(self):
        self.getcfgPath(self.path)
        self.cfgDir = f"{self.path}/sim/psim/cfg"
        return self.cfgDir

    def creatcfg(self):
        dbinfo = {
            "name": "db",
            "drop": "yes",
            "replica": 1,
            "days": 10,
            "cache": 16,
            "blocks": 8,
            "precision": "ms",
            "keep": 3650,
            "minRows": 100,
            "maxRows": 4096,
            "comp": 2,
            "walLevel": 1,
            "cachelast": 0,
            "quorum": 1,
            "fsync": 3000,
            "update": 0
        }

        # set stable schema
        stable1 = {
            "name": "stb1",
            "child_table_exists": "no",
            "childtable_count": 1000,
            "childtable_prefix": "t1",
            "auto_create_table": "no",
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": 1,
            "multi_thread_write_one_tbl": "no",
            "number_of_tbl_in_one_sql": 0,
            "rows_per_tbl": 1,
            "max_sql_len": 65480,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 20000,
            "start_timestamp": "2020-12-31 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": [
                {"type": "INT", "count": 2},
                {"type": "DOUBLE", "count": 2},
                {"type": "BIGINT", "count": 2},
                {"type": "FLOAT", "count": 2},
                {"type": "SMALLINT", "count": 2},
                {"type": "TINYINT", "count": 2},
                {"type": "BOOL", "count": 2},
                {"type": "NCHAR", "len": 3, "count": 1},
                {"type": "BINARY", "len": 8, "count": 1}

            ],
            "tags": [
                {"type": "INT", "count": 2},
                {"type": "DOUBLE", "count": 2},
                {"type": "BIGINT", "count": 2},
                {"type": "FLOAT", "count": 2},
                {"type": "SMALLINT", "count": 2},
                {"type": "TINYINT", "count": 2},
                {"type": "BOOL", "count": 2},
                {"type": "NCHAR", "len": 3, "count": 1},
                {"type": "BINARY", "len": 8, "count": 1}
            ]
        }
        stable2 = {
            "name": "stb2",
            "child_table_exists": "no",
            "childtable_count": 1000,
            "childtable_prefix": "t2",
            "auto_create_table": "no",
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": 1,
            "multi_thread_write_one_tbl": "no",
            "number_of_tbl_in_one_sql": 0,
            "rows_per_tbl": 1,
            "max_sql_len": 65480,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 20000,
            "start_timestamp": "2020-12-31 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": [
                {"type": "INT", "count": 2},
                {"type": "DOUBLE", "count": 2},
                {"type": "BIGINT", "count": 2},
                {"type": "FLOAT", "count": 2},
                {"type": "SMALLINT", "count": 2},
                {"type": "TINYINT", "count": 2},
                {"type": "BOOL", "count": 2},
                {"type": "NCHAR", "len": 3, "count": 1},
                {"type": "BINARY", "len": 8, "count": 1}

            ],
            "tags": [
                {"type": "INT", "count": 2},
                {"type": "DOUBLE", "count": 2},
                {"type": "BIGINT", "count": 2},
                {"type": "FLOAT", "count": 2},
                {"type": "SMALLINT", "count": 2},
                {"type": "TINYINT", "count": 2},
                {"type": "BOOL", "count": 2},
                {"type": "NCHAR", "len": 3, "count": 1},
                {"type": "BINARY", "len": 8, "count": 1}
            ]
        }

        # create different stables like stable1 and add to list super_tables
        super_tables = []
        super_tables.append(stable1)
        super_tables.append(stable2)
        database = {
            "dbinfo": dbinfo,
            "super_tables": super_tables
        }

        cfgdir = self.getCfgDir()
        create_table = {
            "filetype": "insert",
            "cfgdir": cfgdir,
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 4,
            "thread_count_create_tbl": 4,
            "result_file": "/tmp/insert_res.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 100,
            "databases": [database]
        }
        return create_table

    def createinsertfile(self):
        create_table = self.creatcfg()
        date = datetime.datetime.now().strftime("%Y%m%d%H%M")
        file_create_table = f"/tmp/insert_{date}.json"

        with open(file_create_table, 'w') as f:
            json.dump(create_table, f)
        return file_create_table

    def inserttable(self, filepath):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info(f"taosd found in {buildPath}")
        binPath = buildPath + "/build/bin/"

        create_table_cmd = f"{binPath}taosdemo -f {filepath} > /dev/null 2>&1"
        _ = subprocess.check_output(create_table_cmd, shell=True).decode("utf-8")

    def droptmpfile(self):
        drop_file_cmd = "rm -f /tmp/insert_* "
        _ = subprocess.check_output(drop_file_cmd, shell=True).decode("utf-8")

    def run(self):
        tdLog.printNoPrefix("==========step1:create database and insert records")
        file_create_table = self.createinsertfile()
        self.inserttable(file_create_table)

        tdLog.printNoPrefix("==========step2:check database and stable records")
        tdSql.query("show databases")
        tdSql.checkData(0, 2, 2000)
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 1000)
        tdSql.checkData(1, 4, 1000)

        self.droptmpfile()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
