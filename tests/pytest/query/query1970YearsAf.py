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

import taos
import sys
import os
import json
import subprocess
import datetime

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnode

class TDTestCase:

    def __init__(self):
        self.path = ""

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

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
            "keep": 36500,
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
            "name": "stb2",
            "child_table_exists": "no",
            "childtable_count": 10,
            "childtable_prefix": "t",
            "auto_create_table": "no",
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": 5000,
            "multi_thread_write_one_tbl": "no",
            "number_of_tbl_in_one_sql": 0,
            "rows_per_tbl": 1,
            "max_sql_len": 65480,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 20000,
<<<<<<< HEAD
            "start_timestamp": "1969-12-31 00:00:00.000",
=======
            "start_timestamp": "1969-12-30 23:59:40.000",
>>>>>>> 53a128b2a3a469b06ae28ff013397b8d015433c8
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
        create_table_cmd = f"taosdemo -f {filepath}  > /dev/null 2>&1"
        _ = subprocess.check_output(create_table_cmd, shell=True).decode("utf-8")

    def sqlsquery(self):
        # stable query
        tdSql.query(
            "select * from stb2 where stb2.ts < '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(43200)

        tdSql.query(
            "select * from stb2 where stb2.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(6800)

        tdSql.query(
            "select * from stb2 where stb2.ts > '1969-12-31 23:00:00.000' and stb2.ts <'1970-01-01 01:00:00.000' "
        )
        tdSql.checkRows(3590)

<<<<<<< HEAD
        # child-tables query
=======
        # child-table query
>>>>>>> 53a128b2a3a469b06ae28ff013397b8d015433c8
        tdSql.query(
            "select * from t0 where t0.ts < '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(4320)

        tdSql.query(
            "select * from t1 where t1.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(680)

        tdSql.query(
            "select * from t9 where t9.ts > '1969-12-31 22:00:00.000' and t9.ts <'1970-01-01 02:00:00.000' "
        )
        tdSql.checkRows(719)

        tdSql.query(
            "select * from t0,t1 where t0.ts=t1.ts and t1.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(680)

        tdSql.query(
            "select diff(col1) from t0 where t0.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(679)

        tdSql.query(
            "select t0,col1 from stb2 where stb2.ts < '1970-01-01 00:00:00.000' order by ts"
        )
        tdSql.checkRows(43200)

        # query with timestamp in 'where ...'
        tdSql.query(
            "select * from stb2 where stb2.ts > -28800000 "
        )
        tdSql.checkRows(6790)

        tdSql.query(
            "select * from stb2 where stb2.ts > -28800000 and stb2.ts < '1970-01-01 08:00:00.000' "
        )
        tdSql.checkRows(6790)

        tdSql.query(
            "select * from stb2 where stb2.ts < -28800000 and stb2.ts > '1969-12-31 22:00:00.000' "
        )
        tdSql.checkRows(3590)

    def run(self):
        s = 'reset query cache'
        tdSql.execute(s)
        s = 'create database if not exists db'
        tdSql.execute(s)
        s = 'use db'
        tdSql.execute(s)

        tdLog.info("==========step1:create table stable and child table,then insert data automatically")
        insertfile = self.createinsertfile()
        self.inserttable(insertfile)

        tdLog.info("==========step2:query join")
        self.sqlsquery()

        # after wal and sync, check again
        tdSql.query("show dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)

        tdLog.info("==========step3: query join again")
        self.sqlsquery()

<<<<<<< HEAD
        # delete temporary file
        rm_cmd = f"rm -f /tmp/insert* > /dev/null 2>&1"
        _ = subprocess.check_output(rm_cmd, shell=True).decode("utf-8")

    def stop(self):
        tdSql.close()
=======
    def stop(self):
        tdSql.close()
        rm_cmd = f"rm -f /tmp/insert* > /dev/null 2>&1"
        _ = subprocess.check_output(rm_cmd, shell=True).decode("utf-8")
>>>>>>> 53a128b2a3a469b06ae28ff013397b8d015433c8
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())