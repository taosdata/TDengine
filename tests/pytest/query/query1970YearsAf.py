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

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def getCfgDir(self, path):
        binPath = os.path.dirname(os.path.realpath(__file__))
        binPath = binPath + "/../../../debug/"
        tdLog.debug("binPath %s" % (binPath))
        binPath = os.path.realpath(binPath)
        tdLog.debug("binPath real path %s" % (binPath))
        if path == "":
            self.path = os.path.abspath(binPath + "../../")
        else:
            self.path = os.path.realpath(path)

        self.cfgDir = "%s/sim/psim/cfg" % (self.path)
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

        # 设置创建的超级表格式
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
            "rows_per_tbl": 1000,
            "max_sql_len": 65480,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 19000000,
            "start_timestamp": "1969-01-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": [
                {"type": "INT"},
                {"type": "DOUBLE", "count": 10},
                {"type": "BINARY", "len": 16, "count": 3},
                {"type": "BINARY", "len": 32, "count": 6}
            ],
            "tags": [
                {"type": "TINYINT", "count": 2},
                {"type": "BINARY", "len": 16, "count": 5}
            ]
        }

        # 需要创建多个超级表时，只需创建不同的超级表格式并添加至super_tables
        super_tables = [stable1]
        database = {
            "dbinfo": dbinfo,
            "super_tables": super_tables
        }

        cfgdir = self.getCfgDir("")
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

    def inserttable(self):
        create_table = self.creatcfg()
        date = datetime.datetime.now().strftime("%Y%m%d%H%M")
        file_create_table = f"/tmp/insert_{date}.json"

        with open(file_create_table, 'w') as f:
            json.dump(create_table, f)

        create_table_cmd = f"taosdemo -f {file_create_table}"
        _ = subprocess.check_output(create_table_cmd, shell=True).decode("utf-8")


    def run(self):
        s = 'reset query cache'
        tdSql.execute(s)
        s = 'create database if not exists db'
        tdSql.execute(s)
        s = 'use db'
        tdSql.execute(s)

        tdLog.info("==========step1:create table stable and child table,then insert data automatically")
        self.inserttable()
        # tdSql.execute(
        #     '''create table if not exists supt
        #     (ts timestamp, c1 int, c2 float, c3 bigint, c4 double, c5 smallint, c6 tinyint)
        #     tags(location binary(64), type int, isused bool , family nchar(64))'''
        # )
        # tdSql.execute("create table t1 using supt tags('beijing', 1, 1, '自行车')")
        # tdSql.execute("create table t2 using supt tags('shanghai', 2, 0, '拖拉机')")
        # tdSql.execute(
        #     f"insert into t1 values (-31564800000, 6, 5, 4, 3, 2, 1)"
        # )



        tdLog.info("==========step2:query join")

        # stable query
        tdSql.query(
            "select * from stb2 where stb2.ts < '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(16600)

        tdSql.query(
            "select * from stb2 where stb2.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(33400)

        tdSql.query(
            "select * from stb2 where stb2.ts > '1969-12-01 00:00:00.000' and stb2.ts <'1970-01-31 00:00:00.000' "
        )
        tdSql.checkRows(2780)

        # child-table query
        tdSql.query(
            "select * from t0 where t0.ts < '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(1660)

        tdSql.query(
            "select * from t1 where t1.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(3340)

        tdSql.query(
            "select * from t9 where t9.ts > '1969-12-01 00:00:00.000' and t9.ts <'1970-01-31 00:00:00.000' "
        )
        tdSql.checkRows(278)

        tdSql.query(
            "select * from t0,t1 where t0.ts=t1.ts and t1.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(3340)

        tdSql.query(
            "select diff(col1) from t0 where t0.ts >= '1970-01-01 00:00:00.000' "
        )
        tdSql.checkRows(3339)

        tdSql.query(
            "select t0,col1 from stb2 where stb2.ts < '1970-01-01 00:00:00.000' order by ts"
        )
        tdSql.checkRows(16600)

        # query with timestamp in 'where ...'
        tdSql.query(
            "select * from stb2 where stb2.ts > -28800000 "
        )
        tdSql.checkRows(33400)

        tdSql.query(
            "select * from stb2 where stb2.ts > -28800000 and stb2.ts < '1970-01-01 08:00:00.000' "
        )
        tdSql.checkRows(20)

        tdSql.query(
            "select * from stb2 where stb2.ts < -28800000 and stb2.ts > '1969-12-31 16:00:00.000' "
        )


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())