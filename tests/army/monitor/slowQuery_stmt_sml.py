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
import time
import shutil
import socket
import taos
import frame
import frame.etool
import frame.eutil
import shutil

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.server.dnodes import *
from frame.srvCtl import *
from frame.taosadapter import *
from monitor.common import *
from taos import SmlProtocol, SmlPrecision


class TDTestCase(TBase):

    updatecfgDict = {
        "slowLogThresholdTest": "0",  # special setting only for testing
        "slowLogExceptDb": "log",
        "monitor": "1",
        "monitorInterval": "1",
        "monitorFqdn": "localhost"
        # "monitorLogProtocol": "1"
        # "monitorPort": "6043"
    }
    
    def test_stmt(self):
        # bind_param_batch
        db_name = 'stmt_bind_param_batch'
        stable_name = 'stable_stmt_bind_param_batch'
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name}")
        tdSql.execute(f"USE {db_name}")
        tdSql.execute(f"drop table if exists {stable_name}")
        tdSql.execute(f"create table {stable_name} (ts timestamp, pk int primary key, c2 double, c3 float) tags (engine int)")

        sql = f"INSERT INTO ? USING {stable_name} TAGS(?) VALUES (?,?,?,?)"

        conn = taos.connect()
        conn.select_db(db_name)
        stmt = conn.statement(sql)
        
        tbname = f"d1001"

        tags = taos.new_bind_params(1)
        tags[0].int([2])

        stmt.set_tbname_tags(tbname, tags)

        params = taos.new_bind_params(4)
        params[0].timestamp((1626861392589, 1626861392589, 1626861392592))
        params[1].int((10, 12, 12))
        params[2].double([194, 200, 201])
        params[3].float([0.31, 0.33, 0.31])

        stmt.bind_param_batch(params)

        stmt.execute()

        stmt=conn.statement(f"select * from {stable_name} where pk = ?")
        queryparam1=taos.new_bind_params(1)
        queryparam1[0].int(10)
        stmt.bind_param(queryparam1)
        stmt.execute()
        result1=stmt.use_result()
        rows1=result1.fetch_all()
        result1.close()
        time.sleep(2)

        tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and type=2', queryTimes=1)
        tdSql.checkRows(1)

        # bind_param
        db_name = 'stmt_bind_param'
        stable_name = 'stable_stmt_bind_param'
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name}")
        tdSql.execute(f"USE {db_name}")
        tdSql.execute(f"drop table if exists {stable_name}")
        tdSql.execute(f"create table {stable_name} (ts timestamp, pk int primary key, c2 double, c3 float) tags (engine int)")

        params = taos.new_bind_params(4)
        params[0].timestamp((1626861392589))
        params[1].int((11))
        params[2].int([199])
        params[3].float([0.31])

        stmt.bind_param(params)

        stmt.execute()

        tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and type=2', queryTimes=1)
        tdSql.checkRows(1)

        stmt.close()

    def test_schemaless(self):
        # 5.1.insert into values via influxDB
        lines = ["meters,location=California.LosAngeles,groupid=2 current=11i32,voltage=221,phase=0.28 1648432611249000",
         "meters,location=California.LosAngeles,groupid=2 current=13i32,voltage=223,phase=0.29 1648432611249000",
         "meters,location=California.LosAngeles,groupid=3 current=10i32,voltage=223,phase=0.29 1648432611249300",
         "meters,location=California.LosAngeles,groupid=3 current=11i32,voltage=221,phase=0.35 1648432611249300",
         ]
        conn = taos.connect()
        conn.execute("drop database if exists influxDB")
        conn.execute("CREATE DATABASE influxDB precision 'us'")
        conn.execute("USE influxDB")
        conn.execute("CREATE STABLE `meters` (`_ts` TIMESTAMP, `current` int, `voltage` DOUBLE, `phase` DOUBLE) TAGS (`location` NCHAR(32), `groupid` NCHAR(2))")
    
        conn.schemaless_insert(lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)

        tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and type=2', queryTimes=1)
        tdSql.checkRows(1)

        # 5.2.insert into values via OpenTSDB
        lines = ["meters.current 1648432611249 10i32 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611250 12i32 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611249 10i32 location=California.LosAngeles groupid=3",
         "meters.current 1648432611250 11i32 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611249 219i32 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611250 218i32 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611249 221i32 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611250 217i32 location=California.LosAngeles groupid=3",
         ]
        try:
            conn = taos.connect()
            conn.execute("drop database if exists OpenTSDB")
            conn.execute("CREATE DATABASE OpenTSDB precision 'us'")
            conn.execute("USE OpenTSDB")
            conn.execute("CREATE STABLE `meters_current` (`_ts` TIMESTAMP, `_value` INT primary key) TAGS (`location` NCHAR(32), `groupid` NCHAR(2))")
            conn.execute("CREATE TABLE `t_c66ea0b2497be26ca9d328b59c39dd61` USING `meters_current` (`location`, `groupid`) TAGS ('California.LosAngeles', '3')")
            conn.execute("CREATE TABLE `t_e71c6cf63cfcabb0e261886adea02274` USING `meters_current` (`location`, `groupid`) TAGS ('California.SanFrancisco', '2')")
            conn.schemaless_insert(lines, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual(False, True)
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg == 'Can not insert data into table with primary key', True)

        # 5.3.insert into values via OpenTSDB Json
        lines = [{"metric": "meters.current", "timestamp": 1648432611249, "value": "a32", "tags": {"location": "California.SanFrancisco", "groupid": 2}}]
        try:
            conn = taos.connect()
            conn.execute("drop database if exists OpenTSDBJson")
            conn.execute("CREATE DATABASE OpenTSDBJson")
            conn.execute("USE OpenTSDBJson")
            conn.execute("CREATE STABLE `meters_current` (`_ts` TIMESTAMP, `_value` varchar(10) primary key) TAGS (`location` VARCHAR(32), `groupid` DOUBLE)")
            conn.execute("CREATE TABLE `t_71d176bfc4c952b64d30d719004807a0` USING `meters_current` (`location`, `groupid`) TAGS ('California.SanFrancisco', 2.000000e+00)")
            # global lines
            lines = json.dumps(lines)
            # note: the first parameter must be a list with only one element.
            conn.schemaless_insert([lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual(False, True)
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg == 'Can not insert data into table with primary key', True)

        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
    # run
    def run(self):
        tdLog.info(f"check_show_log_scope")

        monitor_common.init_env()

        # 1.test sml
        # updatecfgDict = {"slowLogScope": "INSERT"}
        # monitor_common.alter_variables(1, updatecfgDict)
        # self.test_schemaless()

        # monitor_common.update_taos_cfg(1, updatecfgDict)
        # self.test_schemaless()

        # updatecfgDict = {"slowLogScope": "ALL"}
        # monitor_common.alter_variables(1, updatecfgDict)
        # self.test_schemaless()

        # monitor_common.update_taos_cfg(1, updatecfgDict)
        # self.test_schemaless()

        # 2.test stmt
        # updatecfgDict = {"slowLogScope": "INSERT"}
        # monitor_common.alter_variables(1, updatecfgDict)
        # self.test_stmt()

        # monitor_common.update_taos_cfg(1, updatecfgDict)
        # self.test_stmt()

        updatecfgDict = {"slowLogScope": "ALL"}
        monitor_common.alter_variables(1, updatecfgDict)
        self.test_stmt()

        monitor_common.update_taos_cfg(1, updatecfgDict)
        self.test_stmt()

        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
