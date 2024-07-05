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

global_count_stmt = 0
global_count_sml = 0


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
    
    def test_stmt(self, slowLogScope: str = "ALL"):
        global global_count_stmt
        db_name = 'stmt_bind_param_batch' + str(global_count_stmt)

        global_count_stmt = global_count_stmt + 1

        stable_name = 'stable_stmt_bind_param_batch'
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name}")
        tdSql.execute(f"USE {db_name}")
        tdSql.execute(f"drop table if exists {stable_name}")
        tdSql.execute(f"create table {stable_name} (ts timestamp, pk int primary key, c2 double, c3 float) tags (engine int)")

        sql1 = f"INSERT INTO ? USING {stable_name} TAGS(?) VALUES (?,?,?,?)"

        conn = taos.connect()
        conn.select_db(db_name)
        stmt = conn.statement(sql1)
        
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

        sql2 = f"select * from {stable_name} where pk = ?"

        stmt=conn.statement(sql2)
        queryparam1=taos.new_bind_params(1)
        queryparam1[0].int(10)
        stmt.bind_param(queryparam1)
        stmt.execute()
        
        stmt=conn.statement(f"select 1=?")
        queryparam1=taos.new_bind_params(1)
        queryparam1[0].int(1)
        stmt.bind_param(queryparam1)
        stmt.execute()

        time.sleep(2)

        if slowLogScope == 'ALL':
            tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and sql="{sql1.lower()}" and type=2')
            tdSql.checkRows(1)
            tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and sql="{sql2.lower()}" and type=1')
            tdSql.checkRows(1)
        if slowLogScope == 'INSERT':
            tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and sql="{sql1.lower()}" and type=2')
            tdSql.checkRows(1)
            tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and sql="{sql2.lower()}" and type=1')
            tdSql.checkRows(0)

        stmt.close()

    def test_schemaless(self):
        # 5.1.insert into values via influxDB
        global global_count_sml
        db_name = 'sml_influxdb' + str(global_count_sml)
        # global_count_sml = global_count_sml + 1

        lines = ["meters,location=California.LosAngeles,groupid=2 current=11i32,voltage=221,phase=0.28 1648432611249000",
         "meters,location=California.LosAngeles,groupid=2 current=13i32,voltage=223,phase=0.29 1648432611249000",
         "meters,location=California.LosAngeles,groupid=3 current=10i32,voltage=223,phase=0.29 1648432611249300",
         "meters,location=California.LosAngeles,groupid=3 current=11i32,voltage=221,phase=0.35 1648432611249300",
         ]
        conn = taos.connect()
        conn.execute(f"drop database if exists {db_name}")
        conn.execute(f"CREATE DATABASE {db_name} precision 'us'")
        conn.execute(f"USE {db_name}")
        conn.execute("CREATE STABLE `meters` (`_ts` TIMESTAMP, `current` int, `voltage` DOUBLE, `phase` DOUBLE) TAGS (`location` NCHAR(32), `groupid` NCHAR(2))")
    
        conn.schemaless_insert(lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)

        time.sleep(2)

        tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and type=2', queryTimes=1)
        tdSql.checkRows(1)

        # 5.2.insert into values via OpenTSDB
        db_name = 'sml_opentsdb' + str(global_count_sml)

        lines = [
            "meters.current 1648432611249 10i32 location=California.SanFrancisco groupid=2",
            "meters.current 1648432611250 12i32 location=California.SanFrancisco groupid=2",
        ]

        conn = taos.connect()
        conn.execute(f"drop database if exists {db_name}")
        conn.execute(f"CREATE DATABASE {db_name} precision 'us'")
        conn.execute(f"USE {db_name}")
        conn.execute("CREATE STABLE `meters_current` (`_ts` TIMESTAMP, `_value` INT) TAGS (`location` NCHAR(32), `groupid` NCHAR(2))")
        conn.schemaless_insert(lines, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)

        time.sleep(2)

        tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and type=2', queryTimes=1)
        tdSql.checkRows(1)

        # 5.3.insert into values via OpenTSDB Json
        db_name = 'sml_opentsdb_json' + str(global_count_sml)
        global_count_sml = global_count_sml + 1

        lines = [{"metric": "meters.current", "timestamp": 1648432611249, "value": "a32", "tags": {"location": "California.SanFrancisco", "groupid": 2}}]

        conn = taos.connect()
        conn.execute(f"drop database if exists {db_name}")
        conn.execute(f"CREATE DATABASE {db_name}")
        conn.execute(f"USE {db_name}")
        conn.execute("CREATE STABLE `meters_current` (`_ts` TIMESTAMP, `_value` varchar(10)) TAGS (`location` VARCHAR(32), `groupid` DOUBLE)")
        # global lines
        lines = json.dumps(lines)
        # note: the first parameter must be a list with only one element.
        conn.schemaless_insert([lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)

        time.sleep(2)

        tdSql.query(f'select * from log.taos_slow_sql_detail where db="{db_name}" and type=2', queryTimes=1)
        tdSql.checkRows(1)
    # run
    def run(self):
        tdLog.info(f"check_show_log_scope")

        monitor_common.init_env()

        # 1.test sml
        updatecfgDict = {"slowLogScope": "INSERT"}
        monitor_common.alter_variables(1, updatecfgDict)
        self.test_schemaless()

        monitor_common.update_taos_cfg(1, updatecfgDict)
        self.test_schemaless()

        updatecfgDict = {"slowLogScope": "ALL"}
        monitor_common.alter_variables(1, updatecfgDict)
        self.test_schemaless()

        monitor_common.update_taos_cfg(1, updatecfgDict)
        self.test_schemaless()

        # 2.test stmt
        updatecfgDict = {"slowLogScope": "INSERT"}
        monitor_common.alter_variables(1, updatecfgDict)
        self.test_stmt(slowLogScope="INSERT")

        monitor_common.update_taos_cfg(1, updatecfgDict)
        self.test_stmt(slowLogScope="INSERT")

        updatecfgDict = {"slowLogScope": "ALL"}
        monitor_common.alter_variables(1, updatecfgDict)
        self.test_stmt(slowLogScope="ALL")

        monitor_common.update_taos_cfg(1, updatecfgDict)
        self.test_stmt(slowLogScope="ALL")

        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
