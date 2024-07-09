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
from monitor.common import *

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.server.dnodes import *
from frame.srvCtl import *
from frame.taosadapter import *


class TDTestCase(TBase):

    updatecfgDict = {
        "slowLogThresholdTest": "0",  # special setting only for testing
        "slowLogExceptDb": "log",
        "monitor": "1",
        'monitorInterval': '1',
        "monitorFqdn": "localhost",
        'slowLogScope': 'ALL'
    }
    
    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        monitor_common.init_env()
        # create user
        user_name = 'slow_sql_user'
        tdSql.execute(f"create user {user_name} pass 'test';")

        # connnect to new user 
        testconn = taos.connect(user=user_name, password='test')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)


        db_name = 'check_table_content'
        testSql.execute(f"create database {db_name}", show=True)
        testSql.execute(f"create table {db_name}.t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)", show=True)
        testSql.execute(f"insert into {db_name}.ct1 using {db_name}.t100 tags(1) values('2024-05-17 14:58:52.902', 'a1', '100')", show=True)
        testSql.execute(f"insert into {db_name}.ct1 using {db_name}.t100 tags(1) values('2024-05-17 14:58:52.902', 'a2', '200')", show=True)
        testSql.query(f"select * from {db_name}.t100 order by ts", show=True)
        testSql.query(f"select * from {db_name}.t100 order by ts", show=True)
        testSql.query(f"select * from {db_name}.t100 order by ts", show=True)
        testSql.execute(f"alter table {db_name}.t100 add column name varchar(10)", show=True)
        testSql.error(f"alter table {db_name}.t101 add column name varchar(10)")

        testSql.query("select 1=1")
        time.sleep(2)

        cluster_id = tdSql.getResult("select id from information_schema.ins_cluster")[0][0]

        tdSql.query("select * from log.taos_slow_sql_detail  where sql like '%check_table_content%' order by start_ts desc")
        tdSql.checkRows(9)

        # check start_ts
        # tdLog.info('check column start_ts - START')
        # tdSql.checkData(0, 4, '')
        # tdLog.info('check column start_ts - PASS')

        # check request_id
        # tdLog.info('check column request_id - START')
        # tdSql.checkData(0, 4, '')
        # tdLog.info('check column request_id - PASS')

        # check query_time
        # tdLog.info('check column query_time - START')
        # tdSql.checkData(0, 4, '')
        # tdLog.info('check column query_time - PASS')

        # check code
        tdLog.info('check column code - START')
        tdSql.checkData(0, 3, '-2147473917')
        tdLog.info('check column code - PASS')

        # check error_info
        tdLog.info('check column error_info - START')
        tdSql.checkData(0, 4, 'Table does not exist')
        tdLog.info('check column error_info - PASS')

        # check type
        tdLog.info('check column type - START')
        tdSql.checkData(4, 5, 1)
        tdLog.info('check column type - PASS')

        # check rows_num
        tdLog.info('check column rows_num - START')
        tdSql.checkData(3, 6, 2)
        tdSql.checkData(6, 6, 1)
        tdLog.info('check column rows_num - PASS')

        # check sql
        tdLog.info('check column sql - START')
        tdLog.info(f'check process_id: {f"alter table {db_name}.t100 add column name varchar(10)"}')
        tdSql.checkData(1, 7, f"alter table {db_name}.t100 add column name varchar(10)")
        tdLog.info('check column procsqless_id - PASS')

        # check process_name
        tdLog.info('check column process_name - START')
        tdLog.info(f'check process_id: python3.8')
        tdSql.checkData(0, 8, 'python3', fuzzy=True)
        tdLog.info('check column process_name - PASS')

        # check process_id
        tdLog.info('check column process_id - START')
        current_pid = os.getpid()
        tdLog.info(f'check process_id: {current_pid}')
        tdSql.checkData(0, 9, current_pid)
        tdLog.info('check column process_id - PASS')

        # check db
        tdLog.info('check column db - START')
        tdLog.info(f'check db: {db_name}')
        tdSql.checkData(1, 10, db_name)
        tdLog.info('check column db - PASS')

        # check user
        tdLog.info('check column user - START')
        tdLog.info(f'check user: {user_name}')
        tdSql.checkData(0, 11, user_name)
        tdLog.info('check column user - PASS')

        # check ip
        tdLog.info('check column ip - START')
        tdLog.info(f'check ip: localhost')
        tdSql.checkData(0, 12, 'localhost')
        tdLog.info('check column ip - PASS')

        # check cluster_id
        tdLog.info('check column cluster_id - START')
        tdLog.info(f'cluster_id: {cluster_id}')
        tdSql.checkData(0, 13, cluster_id)
        tdLog.info('check column cluster_id - PASS')

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
