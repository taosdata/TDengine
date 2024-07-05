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
        db_name = 'check_table_content'
        tdSql.execute(f"create database {db_name}", show=True)
        tdSql.execute(f"create table {db_name}.t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)", show=True)
        tdSql.execute(f"insert into {db_name}.ct1 using {db_name}.t100 tags(1) values('2024-05-17 14:58:52.902', 'a1', '100')", show=True)
        tdSql.execute(f"insert into {db_name}.ct1 using {db_name}.t100 tags(1) values('2024-05-17 14:58:52.902', 'a2', '200')", show=True)
        tdSql.query(f"select * from {db_name}.t100 order by ts", show=True)
        tdSql.query(f"select * from {db_name}.t100 order by ts", show=True)
        tdSql.query(f"select * from {db_name}.t100 order by ts", show=True)
        tdSql.execute(f"alter table {db_name}.t100 add column name varchar(10)", show=True)

        tdSql.query("select 1=1")

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
