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


import os
import time
from util.log import *

from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import subprocess
from datetime import datetime, timedelta


class TDTestCase:
    def _prepare_env1(self):
        tdLog.info("============== prepare environment 1 ===============")

        level0_paths = [
            f'{self.dnode_path}/data00',
            f'{self.dnode_path}/data01',
            # f'{self.dnode_path}/data02',
        ]

        cfg = {
            f"{level0_paths[0]} 0 1": 'dataDir',
            f"{level0_paths[1]} 0 0": 'dataDir',
            # f"{level0_paths[2]} 0 0": 'dataDir',
        }

        for path in level0_paths:
            tdSql.createDir(path)
        tdDnodes.stop(1)
        tdDnodes.deploy(1, cfg)
        tdDnodes.start(1)

    def _create_db_write_and_flush(self, dbname):
        tdSql.execute(f'create database {dbname} vgroups 1 stt_trigger 1')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(f'create table t1 (ts timestamp, a int) ')
        tdSql.execute(f'create table t2 (ts timestamp, a int) ')

        now = int(datetime.now().timestamp() * 1000)

        for i in range(1000):
            tdSql.execute(f'insert into t1 values ({now + i}, {i})')

        tdSql.execute(f'flush database {dbname}')

        for i in range(1):
            tdSql.execute(f'insert into t2 values ({now + i}, {i})')

        tdSql.execute(f'flush database {dbname}')

    def run(self):
        self._prepare_env1()

        for dbname in [f'db{i}' for i in range(0, 20)]:
            self._create_db_write_and_flush(dbname)

        cmd = f"find {self.dnode_path}/data00 -name 'v*.data' | wc -l"
        num_files1 = int(subprocess.check_output(cmd, shell=True).strip())

        cmd = f"find {self.dnode_path}/data01 -name 'v*.data' | wc -l"
        num_files2 = int(subprocess.check_output(cmd, shell=True).strip())
        tdSql.checkEqual(num_files1, num_files2)

        tdDnodes.stop(1)

        time.sleep(10)

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.dnode_path = tdCom.getTaosdPath()
        self.cfg_path = f'{self.dnode_path}/cfg'
        self.log_path = f'{self.dnode_path}/log'
        self.db_name = 'test'
        self.vgroups = 10

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
