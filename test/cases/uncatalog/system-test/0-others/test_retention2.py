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
from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom
import os
import time
import glob
import subprocess
from datetime import datetime, timedelta


class TestRetentionTest2:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

        cls.dnode_path = tdCom.getTaosdPath()
        cls.cfg_path = f'{cls.dnode_path}/cfg'
        cls.log_path = f'{cls.dnode_path}/log'
        cls.db_name = 'test'
        cls.vgroups = 10

    def _prepare_env1(self):
        tdLog.info("============== prepare environment 1 ===============")

        level0_paths = [
            os.path.join(self.dnode_path, "data00"),
            os.path.join(self.dnode_path, "data01"),
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

    def test_retention_test2(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        self._prepare_env1()

        for dbname in [f'db{i}' for i in range(0, 20)]:
            self._create_db_write_and_flush(dbname)

        data_dir0 = os.path.join(self.dnode_path, 'data00')
        num_files1 = len(glob.glob(os.path.join(data_dir0, '**', 'v*.data'), recursive=True))

        data_dir1 = os.path.join(self.dnode_path, 'data01')
        num_files2 = len(glob.glob(os.path.join(data_dir1, '**', 'v*.data'), recursive=True))
        tdSql.checkEqual(num_files1, num_files2)

        tdDnodes.stop(1)

        time.sleep(10)

        tdLog.success("%s successfully executed" % __file__)


