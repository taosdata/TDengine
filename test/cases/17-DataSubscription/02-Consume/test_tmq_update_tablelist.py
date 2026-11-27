# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time
import threading
import math

from new_test_framework.utils import tdLog, tdSql, tdCom, tmqCom
from taos.tmq import *
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def consume1(self):
        tdSql.execute(f'use db_alter_tag')
        tdSql.execute(f'create topic t0 as stable stb where id > 2')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        tdLog.info("subscribe success")
        tdSql.execute("ALTER TABLE tb1 SET TAG id = 10, t1 = 0")
        tdSql.execute("ALTER TABLE tb3 SET TAG id = 1, t1 = 0")
        tdSql.execute("drop TABLE tb1")
        tdSql.execute("drop TABLE tb3")

        cnt = 0
        try:
            while True:
                res = consumer.poll(5)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    cnt += len(block.fetchall())
        finally:
            consumer.close()

        if cnt != 2:
            tdLog.exit(f"consume 1 error, cnt: {cnt}")
        tdLog.info(f"consume1 completed, total rows: {cnt}")

    def consume2(self):
        tdSql.execute(f'use db_alter_tag')
        tdSql.execute(f'create topic t1 as stable stb where t1 > 2')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t1"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        tdLog.info("subscribe success")
        tdSql.execute("ALTER TABLE tb1 SET TAG id = 6")

        cnt = 0
        try:
            while True:
                res = consumer.poll(5)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    cnt += len(block.fetchall())
                    
        finally:
            consumer.close()
        if cnt != 2:
            tdLog.exit(f"consume 2 error, cnt: {cnt}")
        tdSql.execute(f'drop topic t1')
        tdLog.info(f"consume2 completed, total rows: {cnt}")

    def consume3(self):
        tdSql.execute(f'use db_alter_tag')
        tdSql.execute(f'create topic t2 as stable stb where t1 > 2')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t2"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        tdLog.info("subscribe success")
        tdSql.execute("ALTER TABLE tb3 SET TAG id = 6")

        cnt = 0
        try:
            while True:
                res = consumer.poll(5)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    cnt += len(block.fetchall())
                    
        finally:
            consumer.close()
        if cnt != 2:
            tdLog.exit(f"consume 2 error, cnt: {cnt}")
        tdSql.execute(f'drop topic t2')
        tdLog.info(f"consume2 completed, total rows: {cnt}")

    def insertData(self):
        tdSql.execute("drop database if exists db_alter_tag")
        tdSql.execute("create database db_alter_tag vgroups 1")
        tdSql.execute("use db_alter_tag")
        tdSql.execute("create table stb (ts timestamp, val double) tags (id int, t1 int)")
        tdSql.execute("insert into tb1 using stb tags (1, 1) values (now, 10.5)")
        tdSql.execute("insert into tb2 using stb tags (2, 2) values (now, 20.5)")
        tdSql.execute("insert into tb3 using stb tags (3, 3) values (now, 30.5)")
        tdSql.execute("insert into tb4 using stb tags (4, 4) values (now, 40.5)")
    
    def test_tmq_update_tablelist(self):
        """Consume: update tablelist for tmq
        
        1. Insert data to TSDB
        2. Create filtered topic (SELECT with WHERE)
        3. Enable snapshot consumption
        4. Consume filtered historical data
        5. Verify only matching records retrieved
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-02-02 mark wang created

        """
        tdSql.prepare()
        self.insertData()
        self.consume2()
        self.consume3()
        self.insertData()
        self.consume1()

event = threading.Event()
