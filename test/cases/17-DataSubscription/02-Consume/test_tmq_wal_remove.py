# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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
import shutil
import subprocess
from pathlib import Path

from new_test_framework.utils import tdLog, tdSql, cluster, etool
from taos.tmq import *
from taos import *


class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def prepareData(self):
        for replica in (1, 2, 3):
            database = f"db_repl_{replica}"
            tdLog.info(f"create database {database} and insert data")
            subprocess.run(
                [etool.benchMarkFile(), "-y", "-a", str(replica), "-n", "100",
                 "-t", "100", "-v", "1", "-d", database],
                check=True,
            )

    def insertData(self):
        tdLog.info("insert one record into db_repl_*.d0")
        tdSql.execute("insert into db_repl_1.d0 values(now(),6.8358979,250,148.5000000);")
        tdSql.execute("insert into db_repl_2.d0 values(now(),6.8358979,250,148.5000000);")
        tdSql.execute("insert into db_repl_3.d0 values(now(),6.8358979,250,148.5000000);")

    def flushDatabase(self):
        tdLog.info("flush database db_repl_1")
        tdSql.execute("flush database db_repl_1")

        tdLog.info("flush database db_repl_2")
        tdSql.execute("flush database db_repl_2")

        tdLog.info("flush database db_repl_3")
        tdSql.execute("flush database db_repl_3")

    def checkDatacount(self,expCount):
        tdLog.info("select data count from db_repl_1")
        tdSql.query("select count(*) from db_repl_1.meters")
        actCount = tdSql.getData(0, 0)
        assert actCount == expCount, f"db_repl_1.meters count is {actCount}, expect {expCount}"

        tdLog.info("select data count from db_repl_2")
        tdSql.query("select count(*) from db_repl_2.meters")
        actCount = tdSql.getData(0, 0)
        assert actCount == expCount, f"db_repl_2.meters count is {actCount}, expect {expCount}"


        tdLog.info("select data count from db_repl_3")
        tdSql.query("select count(*) from db_repl_3.meters")
        actCount = tdSql.getData(0, 0)
        assert actCount == expCount, f"db_repl_3.meters count is {actCount}, expect {expCount}"


    def collect_rm_wal_cmds(self):
        self.wal_paths = []
        for node_index in (1, 2, 3):
            node = cluster.dnodes[node_index - 1]
            assert len(node.dataDir) == 1, "This scenario requires one data directory per node"
            data_dir = Path(node.dataDir[0])
            tdSql.query(f"show vnodes on dnode {node_index}")
            for row in tdSql.queryResult:
                wal_path = data_dir / "vnode" / f"vnode{row[1]}" / "wal"
                assert wal_path.is_dir(), f"WAL directory not found: {wal_path}"
                self.wal_paths.append(wal_path)
        assert self.wal_paths, "No WAL directories selected for removal"

    def execute_rm_wal_cmds(self):
        for wal_path in self.wal_paths:
            tdLog.info(f"Remove WAL contents from {wal_path}")
            for entry in wal_path.iterdir():
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
            assert not any(wal_path.iterdir()), f"WAL removal incomplete: {wal_path}"

    def test_tmq_walRemove(self):
        """Advanced: wal removal

        1. Configure WAL retention period
        2. Insert data generating WAL
        3. Trigger WAL cleanup
        4. Verify old WAL removed
        5. Check space reclaimed
        6. Ensure consumption not affected

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_walRemoveLog.py

        """
        print("======== run remove wal test ========")
        self.prepareData()
        self.flushDatabase()

        self.collect_rm_wal_cmds()
        tdSql.execute(f'create topic data_repl_1 as select ts,current from db_repl_1.meters')
        tdSql.execute(f'create topic data_repl_2 as select ts,current from db_repl_2.meters')
        tdSql.execute(f'create topic data_repl_3 as select ts,current from db_repl_3.meters')

        tdDnodes=cluster.dnodes
        tdDnodes[0].stoptaosd()
        tdDnodes[1].stoptaosd()
        tdDnodes[2].stoptaosd()


        time.sleep(10)

        self.execute_rm_wal_cmds()

        tdDnodes[0].starttaosd()
        tdDnodes[1].starttaosd()
        tdDnodes[2].starttaosd()

        self.checkDatacount(10000)
        self.insertData()
        self.checkDatacount(10001)

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["data_repl_1", "data_repl_2", "data_repl_3"])
        except TmqError as error:
            consumer.close()
            tdLog.exit(f"subscribe error: {error}")

        cnt = 0
        deadline = time.monotonic() + 60
        try:
            while time.monotonic() < deadline and cnt < 3:
                res = consumer.poll(1)
                if res is None:
                    continue
                if res.error():
                    tdLog.exit(f"consume error: {res.error()}")
                for block in res.value() or []:
                    rows = block.fetchall()
                    cnt += len(rows)
                    tdLog.info(f"Consumed {len(rows)} rows; total={cnt}")
            assert cnt == 3, f"Expected only 3 rows after WAL removal, consumed {cnt}"
        finally:
            consumer.close()
