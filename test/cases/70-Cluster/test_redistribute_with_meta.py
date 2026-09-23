# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
import os
import sys
import time

from new_test_framework.utils import tdLog, tdSql

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from redistribute_concurrent_common import RedistributeConcurrentBase


class TestRedistributeWithMeta(RedistributeConcurrentBase):
    """Verify metadata operations work while redistribute is in-flight."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_redistribute_with_meta(self):
        """Redistribute concurrent with metadata operations

        Verify that creating super tables / child tables and altering the
        schema keep working while a REDISTRIBUTE VGROUP transaction is running
        in the background.

        1. Deploy a 4-dnode cluster (dnode1 mnode-only) and prepare a database
           with data.
        2. Start a background thread that keeps redistributing the vgroups.
        3. In parallel, create super tables, create child tables and alter the
           super table schema; verify every operation succeeds and the
           metadata is visible.
        4. Stop the background thread and verify data integrity.

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-07-16 Created

        """
        dbname = "rdst_meta"
        # Prepare cluster + a database with some data on the vgroups that will
        # be redistributed.
        self.prepare_cluster()
        totalRows = self.prepare_data(dbname, stbname="stb", ctbNum=10,
                                      rowsPerTbl=100, vgroups=2, replica=1)

        # Start churning the vgroups in the background.
        thread, stop_event, errors = self.start_redistribute(dbname)

        try:
            tdSql.execute(f"use {dbname}")

            # --- create super tables while redistribute is running ---
            # Metadata (create/alter table) is an mnode transaction, so it runs
            # concurrently with the vnode redistribute transactions and must
            # still succeed.
            stbCount = 10
            for i in range(stbCount):
                stb = f"meta_stb_{i}"
                tdLog.info(f"create super table {stb} during redistribute")
                # `create stable` is an mnode DDL. The redistribute transaction
                # holds a GLOBAL conflict lock, so this can be transiently
                # rejected with "Conflict transaction not completed" (0x03D3).
                # That is expected product behavior; retry until it succeeds.
                self.execute_with_conflict_retry(
                    tdSql,
                    f"create stable {dbname}.{stb} "
                    f"(ts timestamp, v1 int, v2 double) tags (gid int)",
                )
                # Verify it is immediately visible.
                tdSql.query(
                    f"select stable_name from information_schema.ins_stables "
                    f"where db_name='{dbname}' and stable_name='{stb}'"
                )
                tdSql.checkRows(1)

                # Create a child table under the new super table and insert.
                # (Creating a child table + insert are vnode operations and do
                # not conflict with the redistribute mnode transaction.)
                ctb = f"meta_ctb_{i}"
                tdSql.execute(
                    f"create table {dbname}.{ctb} using {dbname}.{stb} tags({i})"
                )
                tdSql.execute(
                    f"insert into {dbname}.{ctb} values (now, {i}, {i * 1.5})"
                )

                # --- alter the super table schema while redistribute runs ---
                # `alter stable` is also an mnode DDL; retry on conflict too.
                self.execute_with_conflict_retry(
                    tdSql,
                    f"alter stable {dbname}.{stb} add column v3 bigint",
                )
                tdSql.query(
                    f"select col_name from information_schema.ins_columns "
                    f"where db_name='{dbname}' and table_name='{stb}' "
                    f"and col_name='v3'"
                )
                tdSql.checkRows(1)

                time.sleep(0.2)

            # Verify the expected number of user super tables exist:
            # 1 (original stb) + stbCount created above.
            tdSql.query(
                f"select count(*) from information_schema.ins_stables "
                f"where db_name='{dbname}'"
            )
            tdSql.checkData(0, 0, stbCount + 1)
            tdLog.info("all metadata operations succeeded during redistribute")
        finally:
            # Always stop the background thread, even on assertion failure.
            self.stop_redistribute(thread, stop_event, errors)

        # After redistribute finished, the originally inserted data must remain
        # intact (data integrity across vnode moves).
        tdSql.query(f"select count(*) from {dbname}.stb")
        tdSql.checkData(0, 0, totalRows)
        tdLog.info(f"data integrity verified: {totalRows} rows preserved")
