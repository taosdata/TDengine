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
import sys
import time

from new_test_framework.utils import tdLog, tdSql

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from redistribute_concurrent_common import RedistributeConcurrentBase


class TestRedistributeWithVtableQuery(RedistributeConcurrentBase):
    """Verify virtual table query works while redistribute is in-flight."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_vtables(self, dbname):
        """Create origin normal tables and virtual tables over them.

        A virtual table stitches columns from several origin tables together.
        Querying it forces the query engine to read from multiple vnodes,
        which is exactly what we want to keep exercising while those vnodes are
        being redistributed.
        """
        tdSql.execute(f"use {dbname}")

        # Origin normal tables, each carrying a couple of columns.
        orgCount = 6
        rows = 50
        startTs = 1640966400000  # 2022-01-01 00:00:00.000
        for i in range(orgCount):
            tdSql.execute(
                f"create table {dbname}.vorg_{i} "
                f"(ts timestamp, ival int, fval float)"
            )
            values = []
            for r in range(rows):
                values.append(f"({startTs + r * 1000}, {i * 100 + r}, {r * 1.0})")
            tdSql.execute(
                f"insert into {dbname}.vorg_{i} values {' '.join(values)}"
            )

        # Virtual super table + virtual child tables referencing the origins.
        tdSql.execute(
            f"create stable {dbname}.vstb "
            f"(ts timestamp, ival int, fval float) tags (gid int) virtual 1"
        )
        vctbNum = 3
        for i in range(vctbNum):
            # Each virtual child table pulls its columns from two origin tables.
            src_i = (2 * i) % orgCount
            src_f = (2 * i + 1) % orgCount
            tdSql.execute(
                f"create vtable {dbname}.vctb_{i} ("
                f"{dbname}.vorg_{src_i}.ival, "
                f"{dbname}.vorg_{src_f}.fval) "
                f"using {dbname}.vstb tags ({i})"
            )
        tdLog.info(f"created {vctbNum} virtual child tables over {orgCount} origins")
        return vctbNum, rows

    def test_redistribute_with_vtable_query(self):
        """Redistribute concurrent with virtual table query

        Verify that querying virtual tables keeps returning correct results
        while a REDISTRIBUTE VGROUP transaction is running in the background.

        1. Deploy a 4-dnode cluster (dnode1 mnode-only), prepare a database
           with data, origin tables and virtual tables.
        2. Start a background thread that keeps redistributing the vgroups.
        3. In parallel, repeatedly query the virtual tables (projection and
           aggregation) and verify the results are stable and correct.
        4. Stop the background thread and verify data integrity.

        Since: v3.3.6.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-07-16 Created

        """
        dbname = "rdst_vtable"
        # Prepare cluster + base database with data spread over the vgroups.
        self.prepare_cluster()
        totalRows = self.prepare_data(dbname, stbname="stb", ctbNum=10,
                                      rowsPerTbl=100, vgroups=2, replica=1)
        vctbNum, vrows = self.prepare_vtables(dbname)

        # Start churning the vgroups in the background.
        thread, stop_event, errors = self.start_redistribute(dbname)

        try:
            tdSql.execute(f"use {dbname}")

            # Query the virtual tables many times; every query must succeed and
            # return the same, correct row counts regardless of where the
            # underlying vnodes currently live.
            rounds = 15
            for rnd in range(rounds):
                for i in range(vctbNum):
                    # Projection query over a virtual child table.
                    tdSql.query(f"select * from {dbname}.vctb_{i}")
                    tdSql.checkRows(vrows)

                    # Aggregation over a virtual child table.
                    tdSql.query(f"select count(*) from {dbname}.vctb_{i}")
                    tdSql.checkData(0, 0, vrows)

                # Aggregation across the whole virtual super table.
                tdSql.query(f"select count(*) from {dbname}.vstb")
                tdSql.checkData(0, 0, vctbNum * vrows)

                tdLog.info(f"vtable query round {rnd + 1}/{rounds} ok")
                time.sleep(0.2)
            tdLog.info("all virtual table queries succeeded during redistribute")
        finally:
            # Always stop the background thread, even on assertion failure.
            self.stop_redistribute(thread, stop_event, errors)

        # After redistribute finished, the originally inserted data must remain
        # intact.
        tdSql.query(f"select count(*) from {dbname}.stb")
        tdSql.checkData(0, 0, totalRows)
        tdLog.info(f"data integrity verified: {totalRows} rows preserved")
