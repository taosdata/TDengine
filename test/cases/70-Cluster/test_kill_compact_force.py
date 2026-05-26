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
import time
import pytest
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestKillCompactForce:
    """Test KILL COMPACT <id> FORCE with an offline dnode.

    Force-kill a compact while one dnode is stopped.  Verify that the compact
    record is immediately removed from SDB (show compacts returns empty), that
    the stopped dnode can be restarted without issue, and that data integrity is
    preserved after a normal compact cycle.
    """

    DB = "test_kcf"
    STB = "meters"
    REPLICA = 3
    VGROUPS = 16
    SUBTABLE_NUM = 100
    ROWS_PER_SUBTABLE = 1000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _create_db_and_data(self):
        """Create database, stable, and insert initial data."""
        tdLog.info("====== create database and insert data")
        tdSql.execute(
            f"create database {self.DB} "
            f"replica {self.REPLICA} vgroups {self.VGROUPS} "
            f"duration 1d keep 3650d"
        )
        clusterComCheck.checkDbReady(self.DB)

        tdSql.execute(
            f"create stable {self.DB}.{self.STB} "
            f"(ts timestamp, val double, str binary(64)) "
            f"tags (tid int)"
        )

        # Insert ROWS_PER_SUBTABLE rows into SUBTABLE_NUM child tables
        base_ts = 1600000000000  # 2020-09-13
        step_ms = 60_000

        tdLog.info(f"====== inserting {self.SUBTABLE_NUM} subtables × {self.ROWS_PER_SUBTABLE} rows")
        for i in range(self.SUBTABLE_NUM):
            tdSql.execute(
                f"create table {self.DB}.t{i} using {self.DB}.{self.STB} tags({i})"
            )
            values = ", ".join(
                f"({base_ts + j * step_ms}, {float(i * 1000 + j)}, 'row_{j}')"
                for j in range(self.ROWS_PER_SUBTABLE)
            )
            tdSql.execute(f"insert into {self.DB}.t{i} values {values}")

        # Flush so data lands on disk — makes compact meaningful
        tdSql.execute(f"flush database {self.DB}")
        tdLog.info("====== initial data inserted and flushed")

    def _query_total_rows(self):
        """Return total row count for the stable."""
        tdSql.query(f"select count(*) from {self.DB}.{self.STB}")
        return tdSql.getData(0, 0)

    def _wait_compacts_empty(self, timeout=300, interval=2):
        """Wait until 'show compacts' returns 0 rows."""
        for _ in range(timeout // interval):
            rows = tdSql.query("show compacts")
            if rows == 0:
                return True
            time.sleep(interval)
        return False

    def _get_compact_id(self):
        """Return the compact_id of the first running compact, or None."""
        rows = tdSql.query("show compacts")
        if rows == 0:
            return None
        return tdSql.getData(0, 0)

    # ------------------------------------------------------------------
    # test
    # ------------------------------------------------------------------

    @pytest.mark.cluster
    def test_kill_compact_force(self):
        """KILL COMPACT <id> FORCE removes SDB records even when a dnode is offline

        1.  Start 3-dnode cluster; create DB with replica 3, 16 vgroups.
        2.  Insert data and flush to disk.
        3.  Stop dnode 3.
        4.  Start compact on the database.
        5.  KILL COMPACT <id> FORCE.
        6.  Verify show compacts returns 0 rows (SDB records deleted).
        7.  Restart dnode 3; wait until all 3 dnodes are online again.
        8.  BALANCE VGROUP LEADER.
        9.  Query data — verify row count matches what was inserted.
        10. Run a normal compact and wait for it to finish.
        11. Insert more data, run compact again, wait for finish.
        12. Final query — verify total row count is correct.

        Since: v3.3.6.0

        Labels: cluster,ci

        Jira: None

        History:
            - 2026-05-26 GitHub Copilot Created

        """
        # ----------------------------------------------------------------
        # step 0: cluster precondition
        # ----------------------------------------------------------------
        clusterComCheck.checkDnodes(3)

        # ----------------------------------------------------------------
        # step 1: create database and insert initial data
        # ----------------------------------------------------------------
        self._create_db_and_data()
        expected_rows = self.SUBTABLE_NUM * self.ROWS_PER_SUBTABLE
        actual = self._query_total_rows()
        assert actual == expected_rows, (
            f"initial row count mismatch: expected {expected_rows}, got {actual}"
        )
        tdLog.info(f"====== initial row count verified: {actual}")

        # ----------------------------------------------------------------
        # step 2: stop dnode 3
        # ----------------------------------------------------------------
        tdLog.info("====== step2: stop dnode 3")
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)

        # ----------------------------------------------------------------
        # step 3: start compact (with one dnode offline)
        # ----------------------------------------------------------------
        tdLog.info("====== step3: start compact database")
        tdSql.execute(f"compact database {self.DB}")
        # Wait briefly to let the compact record appear in SDB
        for _ in range(30):
            rows = tdSql.query("show compacts")
            if rows > 0:
                break
            time.sleep(1)
        rows = tdSql.query("show compacts")
        assert rows > 0, "compact did not appear in show compacts after 30 s"
        compact_id = self._get_compact_id()
        tdLog.info(f"====== compact started with id={compact_id}")

        # ----------------------------------------------------------------
        # step 4: KILL COMPACT FORCE
        # ----------------------------------------------------------------
        tdLog.info(f"====== step4: kill compact {compact_id} force")
        tdSql.execute(f"kill compact {compact_id} force")

        # ----------------------------------------------------------------
        # step 5: verify show compacts is empty
        # ----------------------------------------------------------------
        tdLog.info("====== step5: wait for show compacts to become empty")
        ok = self._wait_compacts_empty(timeout=60, interval=1)
        assert ok, "show compacts is still non-empty after KILL COMPACT FORCE"
        tdLog.info("====== show compacts is empty — force-kill succeeded")

        # ----------------------------------------------------------------
        # step 6: restart dnode 3 and wait for all dnodes online
        # ----------------------------------------------------------------
        tdLog.info("====== step6: restart dnode 3")
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkDbReady(self.DB)

        # Update WAL retention period to 24 hours.
        tdLog.info("update wal_retention_period to 86400s (24h)")
        tdSql.execute(f"alter database {self.DB} wal_retention_period 86400")
        time.sleep(2)

        # ----------------------------------------------------------------
        # step 7: balance vgroup leader
        # ----------------------------------------------------------------
        tdLog.info("====== step7: balance vgroup leader")
        tdSql.execute("balance vgroup leader")
        time.sleep(3)

        # ----------------------------------------------------------------
        # step 8: query data — verify no data loss
        # ----------------------------------------------------------------
        tdLog.info("====== step8: verify no data loss after force-kill")
        actual = self._query_total_rows()
        assert actual == expected_rows, (
            f"data loss detected: expected {expected_rows} rows, got {actual}"
        )
        tdLog.info(f"====== data integrity OK: {actual} rows")

        # ----------------------------------------------------------------
        # step 9: run a normal compact and wait for completion
        # ----------------------------------------------------------------
        tdLog.info("====== step9: run normal compact and wait")
        tdSql.execute(f"compact database {self.DB}")
        ok = self._wait_compacts_empty(timeout=600, interval=2)
        assert ok, "normal compact did not finish within 600 s"
        tdLog.info("====== normal compact finished")

        # ----------------------------------------------------------------
        # step 10: insert more data, compact again
        # ----------------------------------------------------------------
        tdLog.info("====== step10: insert more data")
        extra_rows = self.SUBTABLE_NUM * 100
        base_ts2 = 1700000000000  # different timestamp range
        step_ms = 60_000
        for i in range(self.SUBTABLE_NUM):
            values = ", ".join(
                f"({base_ts2 + j * step_ms}, {float(i * 2000 + j)}, 'extra_{j}')"
                for j in range(100)
            )
            tdSql.execute(f"insert into {self.DB}.t{i} values {values}")

        tdSql.execute(f"flush database {self.DB}")
        expected_rows += extra_rows
        tdLog.info(f"====== extra data inserted; expected total: {expected_rows}")

        tdLog.info("====== compact after extra insert")
        tdSql.execute(f"compact database {self.DB}")
        ok = self._wait_compacts_empty(timeout=600, interval=2)
        assert ok, "second compact did not finish within 600 s"
        tdLog.info("====== second compact finished")

        # ----------------------------------------------------------------
        # step 11: final data integrity check
        # ----------------------------------------------------------------
        tdLog.info("====== step11: final data integrity check")
        actual = self._query_total_rows()
        assert actual == expected_rows, (
            f"final data mismatch: expected {expected_rows} rows, got {actual}"
        )
        tdLog.info(f"====== all checks passed — total rows: {actual}")
