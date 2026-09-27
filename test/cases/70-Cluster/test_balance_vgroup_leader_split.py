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
import pytest
from new_test_framework.utils import tdLog, tdSql, clusterComCheck


class TestBalanceVgroupLeaderSplit:
    """Test the split of BALANCE VGROUP LEADER into two modes:

    - Non-forced mode (default, no FORCE keyword): the current leader steps
      down and all replicas compete equally.  The election is random, but the
      vgroup must never end up leaderless because a lagging replica simply
      loses the vote instead of being forced to win.
    - Forced mode (FORCE keyword): biases the election so a specific replica
      becomes leader, achieving an even distribution.  This is the historical
      behavior.

    This case verifies that all six command forms (ALL / ON <vgId> / DATABASE,
    each with and without FORCE) execute successfully and that after each
    balance every vgroup still has exactly one leader.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _count_leaders(self, db: str, vgroup_rows: int) -> int:
        """Return how many vgroups for *db* have a leader on any replica slot.

        For a replica-3 database, `show <db>.vgroups` places the per-replica
        status in columns 4, 7 and 10 (v*_status).  A healthy vgroup has
        exactly one of them equal to "leader".
        """
        tdSql.query(f"show {db}.vgroups", show=True)
        leaders = 0
        for i in range(vgroup_rows):
            if (
                tdSql.checkDataV2(i, 4, "leader", False)
                or tdSql.checkDataV2(i, 7, "leader", False)
                or tdSql.checkDataV2(i, 10, "leader", False)
            ):
                leaders += 1
        return leaders

    def _run_and_check(self, db: str, sql: str, vgroup_rows: int, step: str):
        """Execute a balance command, wait for transactions to settle, and
        assert that every vgroup still has a leader (no deadlock)."""
        tdLog.info(f"========== {step}: {sql}")
        tdSql.execute(sql)
        # Wait for the balance transactions to complete before inspecting state.
        clusterComCheck.checkTransactions()
        clusterComCheck.checkDbReady(db)
        leaders = self._count_leaders(db, vgroup_rows)
        if leaders != vgroup_rows:
            tdLog.exit(f"{step} failed: expected {vgroup_rows} leaders, got {leaders}")
        tdLog.info(f"========== {step} passed: all {vgroup_rows} vgroups have a leader")

    @pytest.mark.cluster
    def test_balance_vgroup_leader_split(self):
        """BALANCE VGROUP LEADER non-forced (random) vs forced modes

        1. Create 3-dnode cluster
        2. Create database (replica 3, vgroups 2)
        3. Non-forced mode (no FORCE):
           - balance vgroup leader                  (ALL)
           - balance vgroup leader on <vgId>        (single vgroup)
           - balance vgroup leader database <db>    (per-database)
        4. Forced mode (FORCE keyword):
           - balance vgroup leader force
           - balance vgroup leader on <vgId> force
           - balance vgroup leader database <db> force
        5. After each command, verify no deadlock: every vgroup has a leader

        Since: v3.3.7.0

        Labels: ci,cluster,integration,functional

        Jira: None

        History:
            - 2026-07-21 GitHub Copilot Created

        """
        db = "test_split"
        vgroups = 2

        clusterComCheck.checkDnodes(3)

        tdLog.info("========== create database")
        tdSql.execute(f"create database {db} replica 3 vgroups {vgroups}")
        clusterComCheck.checkDbReady(db)

        # Grab a concrete vgroup id for the "on <vgId>" variants.
        tdSql.query(f"show {db}.vgroups", show=True)
        vgroup_id = tdSql.getData(0, 0)
        tdLog.info(f"picked vgroup_id={vgroup_id} for on-<vgId> variants")

        # ------------------------------------------------------------------
        # Non-forced mode (default, random) -- must never deadlock
        # ------------------------------------------------------------------
        self._run_and_check(db, "balance vgroup leader", vgroups, "step1-nonforce-all")
        self._run_and_check(
            db, f"balance vgroup leader on {vgroup_id}", vgroups, "step2-nonforce-on"
        )
        self._run_and_check(
            db, f"balance vgroup leader database {db}", vgroups, "step3-nonforce-db"
        )

        # ------------------------------------------------------------------
        # Forced mode -- historical behavior, still expected to succeed here
        # because all replicas are in sync in this test setup
        # ------------------------------------------------------------------
        self._run_and_check(
            db, "balance vgroup leader force", vgroups, "step4-force-all"
        )
        self._run_and_check(
            db,
            f"balance vgroup leader on {vgroup_id} force",
            vgroups,
            "step5-force-on",
        )
        self._run_and_check(
            db,
            f"balance vgroup leader database {db} force",
            vgroups,
            "step6-force-db",
        )

        tdLog.info("========== all balance vgroup leader split cases passed")
