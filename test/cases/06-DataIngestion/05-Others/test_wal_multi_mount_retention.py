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

import glob
import os
import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes
from new_test_framework.utils.pathFinding import find_proj_root

# Three level-0 mount points on one dnode: taos00 is primary, the other two are not.
# Deployed through updatecfgDict (same convention as cases/25-Privileges/test_priv_rbac.py)
# so the paths live under the run's own sim/ tree instead of a hard-coded /mnt.
_HOST_PATH = os.path.join(find_proj_root(), "sim", "dnode1", "multi")
MOUNTS = [
    os.path.join(_HOST_PATH, "taos00"),
    os.path.join(_HOST_PATH, "taos01"),
    os.path.join(_HOST_PATH, "taos02"),
]
PRIMARY = MOUNTS[0]


def _wal_files(mount, vg_id, suffix):
    return glob.glob(os.path.join(mount, "vnode", f"vnode{vg_id}", "wal", f"*.{suffix}"))


def _layout(vg_id, suffix="log"):
    """Map of mount -> sorted segment basenames currently present on that mount."""
    return {m: sorted(os.path.basename(f) for f in _wal_files(m, vg_id, suffix)) for m in MOUNTS}


def _non_primary(layout):
    """Flatten a layout down to the basenames sitting anywhere other than the primary."""
    return {f for m, files in layout.items() if m != PRIMARY for f in files}


def _count(vg_id, suffix):
    """Total number of files with the given suffix across all three mounts."""
    return sum(len(_wal_files(m, vg_id, suffix)) for m in MOUNTS)


# walTxnFilesTrim keeps (live .log count + WAL_TXN_TRIM_EXTRA_FILES) .txn files as a
# deliberate grace margin for a lagging CDC consumer, and trims the rest. Mirrors
# WAL_TXN_TRIM_EXTRA_FILES in source/libs/wal/inc/walInt.h.
TXN_TRIM_EXTRA_FILES = 6


class TestWalMultiMountRetention:
    """WAL retention must reclaim segments that live on non-primary level-0 mount points.

    Regression coverage for the field incident where WAL kept growing without bound on a
    multi-level-storage deployment. Each segment records the tfs disk it was placed on,
    but the retention and rollback paths dropped the segment's bookkeeping entry before
    building the file name to unlink. The name then resolved back to the primary mount,
    so every unlink for a segment on a non-primary mount silently hit a path that does
    not exist -- ENOENT is tolerated there, so "wal log files recycled" was still logged
    while the files stayed on disk forever.
    """

    updatecfgDict = {
        "dataDir": [
            f"{MOUNTS[0]} 0 1",
            f"{MOUNTS[1]} 0 0",
            f"{MOUNTS[2]} 0 0",
        ],
    }

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # -- helpers -----------------------------------------------------------------

    def get_vg_id(self, db):
        tdSql.query(f"select vgroup_id from information_schema.ins_vgroups where db_name='{db}'")
        return int(tdSql.queryResult[0][0])

    def create_db(self, db):
        tdSql.execute(f"drop database if exists {db}")
        # wal_segment_size 1 rolls a new segment on almost every insert, so a modest
        # number of writes spreads segments across all three mounts quickly.
        #
        # wal_retention_period -1 disables recycling for now. Creating the database with
        # 0 instead makes the test vacuous: retention then runs from the first roll and
        # reclaims everything but the live write segment, so the multi-mount spread this
        # test needs to observe is already gone by the time it looks. Recycling is turned
        # on explicitly by enable_retention() once the spread has been captured.
        tdSql.execute(
            f"create database {db} vgroups 1 wal_level 1 "
            f"wal_segment_size 1 wal_roll_period 1 wal_retention_period -1"
        )
        tdSql.execute(f"use {db}")
        tdSql.execute("create table t1 (ts timestamp, v int)")

    def write(self, db, count, ts_base):
        tdSql.execute(f"use {db}")
        for i in range(count):
            tdSql.execute(f"insert into t1 values ({ts_base + i}, {i})")

    def enable_retention(self, db):
        """Switch the database over to immediate WAL recycling and drive one cycle."""
        tdSql.execute(f"alter database {db} wal_retention_period 0")
        tdSql.execute(f"flush database {db}")
        tdSql.execute(f"trim database {db} wal")

    def capture_non_primary(self, vg_id, max_retry=60):
        """Return the segment basenames currently sitting on non-primary mounts.

        Placement is picked by tfs at roll time, so this is asserted as a precondition
        rather than assumed -- without segments on a non-primary mount every deletion
        assertion below would pass without testing anything. Only meaningful while
        recycling is still disabled.
        """
        for _ in range(max_retry):
            names = _non_primary(_layout(vg_id))
            if names:
                tdLog.info(f"segments on non-primary mounts: {sorted(names)}")
                return names
            time.sleep(1)
        assert False, (
            f"no WAL segment landed on a non-primary mount point; multi-mount placement "
            f"did not engage, so this test would be vacuous. layout: {_layout(vg_id)}"
        )

    def wait_until_gone(self, vg_id, expected_gone, db):
        """Poll until none of expected_gone remain on any non-primary mount."""
        max_retry = 240 if os.environ.get("CI_ASAN_BUILD") == "1" else 120
        for attempt in range(max_retry):
            still_there = _non_primary(_layout(vg_id)) & expected_gone
            if not still_there:
                return True
            if attempt > 0 and attempt % 15 == 0:
                tdLog.info(f"still pending deletion: {sorted(still_there)}")
                tdSql.execute(f"flush database {db}")
                tdSql.execute(f"trim database {db} wal")
            time.sleep(1)
        return False

    # -- scenarios ---------------------------------------------------------------

    def check_retention_reclaims_non_primary_mounts(self, db):
        """Retention must delete segments on non-primary mounts, not just the primary."""
        vg_id = self.get_vg_id(db)

        # Recycling is still off, so the full spread is observable here.
        retired = self.capture_non_primary(vg_id)
        before = _layout(vg_id)
        tdLog.info(f"layout before retention: {before}")

        self.enable_retention(db)
        ok = self.wait_until_gone(vg_id, retired, db)
        after = _layout(vg_id)
        assert ok, (
            f"WAL segments on non-primary mount points were never reclaimed by retention.\n"
            f"  before: {before}\n"
            f"  after : {after}\n"
            f"  leaked: {sorted(_non_primary(after) & retired)}"
        )

        # .idx files must be reclaimed alongside their .log siblings.
        retired_stems = {os.path.splitext(f)[0] for f in retired}
        leftover_idx = {os.path.splitext(f)[0] for f in _non_primary(_layout(vg_id, "idx"))} & retired_stems
        assert not leftover_idx, f"orphaned .idx files left on non-primary mounts: {sorted(leftover_idx)}"

        tdLog.info(f"layout after retention: {after}")

    def check_txn_files_reclaimed(self, db):
        """Retention must also reclaim the .txn files it orphans.

        A vnode WAL always has .txn files enabled (vnodeOpen sets enableTxnFile), and
        walEndSnapshot unlinks only .log and .idx -- the .txn files of the segments it
        recycles are left for walTxnFilesTrim. Field state showed dozens of 0-byte .txn
        files accumulating across data3/data4/data5 while the segments beside them were
        recycled normally: the trim's age guard compared each orphan's mtime against a
        cutoff derived from the oldest *surviving* segment, which after retention is the
        newest thing on disk, so with wal_retention_period 0 the guard could never be
        satisfied and the leak was unbounded.

        Must run after retention has been enabled for this database.
        """
        vg_id = self.get_vg_id(db)

        max_retry = 240 if os.environ.get("CI_ASAN_BUILD") == "1" else 120
        bound = None
        for attempt in range(max_retry):
            log_count = _count(vg_id, "log")
            txn_count = _count(vg_id, "txn")
            bound = log_count + TXN_TRIM_EXTRA_FILES
            if log_count > 0 and txn_count <= bound:
                tdLog.info(f".txn files: {txn_count}, live .log files: {log_count} (bound {bound})")
                return
            if attempt > 0 and attempt % 15 == 0:
                tdLog.info(f".txn files: {txn_count}, live .log files: {log_count} (bound {bound})")
                tdSql.execute(f"flush database {db}")
                tdSql.execute(f"trim database {db} wal")
            time.sleep(1)

        assert False, (
            f"orphaned .txn files were never reclaimed: {_count(vg_id, 'txn')} .txn files "
            f"remain against {_count(vg_id, 'log')} live .log files (bound {bound}).\n"
            f"  .txn layout: {_layout(vg_id, 'txn')}\n"
            f"  .log layout: {_layout(vg_id, 'log')}"
        )

    def check_retention_after_toggle_off(self, db):
        """Segments written while walMultiMountEnable was ON must still be reclaimed
        after the switch is turned OFF.

        Deletion keys off each segment's recorded placement, never off the current value
        of the switch -- otherwise flipping the knob during an incident would strand
        everything already written to non-primary mounts as permanently unreachable
        garbage.

        Uses its own database: the scenario above leaves recycling enabled, which would
        reclaim this one's segments before the spread could be captured.
        """
        self.create_db(db)
        self.write(db, 300, 1700200000000)
        vg_id = self.get_vg_id(db)

        written_while_on = self.capture_non_primary(vg_id)
        tdLog.info(f"non-primary segments written while the switch was ON: {sorted(written_while_on)}")

        # Flip the switch off, exactly as an operator would mid-incident.
        tdSql.execute("alter all dnodes 'walMultiMountEnable 0'")
        time.sleep(2)

        # Keep writing so retention has a reason to advance past the old segments.
        self.write(db, 150, 1700300000000)
        self.enable_retention(db)
        ok = self.wait_until_gone(vg_id, written_while_on, db)
        after = _layout(vg_id)

        # Restore the default before asserting, so a failure here cannot leak the switch
        # into later cases in the same run.
        tdSql.execute("alter all dnodes 'walMultiMountEnable 1'")
        time.sleep(2)

        assert ok, (
            f"segments written while walMultiMountEnable was ON survived retention after "
            f"the switch was turned OFF -- they are now unreachable garbage.\n"
            f"  written-while-on: {sorted(written_while_on)}\n"
            f"  current layout  : {after}"
        )

    def check_restart_after_retention(self, db):
        """The surviving WAL must still load after a restart.

        This is the half of the fix that lives in walCheckAndRepairMeta: the repair pass
        scans every mount, so anything retention left behind reappears as a hole in the
        version range, and with walDeleteOnCorruption on by default the whole WAL
        directory gets renamed away. If that happened the vnode would come back empty.
        """
        tdSql.execute(f"use {db}")
        tdSql.query("select count(*) from t1")
        count_before = int(tdSql.queryResult[0][0])
        assert count_before > 0, "no rows visible before restart"

        tdDnodes.stop(1)
        time.sleep(3)
        tdDnodes.start(1)
        time.sleep(5)

        tdSql.execute(f"use {db}")
        tdSql.query("select count(*) from t1")
        count_after = int(tdSql.queryResult[0][0])
        assert count_after == count_before, (
            f"row count changed across restart after WAL retention: {count_before} -> {count_after}"
        )
        tdLog.info(f"row count stable across restart: {count_after}")

    # -- entry point ---------------------------------------------------------------

    def test_wal_multi_mount_retention(self):
        """WAL retention reclaims segments on every level-0 mount point

        1. Deploy one dnode with three level-0 mount points (1 primary + 2 non-primary)
        2. Write with a tiny wal_segment_size so WAL segments spread across the mounts
        3. Trigger retention and assert the segments on non-primary mounts are physically
           deleted (both .log and .idx), not just the ones on the primary mount
        4. Assert the .txn files orphaned by that retention are reclaimed too, down to the
           (live .log count + 6) grace margin, instead of accumulating without bound
        5. Turn walMultiMountEnable off and assert segments written while it was on are
           still reclaimed -- deletion follows each segment's recorded placement, not the
           current switch value
        6. Restart the dnode and assert the surviving WAL still loads and the data is intact

        Catalog:
            - Database:WAL

        Since: v3.3.7.0

        Labels: common,ci,storage,wal

        Jira: None

        History:
            - 2026-10-19 Claude Opus 5 created
        """
        db = "wal_mm_retention_db"
        db_toggle = "wal_mm_toggle_db"
        try:
            self.create_db(db)
            self.write(db, 300, 1700000000000)
            self.check_retention_reclaims_non_primary_mounts(db)
            self.check_txn_files_reclaimed(db)

            # Owns its database: the scenario above leaves recycling enabled.
            self.check_retention_after_toggle_off(db_toggle)

            self.check_restart_after_retention(db)
        finally:
            for name in (db, db_toggle):
                try:
                    tdSql.execute(f"drop database if exists {name}")
                except Exception as e:
                    tdLog.info(f"ignoring cleanup failure for {name}: {e}")
