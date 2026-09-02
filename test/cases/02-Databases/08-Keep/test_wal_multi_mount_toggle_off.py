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

import glob
import os
import shutil
import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes

MOUNTS = [
    "/mnt/wal_mm_off1",
    "/mnt/wal_mm_off2",
    "/mnt/wal_mm_off3",
]


def _wal_log_files(mount, vg_id):
    return glob.glob(os.path.join(mount, "vnode", f"vnode{vg_id}", "wal", "*.log"))


def _get_vg_id(db):
    tdSql.query(f"select vgroup_id from information_schema.ins_vgroups where db_name='{db}'")
    return int(tdSql.queryResult[0][0])


class TestWalMultiMountToggleOff:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_deploy_enabled(self):
        # walMultiMountEnable=1 (also the default) at boot, so the vnode's WAL starts
        # out spreading segments across all configured level-0 mount points.
        cfg = {
            f"{MOUNTS[0]} 0 1 0": "dataDir",
            f"{MOUNTS[1]} 0 0 0": "dataDir",
            f"{MOUNTS[2]} 0 0 0": "dataDir",
            "walMultiMountEnable": "1",
        }
        for m in MOUNTS:
            tdSql.createDir(m)

        tdDnodes.stop(1)
        time.sleep(3)
        tdDnodes.deploy(1, cfg)
        tdDnodes.start(1)

    def do_check_switch_value(self, expected):
        tdSql.query("show dnode 1 variables like 'walMultiMountEnable'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == expected, f"expected walMultiMountEnable={expected}, got {value}"

    def do_create_db_and_write(self, db, count):
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 wal_level 1 wal_segment_size 1 wal_roll_period 1")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table t1 (ts timestamp, v int)")
        for i in range(count):
            tdSql.execute(f"insert into t1 values ({1700000000000 + i}, {i})")

    def do_write_more(self, db, start_ts, count):
        tdSql.execute(f"use {db}")
        for i in range(count):
            tdSql.execute(f"insert into t1 values ({start_ts + i}, {i})")

    def do_cleanup(self, db):
        tdSql.execute(f"drop database if exists {db}")
        tdDnodes.stop(1)
        time.sleep(3)
        for m in MOUNTS:
            shutil.rmtree(m, ignore_errors=True)

    def test_wal_multi_mount_toggle_off(self):
        """WAL keeps working correctly after walMultiMountEnable is turned off mid-run

        1. Deploy 0-level multi-level storage (3 mount points) with walMultiMountEnable=1 (default)
        2. Create a database and write data -- confirm WAL segments spread across >=2 mount points
        3. ALTER ALL DNODES 'walMultiMountEnable' '0', confirm the new value via SHOW DNODE VARIABLES
        4. Write more data -- confirm no *new* WAL segments land on the secondary mounts, while
           writes/queries keep succeeding
        5. Restart the dnode and confirm every row -- including rows in segments that were placed
           on secondary mounts before the toggle -- is still present and further writes still work

        Since: v3.4.3.0

        Labels: common,ci,storage,wal

        Jira: None

        History:
            - 2026-08-17 Codex Created

        """
        self.do_deploy_enabled()
        self.do_check_switch_value("1")

        db = "wal_mm_toggle_off_db"
        self.do_create_db_and_write(db, 300)

        vg_id = _get_vg_id(db)
        mounts_with_files_before = [m for m in MOUNTS if len(_wal_log_files(m, vg_id)) > 0]
        assert len(mounts_with_files_before) >= 2, (
            f"expected WAL segments spread across >=2 mounts before disabling, got {mounts_with_files_before}"
        )

        tdSql.execute("ALTER ALL DNODES 'walMultiMountEnable' '0'")
        self.do_check_switch_value("0")

        secondary_count_before_more_writes = sum(len(_wal_log_files(m, vg_id)) for m in MOUNTS[1:])

        self.do_write_more(db, 1700100000000, 300)

        secondary_count_after = sum(len(_wal_log_files(m, vg_id)) for m in MOUNTS[1:])
        assert secondary_count_after == secondary_count_before_more_writes, (
            "no new WAL segments should appear on secondary mounts once walMultiMountEnable=0"
        )

        tdSql.query(f"select count(*) from {db}.t1")
        tdSql.checkData(0, 0, 600)

        # Restart: WAL restore must still find every historical segment, including the
        # ones placed on secondary mounts before the toggle (meta stays centralized on
        # the primary mount and records each segment's disk, per the DS design).
        tdDnodes.stop(1)
        time.sleep(3)
        tdDnodes.start(1)

        tdSql.query(f"select count(*) from {db}.t1")
        tdSql.checkData(0, 0, 600)

        tdSql.execute(f"use {db}")
        tdSql.execute("insert into t1 values (1800000000000, 999)")
        tdSql.query(f"select count(*) from {db}.t1")
        tdSql.checkData(0, 0, 601)

        self.do_cleanup(db)
