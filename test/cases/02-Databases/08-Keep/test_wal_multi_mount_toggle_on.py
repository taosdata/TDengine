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
    "/mnt/wal_mm_on1",
    "/mnt/wal_mm_on2",
    "/mnt/wal_mm_on3",
]


def _wal_log_files(mount, vg_id):
    return glob.glob(os.path.join(mount, "vnode", f"vnode{vg_id}", "wal", "*.log"))


def _get_vg_id(db):
    tdSql.query(f"select vgroup_id from information_schema.ins_vgroups where db_name='{db}'")
    return int(tdSql.queryResult[0][0])


class TestWalMultiMountToggleOn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_deploy_disabled(self):
        # walMultiMountEnable=0 at boot: equivalent to the "unbound/disabled" branch --
        # WAL must stay pinned to the primary mount point until the switch flips on.
        cfg = {
            f"{MOUNTS[0]} 0 1 0": "dataDir",
            f"{MOUNTS[1]} 0 0 0": "dataDir",
            f"{MOUNTS[2]} 0 0 0": "dataDir",
            "walMultiMountEnable": "0",
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

    def test_wal_multi_mount_toggle_on(self):
        """WAL starts pinned to the primary mount, spreads across mounts once walMultiMountEnable is turned on

        1. Deploy 0-level multi-level storage (3 mount points) with walMultiMountEnable=0 at boot
        2. Create a database and write data -- confirm every WAL segment lands only on the primary mount
        3. ALTER ALL DNODES 'walMultiMountEnable' '1', confirm the new value via SHOW DNODE VARIABLES
        4. Write more data -- confirm new WAL segments start appearing on the secondary mounts,
           while all previously written rows remain intact and queryable

        Since: v3.4.3.0

        Labels: common,ci,storage,wal

        Jira: None

        History:
            - 2026-08-17 Codex Created

        """
        self.do_deploy_disabled()
        self.do_check_switch_value("0")

        db = "wal_mm_toggle_on_db"
        self.do_create_db_and_write(db, 300)

        vg_id = _get_vg_id(db)
        primary_files = _wal_log_files(MOUNTS[0], vg_id)
        secondary_files = _wal_log_files(MOUNTS[1], vg_id) + _wal_log_files(MOUNTS[2], vg_id)
        assert len(primary_files) > 0, "expected WAL segments on the primary mount while the switch is off"
        assert len(secondary_files) == 0, (
            f"expected no WAL segments on secondary mounts while walMultiMountEnable=0, got {secondary_files}"
        )

        tdSql.execute("ALTER ALL DNODES 'walMultiMountEnable' '1'")
        self.do_check_switch_value("1")

        self.do_write_more(db, 1700100000000, 300)

        secondary_files_after = _wal_log_files(MOUNTS[1], vg_id) + _wal_log_files(MOUNTS[2], vg_id)
        assert len(secondary_files_after) > 0, (
            "expected new WAL segments to appear on secondary mounts after enabling walMultiMountEnable"
        )

        tdSql.query(f"select count(*) from {db}.t1")
        tdSql.checkData(0, 0, 600)

        self.do_cleanup(db)
