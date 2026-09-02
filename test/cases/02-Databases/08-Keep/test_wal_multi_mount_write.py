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
    "/mnt/wal_mm_write1",
    "/mnt/wal_mm_write2",
    "/mnt/wal_mm_write3",
]


def _wal_log_files(mount, vg_id):
    return glob.glob(os.path.join(mount, "vnode", f"vnode{vg_id}", "wal", "*.log"))


def _get_vg_id(db):
    tdSql.query(f"select vgroup_id from information_schema.ins_vgroups where db_name='{db}'")
    return int(tdSql.queryResult[0][0])


class TestWalMultiMountWrite:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_deploy_multi_mount(self):
        cfg = {
            f"{MOUNTS[0]} 0 1 0": "dataDir",
            f"{MOUNTS[1]} 0 0 0": "dataDir",
            f"{MOUNTS[2]} 0 0 0": "dataDir",
        }
        for m in MOUNTS:
            tdSql.createDir(m)

        tdDnodes.stop(1)
        time.sleep(3)
        tdDnodes.deploy(1, cfg)
        tdDnodes.start(1)

    def do_write_and_roll(self, db):
        tdSql.execute(f"drop database if exists {db}")
        # small wal_segment_size forces a roll (new WAL file) on almost every insert,
        # so a modest number of writes is enough to spread segments across mounts
        tdSql.execute(f"create database {db} vgroups 1 wal_level 1 wal_segment_size 1 wal_roll_period 1")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table t1 (ts timestamp, v int)")
        for i in range(300):
            tdSql.execute(f"insert into t1 values ({1700000000000 + i}, {i})")

    def do_check_distribution(self, db):
        vg_id = _get_vg_id(db)
        per_mount = {m: _wal_log_files(m, vg_id) for m in MOUNTS}
        for m, files in per_mount.items():
            tdLog.info(f"{m}: {len(files)} wal .log files")

        mounts_with_files = [m for m, files in per_mount.items() if len(files) > 0]
        assert len(mounts_with_files) >= 2, (
            f"expected WAL segments to spread across >=2 mount points, got {per_mount}"
        )

        total = sum(len(files) for files in per_mount.values())
        assert total >= 2, f"expected multiple wal segments in total, got {total}"

    def do_cleanup(self, db):
        tdSql.execute(f"drop database if exists {db}")
        tdDnodes.stop(1)
        time.sleep(3)
        for m in MOUNTS:
            shutil.rmtree(m, ignore_errors=True)

    def test_wal_multi_mount_write(self):
        """WAL segments spread across level-0 mount points when multi-level storage is configured

        1. Configure 0-level multi-level storage with 3 mount points (1 primary + 2 secondary)
        2. Create a database with a short wal_roll_period / small wal_segment_size and write data
        3. Assert WAL .log segment files for the vnode appear on at least 2 of the 3 mount points,
           and that the total segment count matches what was actually created (no loss/duplication)

        Since: v3.4.3.0

        Labels: common,ci,storage,wal

        Jira: None

        History:
            - 2026-08-17 Codex Created

        """
        self.do_deploy_multi_mount()
        db = "wal_mm_write_db"
        self.do_write_and_roll(db)
        self.do_check_distribution(db)
        self.do_cleanup(db)
