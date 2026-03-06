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

from new_test_framework.utils import tdLog, tdSql, tdDnodes
import os
import shutil
import time
from datetime import datetime
import shlex
import subprocess


class TestMetaForceRepair:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def _get_taosd_bin(self):
        candidates = []
        if tdDnodes.binPath:
            candidates.append(tdDnodes.binPath)

        taosd_bin = os.getenv("TAOSD_BIN")
        if taosd_bin:
            candidates.append(taosd_bin)

        taos_bin_path = os.getenv("TAOS_BIN_PATH")
        if taos_bin_path:
            candidates.append(os.path.join(taos_bin_path, "taosd"))

        for bin_path in candidates:
            if os.path.isfile(bin_path) and os.access(bin_path, os.X_OK):
                tdDnodes.binPath = bin_path
                tdLog.info("taosd found in %s" % bin_path)
                return bin_path

        tdLog.exit(
            "taosd not found! set TAOSD_BIN or TAOS_BIN_PATH when running this case standalone."
        )

    def _run_taosd(self, args):
        bin_path = self._get_taosd_bin()
        cmd = [bin_path] + shlex.split(args)
        tdLog.info("run cmd: %s" % " ".join(cmd))
        env = os.environ.copy()
        asan_options = env.get("ASAN_OPTIONS", "")
        if "detect_leaks=" not in asan_options:
            env["ASAN_OPTIONS"] = (
                "detect_leaks=0" if not asan_options else asan_options + ":detect_leaks=0"
            )
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env=env,
        )
        output = proc.stdout or ""
        tdLog.info("ret=%s output=%s" % (proc.returncode, output[:500].replace("\n", "\\n")))
        return proc.returncode, output

    def _assert_case(self, name, args, expected_code, expected_text):
        tdLog.info("verify meta force repair case=%s" % name)
        code, output = self._run_taosd(args)
        tdSql.checkEqual(code, expected_code)
        tdSql.checkEqual(expected_text in output, True)


    def _get_cfg_dir(self):
        return tdDnodes.dnodes[0].cfgDir

    def _get_primary_data_dir(self):
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            first = data_dir[0]
            return first.split(" ")[0]
        return data_dir

    def _get_vnode_ids(self):
        vnode_root = os.path.join(self._get_primary_data_dir(), "vnode")
        if not os.path.isdir(vnode_root):
            return []

        vnode_ids = []
        for name in os.listdir(vnode_root):
            if not name.startswith("vnode"):
                continue
            suffix = name[5:]
            if suffix.isdigit():
                vnode_ids.append(int(suffix))
        return sorted(vnode_ids)

    def _start_repair_process(self, args):
        bin_path = self._get_taosd_bin()
        cmd = [bin_path, "-c", self._get_cfg_dir()] + shlex.split(args)
        tdLog.info("run repair cmd: %s" % " ".join(cmd))
        env = os.environ.copy()
        asan_options = env.get("ASAN_OPTIONS", "")
        if "detect_leaks=" not in asan_options:
            env["ASAN_OPTIONS"] = (
                "detect_leaks=0" if not asan_options else asan_options + ":detect_leaks=0"
            )
        env.setdefault("LSAN_OPTIONS", "detect_leaks=0")
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env=env,
        )

    def _stop_repair_process(self, proc):
        if proc.poll() is not None:
            output = proc.stdout.read() if proc.stdout else ""
            tdLog.info("repair proc exited early: %s" % output[:500].replace("\n", "\\n"))
            return

        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)

        output = proc.stdout.read() if proc.stdout else ""
        tdLog.info("repair proc output=%s" % output[:500].replace("\n", "\\n"))

    def _wait_for_path(self, path, timeout_sec=20):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if os.path.exists(path):
                return True
            time.sleep(1)
        return False

    def test_meta_force_repair_all_vnodes_without_vnode_id(self):
        """Meta force repair should allow omitted vnode-id.

        1. Verify `--file-type meta --mode force` no longer requires `--vnode-id`.

        Since: v3.4.1.0

        Labels: common,ci
        """
        cases = [
            ("default_backup", "-r --node-type vnode --file-type meta --mode force -V", 0, "version"),
            (
                "custom_backup",
                "-r --node-type vnode --file-type meta --mode force --backup-path /tmp/meta-force-repair -V",
                0,
                "version",
            ),
            (
                "custom_backup_trailing_slash",
                "-r --node-type vnode --file-type meta --mode force --backup-path /tmp/meta-force-repair/ -V",
                0,
                "version",
            ),
        ]

        for name, args, expected_code, expected_text in cases:
            self._assert_case(name, args, expected_code, expected_text)

    def test_meta_force_repair_keeps_non_meta_in_phase1_placeholder(self):
        """Non-meta repair stays on the phase1 placeholder path.

        1. Verify this file only relaxes meta force repair and does not silently enable wal repair.

        Since: v3.4.1.0

        Labels: common,ci
        """
        self._assert_case(
            "wal_still_placeholder",
            "-r --node-type vnode --file-type wal --vnode-id 1 --mode force",
            0,
            "repair execution is not enabled in this phase",
        )


    def test_meta_force_repair_creates_backup_for_real_vnode(self):
        """Meta force repair should create backup files for a real vnode.

        1. Create real vnode data.
        2. Stop taosd and run meta force repair against that vnode.
        3. Verify external backup directory is created and contains meta files.

        Since: v3.4.1.0

        Labels: common,ci
        """
        tdSql.prepare()
        dbname = "meta_repair_real"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, 1)

        vnode_ids = self._get_vnode_ids()
        tdSql.checkEqual(len(vnode_ids) > 0, True)
        vnode_id = vnode_ids[0]

        backup_root = "/tmp/meta-force-repair-e2e"
        date_str = datetime.now().strftime("%Y%m%d")
        expected_backup_dir = os.path.join(
            backup_root, f"taos_backup_{date_str}", f"vnode{vnode_id}", "meta"
        )

        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)

        proc = None
        try:
            tdDnodes.stop(1)
            proc = self._start_repair_process(
                f"-r --node-type vnode --file-type meta --vnode-id {vnode_id} --mode force --backup-path {backup_root} --log-output /dev/null"
            )
            tdSql.checkEqual(self._wait_for_path(expected_backup_dir), True)
            tdSql.checkEqual(os.path.isdir(expected_backup_dir), True)
            tdSql.checkEqual(len(os.listdir(expected_backup_dir)) > 0, True)
        finally:
            if proc is not None:
                self._stop_repair_process(proc)
            tdDnodes.start(1)
            time.sleep(2)
            tdSql.query("select * from information_schema.ins_databases")
            if os.path.exists(backup_root):
                shutil.rmtree(backup_root)
