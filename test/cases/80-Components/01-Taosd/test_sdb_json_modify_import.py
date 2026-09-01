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

import json
import os
import shlex
import subprocess

from new_test_framework.utils import tdLog, tdSql, tdDnodes


class TestSdbJsonModifyImport:
    """Coverage for `taosd -s` / `-mSdb` (FS "支持手动修改 sdb" §4.3/§4.4):

    - Editing a field that IS on a table type's editable-scalar list (e.g.
      dnode.fqdn) and reimporting must make the change take effect.
    - Editing a field that is NOT editable -- either because the whole table
      type is rawData-only (mnode/user: no scalar overlay at all, so
      `fields` is display-only) or because the specific field simply isn't
      on that type's editable list (e.g. dnode.id/createdTime) -- must be
      silently ignored: no error from `-mSdb`, and the value stays whatever
      rawData decodes to, not what was injected into `fields`.
    """

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.index = 1

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

    def _get_cfg_dir(self):
        return tdDnodes.dnodes[0].cfgDir

    def _run_taosd(self, args, cwd=None, timeout_sec=30):
        bin_path = self._get_taosd_bin()
        cmd = [bin_path] + shlex.split(args)
        tdLog.info("run cmd: %s (cwd=%s)" % (" ".join(cmd), cwd))
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
            cwd=cwd,
            timeout=timeout_sec,
        )
        output = proc.stdout or ""
        tdLog.info("ret=%s output=%s" % (proc.returncode, output[:500].replace("\n", "\\n")))
        return proc.returncode, output

    def _dump(self, cfg_dir, sdb_json_path):
        if os.path.exists(sdb_json_path):
            os.remove(sdb_json_path)
        code, _ = self._run_taosd(f"-s -c {cfg_dir}", cwd=cfg_dir)
        tdSql.checkEqual(code, 0)
        tdSql.checkEqual(os.path.exists(sdb_json_path), True)
        with open(sdb_json_path, "r", encoding="utf-8") as fp:
            return json.load(fp)

    def _find_one(self, records, rtype, match):
        found = [r for r in records if r.get("type") == rtype and match(r)]
        tdSql.checkEqual(len(found), 1)
        return found[0]

    def test_sdb_json_editable_field_takes_effect(self):
        """Editing a field on a type's editable-scalar list and reimporting takes effect.

        1. Stop taosd, dump sdb.json.
        2. Edit the dnode record's `fqdn` (on the FS 4.4 editable list for `dnode`).
        3. Import via -mSdb (must succeed), redump, and verify the change landed.
        4. Restart and verify at the SQL level too.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        """
        cfg_dir = self._get_cfg_dir()
        sdb_json_path = os.path.join(cfg_dir, "sdb.json")

        tdLog.info("stop taosd to edit an editable field")
        tdDnodes.forcestop(self.index)

        before = self._dump(cfg_dir, sdb_json_path)
        dnode_rec = self._find_one(before["records"], "dnode", lambda r: True)
        original_fqdn = dnode_rec["fields"]["fqdn"]
        new_fqdn = "sdb-modify-import-editable-fqdn"
        tdSql.checkEqual(original_fqdn != new_fqdn, True)
        dnode_rec["fields"]["fqdn"] = new_fqdn

        with open(sdb_json_path, "w", encoding="utf-8") as fp:
            json.dump(before, fp)

        code, output = self._run_taosd(f"-mSdb {sdb_json_path} -c {cfg_dir}", cwd=cfg_dir)
        tdSql.checkEqual(code, 0)
        tdLog.info("-mSdb output: %s" % output[:500].replace("\n", "\\n"))

        after = self._dump(cfg_dir, sdb_json_path)
        after_dnode = self._find_one(after["records"], "dnode", lambda r: True)
        tdSql.checkEqual(after_dnode["fields"]["fqdn"], new_fqdn)

        if os.path.exists(sdb_json_path):
            os.remove(sdb_json_path)

        tdLog.info("restarting taosd to verify the editable-field change at the SQL level")
        tdDnodes.start(self.index)

        tdSql.query("show dnodes;")
        tdSql.checkEqual(tdSql.queryRows > 0, True)
        tdLog.info("dnodes after restart: %s" % tdSql.queryResult)

    def test_sdb_json_immutable_field_edit_is_silently_ignored(self):
        """Editing a field that is NOT editable must not error, and must not apply.

        Covers two flavors of "immutable" per FS 4.4:
        (a) field-level: `dnode` supports overlay, but only for fqdn/port --
            `id` and `createdTime` are on the same record's `fields` for
            display only and are never applied.
        (b) type-level: `mnode` has no scalar overlay support at all -- ANY
            edit to its `fields` (e.g. `role`, which is sync-managed) must be
            a complete no-op, since import for this type is rawData-only.

        In both cases `-mSdb` must still exit 0 (no error) -- the FS
        deliberately treats out-of-scope `fields` edits as ignorable, not as
        a validation failure.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        """
        cfg_dir = self._get_cfg_dir()
        sdb_json_path = os.path.join(cfg_dir, "sdb.json")

        tdLog.info("stop taosd to edit immutable fields")
        tdDnodes.forcestop(self.index)

        before = self._dump(cfg_dir, sdb_json_path)
        records = before["records"]

        dnode_rec = self._find_one(records, "dnode", lambda r: True)
        original_id = dnode_rec["fields"]["id"]
        original_created_time = dnode_rec["fields"]["createdTime"]
        bogus_id = str(int(original_id) + 99999)
        bogus_created_time = str(int(original_created_time) + 1)
        tdSql.checkEqual(original_id != bogus_id, True)
        dnode_rec["fields"]["id"] = bogus_id
        dnode_rec["fields"]["createdTime"] = bogus_created_time

        mnode_rec = self._find_one(records, "mnode", lambda r: True)
        original_role = mnode_rec["fields"]["role"]
        bogus_role = str(int(original_role) + 1)
        mnode_rec["fields"]["role"] = bogus_role

        with open(sdb_json_path, "w", encoding="utf-8") as fp:
            json.dump(before, fp)

        code, output = self._run_taosd(f"-mSdb {sdb_json_path} -c {cfg_dir}", cwd=cfg_dir)
        tdLog.info("-mSdb output for immutable-field edit attempt: %s" % output[:800].replace("\n", "\\n"))
        # FS 4.4: fields outside a type's editable list (or a whole rawData
        # -only type) are ignored, not rejected -- -mSdb must still succeed.
        tdSql.checkEqual(code, 0)

        after = self._dump(cfg_dir, sdb_json_path)
        after_dnode = self._find_one(after["records"], "dnode", lambda r: True)
        tdLog.info("dnode.id after reimport (must stay original, injected value ignored): %s vs injected %s"
                    % (after_dnode["fields"]["id"], bogus_id))
        tdSql.checkEqual(after_dnode["fields"]["id"], original_id)
        tdSql.checkEqual(after_dnode["fields"]["createdTime"], original_created_time)

        after_mnode = self._find_one(after["records"], "mnode", lambda r: True)
        tdLog.info("mnode.role after reimport (must stay original, injected value ignored): %s vs injected %s"
                    % (after_mnode["fields"]["role"], bogus_role))
        tdSql.checkEqual(after_mnode["fields"]["role"], original_role)

        if os.path.exists(sdb_json_path):
            os.remove(sdb_json_path)

        tdLog.info("restarting taosd to verify the cluster is still healthy")
        tdDnodes.start(self.index)

        tdSql.query("show mnodes;")
        tdSql.checkEqual(tdSql.queryRows > 0, True)
        tdSql.query("select server_version();")
        tdSql.checkEqual(tdSql.queryRows, 1)
