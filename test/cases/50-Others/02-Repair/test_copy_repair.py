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

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
import getpass
import json
import os
import platform
import shutil
import subprocess

import pytest


def _get_sim_path():
    """Return <project_root>/sim, same as the test framework."""
    self_path = os.path.dirname(os.path.realpath(__file__))
    if "community" in self_path:
        proj_path = self_path[:self_path.find("community")]
    else:
        proj_path = self_path[:self_path.find("test")]
    return os.path.join(proj_path, "sim")


SIM_PATH = _get_sim_path()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_file(path, content):
    """Write text content to a file, creating parent dirs."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if isinstance(content, bytes):
        with open(path, "wb") as f:
            f.write(content)
    else:
        with open(path, "w") as f:
            f.write(content)


def read_file(path):
    with open(path, "r") as f:
        return f.read()


def read_bin(path):
    with open(path, "rb") as f:
        return f.read()


def make_fake_file(path, size, seed=None):
    """Create a file filled with deterministic data."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if seed is None:
        seed = abs(hash(path)) & 0xFF
    data = bytes([(seed + i) & 0xFF for i in range(size)])
    with open(path, "wb") as f:
        f.write(data)
    return data


def files_equal(path_a, path_b):
    """Check if two files are byte-identical."""
    return read_bin(path_a) == read_bin(path_b)


def make_taos_cfg(cfg_dir, data_dirs, log_dir, extra=None):
    """Write a taos.cfg file.

    data_dirs: list of (path, level, primary) tuples.
    """
    os.makedirs(cfg_dir, exist_ok=True)
    lines = [
        "firstEp localhost:6030",
        f"logDir {log_dir}",
    ]
    for path, level, primary in data_dirs:
        lines.append(f"dataDir {path} {level} {primary}")
    if extra:
        for k, v in extra.items():
            lines.append(f"{k} {v}")
    write_file(os.path.join(cfg_dir, "taos.cfg"), "\n".join(lines) + "\n")


def make_dnode_json(data_dir, dnode_id):
    """Create dnode/dnode.json with the given dnodeId."""
    dnode_dir = os.path.join(data_dir, "dnode")
    os.makedirs(dnode_dir, exist_ok=True)
    content = json.dumps({"dnodeId": dnode_id})
    write_file(os.path.join(dnode_dir, "dnode.json"), content)


def make_vnode_json(vnode_dir, vnode_id, dnode_ids, my_index=0):
    """Create a vnode.json that repair code can parse.

    The repair code looks for:
      - config.syncCfg.nodeInfo[] with "nodeId" per entry (string-encoded int)
      - config.syncCfg.myIndex (string-encoded int)
    Values are written as string-encoded integers to match tjsonAddIntegerToObject.
    """
    node_info = []
    for did in dnode_ids:
        node_info.append({
            "nodeId": str(did),
            "clusterId": "0",
            "nodeFqdn": "localhost",
            "nodePort": "6030",
        })
    vnode_json = {
        "config": {
            "syncCfg.myIndex": str(my_index),
            "syncCfg.nodeInfo": node_info,
        }
    }
    write_file(os.path.join(vnode_dir, "vnode.json"), json.dumps(vnode_json))


def make_raft_config_json(sync_dir, dnode_ids, my_index=0):
    """Create sync/raft_config.json."""
    node_info = []
    for did in dnode_ids:
        node_info.append({
            "nodeId": str(did),
            "clusterId": "0",
            "nodeFqdn": "localhost",
            "nodePort": 6030,
        })
    raft_cfg = {
        "RaftCfg": {
            "SSyncCfg": {
                "myIndex": my_index,
                "nodeInfo": node_info,
            }
        }
    }
    os.makedirs(sync_dir, exist_ok=True)
    write_file(os.path.join(sync_dir, "raft_config.json"), json.dumps(raft_cfg))


def make_raft_store_json(sync_dir):
    """Create sync/raft_store.json (should be cleaned after repair)."""
    os.makedirs(sync_dir, exist_ok=True)
    write_file(os.path.join(sync_dir, "raft_store.json"), '{"vote":0}')
    write_file(os.path.join(sync_dir, "some_state.bak"), "backup data")


def tsdb_filename(vnode_id, fid, cid, suffix, mid=0, lcn=0):
    """Build TSDB file name matching dmBuildTsdbFilePath logic.

    mid!=0 && lcn>0: v{vid}f{fid}ver{cid}.m{mid}.{lcn}.{suffix}
    mid!=0 && lcn==0: v{vid}f{fid}ver{cid}.m{mid}.{suffix}
    mid==0 && lcn>0: v{vid}f{fid}ver{cid}.{lcn}.{suffix}
    mid==0 && lcn==0: v{vid}f{fid}ver{cid}.{suffix}
    """
    if lcn > 0:
        if mid != 0:
            return f"v{vnode_id}f{fid}ver{cid}.m{mid}.{lcn}.{suffix}"
        else:
            return f"v{vnode_id}f{fid}ver{cid}.{lcn}.{suffix}"
    else:
        if mid != 0:
            return f"v{vnode_id}f{fid}ver{cid}.m{mid}.{suffix}"
        else:
            return f"v{vnode_id}f{fid}ver{cid}.{suffix}"


def make_current_json(fsets):
    """Build current.json content from a list of file set dicts.

    Each fset: {
        "fid": int,
        "files": [{type, did_level, did_id, fid, cid, size, lcn, ...}],
        "last_compact": int, "last_commit": int,
    }
    """
    SUFFIXES = {0: "head", 1: "data", 2: "sma", 3: "tomb", 5: "stt"}
    fset_arr = []
    for fs in fsets:
        fset_json = {"fid": fs["fid"]}
        # Non-STT files
        for f in fs["files"]:
            ftype = f["type"]
            if ftype in (0, 1, 2, 3):
                fset_json[SUFFIXES[ftype]] = {
                    "did.level": f.get("did_level", 0),
                    "did.id": f.get("did_id", 0),
                    "lcn": f.get("lcn", 0),
                    "fid": f["fid"],
                    "mid": f.get("mid", 0),
                    "cid": f["cid"],
                    "size": f["size"],
                    "minVer": f.get("minVer", 0),
                    "maxVer": f.get("maxVer", 0),
                }
        # STT files grouped by level
        stt_files = [f for f in fs["files"] if f["type"] == 5]
        stt_levels = sorted(set(f.get("sttLevel", 0) for f in stt_files))
        stt_lvl_arr = []
        for sl in stt_levels:
            level_files = [f for f in stt_files if f.get("sttLevel", 0) == sl]
            files_arr = []
            for f in level_files:
                files_arr.append({
                    "did.level": f.get("did_level", 0),
                    "did.id": f.get("did_id", 0),
                    "lcn": f.get("lcn", 0),
                    "fid": f["fid"],
                    "mid": f.get("mid", 0),
                    "cid": f["cid"],
                    "size": f["size"],
                    "minVer": f.get("minVer", 0),
                    "maxVer": f.get("maxVer", 0),
                    "level": sl,
                })
            stt_lvl_arr.append({"level": sl, "files": files_arr})
        fset_json["stt lvl"] = stt_lvl_arr
        fset_json["last compact"] = fs.get("last_compact", 0)
        fset_json["last commit"] = fs.get("last_commit", 0)
        fset_arr.append(fset_json)
    return json.dumps({"fmtv": 1, "fset": fset_arr})


def make_source_vnode(primary_data, vnode_id, fsets, dnode_ids, my_index=0,
                      extra_data_dirs=None, file_size=1024):
    """Create a complete source vnode directory tree with fake TSDB files.

    Args:
        primary_data: primary data dir path
        vnode_id: integer vnode id
        fsets: list of file set dicts (same as make_current_json)
        dnode_ids: list of dnode IDs for vnode.json nodeInfo
        my_index: source myIndex
        extra_data_dirs: list of (path, level) for non-primary disks with TSDB files
        file_size: size of fake files
    Returns:
        dict mapping (fid, cid, suffix) -> bytes content of each created file
    """
    SUFFIXES = {0: "head", 1: "data", 2: "sma", 3: "tomb", 5: "stt"}
    vnode_dir = os.path.join(primary_data, "vnode", f"vnode{vnode_id}")
    tsdb_dir = os.path.join(vnode_dir, "tsdb")
    sync_dir = os.path.join(vnode_dir, "sync")

    # Create vnode.json
    make_vnode_json(vnode_dir, vnode_id, dnode_ids, my_index)

    # Create sync state files
    make_raft_config_json(sync_dir, dnode_ids, my_index)
    make_raft_store_json(sync_dir)

    # Create a dummy wal file in wal/ subdir
    wal_dir = os.path.join(vnode_dir, "wal")
    write_file(os.path.join(wal_dir, "meta-ver0"), "wal meta content")

    # Build disk map: did (level, id) -> data dir path
    disk_map = {(0, 0): primary_data}
    if extra_data_dirs:
        for path, level in extra_data_dirs:
            # Count existing disks at this level
            existing = sum(1 for (l, _) in disk_map if l == level)
            disk_map[(level, existing)] = path

    # Create TSDB files on appropriate disks
    file_contents = {}
    for fs in fsets:
        for f in fs["files"]:
            ftype = f["type"]
            suffix = SUFFIXES[ftype]
            did_level = f.get("did_level", 0)
            did_id = f.get("did_id", 0)
            mid = f.get("mid", 0)
            lcn = f.get("lcn", 0)
            disk_path = disk_map.get((did_level, did_id), primary_data)
            fname = tsdb_filename(vnode_id, f["fid"], f["cid"], suffix, mid=mid, lcn=lcn)
            fpath = os.path.join(disk_path, "vnode", f"vnode{vnode_id}", "tsdb", fname)
            content = make_fake_file(fpath, f["size"], seed=hash(fname) & 0xFF)
            file_contents[(f["fid"], f["cid"], suffix, mid, lcn)] = content
            # Update size in fset to match actual
            f["size"] = len(content)

    # Write current.json
    os.makedirs(tsdb_dir, exist_ok=True)
    write_file(os.path.join(tsdb_dir, "current.json"), make_current_json(fsets))

    return file_contents


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCopyModeRepair:
    """Repair-copy tests (single-disk and multi-disk), local and remote."""

    taosd_bin = None
    source_host = None
    _ssh_ok = None

    @classmethod
    def setup_class(cls):
        if platform.system() == "Windows":
            pytest.skip("copy-mode repair is not supported on Windows")
        cls.taosd_bin = cls._find_taosd()
        if cls.taosd_bin is None:
            pytest.skip("taosd not found")
        cls.source_host = f"{getpass.getuser()}@127.0.0.1"
        cls._ssh_ok = cls._ssh_localhost_ok()

    @staticmethod
    def _ssh_localhost_ok():
        """Return True if passwordless SSH to 127.0.0.1 works."""
        user = getpass.getuser()
        try:
            r = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                 f"{user}@127.0.0.1", "true"],
                capture_output=True, timeout=10)
            return r.returncode == 0
        except Exception:
            return False

    @staticmethod
    def _find_taosd():
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
        return None

    def _run_repair(self, target_cfg_dir, source_cfg_path, vnode_ids_str,
                    source_host=None, timeout=60):
        """Run taosd in repair-copy mode and return the CompletedProcess."""
        cmd = [
            self.taosd_bin,
            "-c", target_cfg_dir,
            "-r",
            "--mode", "copy",
            "--node-type", "vnode",
            "--source-cfg", source_cfg_path,
            "--vnode", vnode_ids_str,
        ]
        if source_host:
            cmd.extend(["--source-host", source_host])
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    def _setup_env(self, src_disks=None, tgt_disks=None,
                   vnode_id=2, dnode_ids=None, target_dnode_id=2):
        """Set up source and target environments.

        src_disks, tgt_disks: list of (subdir_name, level, primary) tuples.
            Defaults to single-disk: [("data", 0, 1)].
        """
        for d in ("dnode1", "dnode2"):
            p = os.path.join(SIM_PATH, d)
            if os.path.exists(p):
                shutil.rmtree(p)
            os.makedirs(p, exist_ok=True)

        if src_disks is None:
            src_disks = [("data", 0, 1)]
        if tgt_disks is None:
            tgt_disks = [("data", 0, 1)]
        if dnode_ids is None:
            dnode_ids = [1, 2, 3]

        src_data_dirs = []
        tgt_data_dirs = []

        for name, level, primary in src_disks:
            path = os.path.join(SIM_PATH, "dnode1", name)
            os.makedirs(path, exist_ok=True)
            src_data_dirs.append((path, level, primary))

        for name, level, primary in tgt_disks:
            path = os.path.join(SIM_PATH, "dnode2", name)
            os.makedirs(os.path.join(path, "vnode"), exist_ok=True)
            tgt_data_dirs.append((path, level, primary))

        src_cfg = os.path.join(SIM_PATH, "dnode1", "cfg")
        tgt_cfg = os.path.join(SIM_PATH, "dnode2", "cfg")
        tgt_log = os.path.join(SIM_PATH, "dnode2", "log")
        os.makedirs(tgt_log, exist_ok=True)

        make_taos_cfg(src_cfg, src_data_dirs, os.path.join(SIM_PATH, "dnode1", "log"))
        make_taos_cfg(tgt_cfg, tgt_data_dirs, tgt_log)

        tgt_primary = next(p for p, l, pr in tgt_data_dirs if pr == 1)
        make_dnode_json(tgt_primary, target_dnode_id)

        return {
            "src_data_dirs": src_data_dirs,
            "tgt_data_dirs": tgt_data_dirs,
            "src_data": src_data_dirs[0][0],
            "tgt_data": tgt_data_dirs[0][0],
            "src_cfg": os.path.join(src_cfg, "taos.cfg"),
            "tgt_cfg_dir": tgt_cfg,
            "tgt_log": tgt_log,
            "vnode_id": vnode_id,
            "dnode_ids": dnode_ids,
            "target_dnode_id": target_dnode_id,
        }

    def _do_test_basic_copy(self, source_host=None):
        """Copy a vnode with 2 file sets (head+data+sma+tomb each) from source to empty target."""
        env = self._setup_env()
        vid = env["vnode_id"]

        fsets = [
            {
                "fid": 1, "last_compact": 100, "last_commit": 200,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 512, "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 1024, "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 2, "fid": 1, "cid": 10, "size": 256, "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 3, "fid": 1, "cid": 10, "size": 128, "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
            {
                "fid": 2, "last_compact": 300, "last_commit": 400,
                "files": [
                    {"type": 0, "fid": 2, "cid": 20, "size": 512, "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 2, "cid": 20, "size": 2048, "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        src_contents = make_source_vnode(
            env["src_data"], vid, fsets, env["dnode_ids"], my_index=0)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"taosd failed:\nstdout: {result.stdout}\nstderr: {result.stderr}")

        # Verify TSDB files are copied with correct content
        tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing file: {fname}")
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Content mismatch: {fname}")

        # Verify current.json was regenerated
        current_path = os.path.join(tgt_tsdb, "current.json")
        tdSql.checkEqual(os.path.isfile(current_path), True, f"Missing file: current.json")
        current = json.loads(read_file(current_path))
        tdSql.checkEqual(current["fmtv"], 1, "Incorrect fmtv in current.json")
        tdSql.checkEqual(len(current["fset"]), 2, "Incorrect number of fsets in current.json")
        # Check fids are present and sorted
        fids = [fs["fid"] for fs in current["fset"]]
        tdSql.checkEqual(fids, [1, 2], "Fids in current.json are not as expected")

        # Verify non-tsdb files were copied
        tgt_vnode = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}")
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_vnode, "vnode.json")), True, "Missing file: vnode.json")
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_vnode, "wal", "meta-ver0")), True, "Missing file: wal/meta-ver0")

        # Verify sync state cleaned: no raft_store.json, no .bak in sync/
        sync_dir = os.path.join(tgt_vnode, "sync")
        tdSql.checkEqual(not os.path.exists(os.path.join(sync_dir, "raft_store.json")), True, "Unexpected file: raft_store.json")
        for entry in os.listdir(sync_dir) if os.path.isdir(sync_dir) else []:
            tdSql.checkEqual(not entry.endswith(".bak"), True, f"Unexpected .bak file: {entry}")

        # Verify vnode.json myIndex updated
        vnode_json = json.loads(read_file(os.path.join(tgt_vnode, "vnode.json")))
        config = vnode_json["config"]
        # target_dnode_id=2 is at index 1 in dnode_ids=[1,2,3]
        tdSql.checkEqual(int(config["syncCfg.myIndex"]), 1, "Incorrect myIndex in vnode.json")

        # Verify .bak directories cleaned up
        bak_dir = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}.bak")
        tdSql.checkEqual(not os.path.exists(bak_dir), True, "Backup dir should be deleted after success")

    def test_basic_copy_local(self):
        """Basic local mode copy test 

        1. Copy vnode from local source.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_basic_copy()

    def test_basic_copy_remote(self):
        """Basic remote mode copy test

        1. Copy vnode from remote source via SSH.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_basic_copy(source_host=self.source_host)

    def _do_test_stt_files(self, source_host=None):
        """Copy a vnode with STT files at multiple levels."""
        env = self._setup_env()
        vid = env["vnode_id"]

        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 512, "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 1024, "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 5, "fid": 1, "cid": 11, "size": 768, "did_level": 0, "did_id": 0,
                     "lcn": 0, "sttLevel": 0, "minVer": 1, "maxVer": 100},
                    {"type": 5, "fid": 1, "cid": 12, "size": 512, "did_level": 0, "did_id": 0,
                     "lcn": 0, "sttLevel": 1, "minVer": 50, "maxVer": 200},
                ],
            },
        ]
        src_contents = make_source_vnode(
            env["src_data"], vid, fsets, env["dnode_ids"])

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"taosd failed:\nstderr: {result.stderr}")

        # Verify all TSDB files are copied with correct content
        tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing file: {fname}")
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Mismatch: {fname}")

        # Verify current.json has stt lvl entries
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))
        fset0 = current["fset"][0]
        tdSql.checkEqual("stt lvl" in fset0, True, "Missing 'stt lvl' in current.json")
        stt_lvl = fset0["stt lvl"]
        tdSql.checkEqual(len(stt_lvl), 2, "Incorrect number of stt levels in current.json")
        tdSql.checkEqual(stt_lvl[0]["level"], 0, "Incorrect stt level 0 in current.json")
        tdSql.checkEqual(stt_lvl[1]["level"], 1, "Incorrect stt level 1 in current.json")

    def test_stt_files_local(self):
        """STT files local mode copy test

        1. Copy vnode with STT files at multiple levels from local source.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_stt_files()

    def test_stt_files_remote(self):
        """STT files remote mode copy test

        1. Copy vnode with STT files at multiple levels from remote source via SSH.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_stt_files(source_host=self.source_host)

    def _do_test_empty_target(self, source_host=None):
        """When target has no vnode directory at all, all files should be copied."""
        env = self._setup_env()
        vid = env["vnode_id"]

        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 5, "size": 256, "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        make_source_vnode(env["src_data"], vid, fsets, env["dnode_ids"])

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"stderr: {result.stderr}")

        tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb, "current.json")), True, "Missing file: current.json")
        fname = tsdb_filename(vid, 1, 5, "head")
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb, fname)), True, f"Missing file: {fname}")

    def test_empty_target_local(self):
        """Empty target local mode copy test

        1. Copy vnode when target has no vnode directory and no local current.json (local source).
        2. All source files should be copied.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_empty_target()

    def test_empty_target_remote(self):
        """Empty target remote mode copy test

        1. Copy vnode when target has no vnode directory and no local current.json (remote source via SSH).
        2. All source files should be copied.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_empty_target(source_host=self.source_host)

    def _do_test_multiple_vnodes(self, source_host=None):
        """Repair multiple vnodes in one invocation."""
        env = self._setup_env()

        for vid in [2, 5]:
            fsets = [
                {
                    "fid": 1, "last_compact": 0, "last_commit": 0,
                    "files": [
                        {"type": 0, "fid": 1, "cid": vid * 10, "size": 256,
                         "did_level": 0, "did_id": 0, "lcn": 0},
                        {"type": 1, "fid": 1, "cid": vid * 10, "size": 512,
                         "did_level": 0, "did_id": 0, "lcn": 0},
                    ],
                },
            ]
            make_source_vnode(env["src_data"], vid, fsets, env["dnode_ids"])

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], "2,5", source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"stderr: {result.stderr}")

        for vid in [2, 5]:
            tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")
            tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb, "current.json")), True, "Missing file: current.json")

    def test_multiple_vnodes_local(self):
        """Multiple vnodes local mode copy test

        1. Repair multiple vnodes in one invocation from local source.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_multiple_vnodes()

    def test_multiple_vnodes_remote(self):
        """Multiple vnodes remote mode copy test

        1. Repair multiple vnodes in one invocation from remote source via SSH.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_multiple_vnodes(source_host=self.source_host)

    def _do_test_skip_missing_source_vnode(self, source_host=None):
        """When source vnode doesn't exist, it should be skipped (not fail)."""
        env = self._setup_env()
        vid = 99  # Source vnode 99 doesn't exist

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        # Should succeed overall (vnode is skipped, not failed)
        tdSql.checkEqual(result.returncode, 0, "Vnode repair failed for missing source vnode")

    def test_skip_missing_source_vnode_local(self):
        """Skip missing source vnode local mode test

        1. Verify missing source vnode is skipped without failure (local source).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_skip_missing_source_vnode()

    def test_skip_missing_source_vnode_remote(self):
        """Skip missing source vnode remote mode test

        1. Verify missing source vnode is skipped without failure (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_skip_missing_source_vnode(source_host=self.source_host)

    def _do_test_skip_existing_bak(self, source_host=None):
        """When vnode.bak already exists on target, the vnode should be skipped."""
        env = self._setup_env()
        vid = env["vnode_id"]

        # Create source vnode
        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        make_source_vnode(env["src_data"], vid, fsets, env["dnode_ids"])

        # Create a pre-existing .bak on target
        bak_dir = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}.bak")
        os.makedirs(bak_dir, exist_ok=True)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, "Vnode repair failed for existing .bak")

        # .bak should still be there (untouched)
        tdSql.checkEqual(os.path.isdir(bak_dir), True, "Missing .bak directory")

    def test_skip_existing_bak_local(self):
        """Skip existing .bak local mode test

        1. Verify vnode is skipped when .bak already exists on target (local source).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_skip_existing_bak()

    def test_skip_existing_bak_remote(self):
        """Skip existing .bak remote mode test

        1. Verify vnode is skipped when .bak already exists on target (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_skip_existing_bak(source_host=self.source_host)

    def test_exit_code_bad_args(self):
        """Bad arguments exit code test

        1. Run taosd with --mode copy but without --vnode.
        2. Expect non-zero exit code (argument validation failure before repair starts).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        env = self._setup_env()
        # Missing --vnode
        cmd = [
            self.taosd_bin, "-c", env["tgt_cfg_dir"],
            "-r", "--mode", "copy",
            "--node-type", "vnode",
            "--source-cfg", env["src_cfg"],
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        tdSql.checkEqual(result.returncode != 0, True, "Expected non-zero exit code for bad args")

    def test_exit_code_missing_source_cfg(self):
        """Missing source config exit code test

        1. Run taosd with a non-existent --source-cfg path.
        2. Expect the vnode to be SKIPPED (source dataDir not found) or a non-zero exit code.
           cfgLoad falls back to defaults so the source path is wrong, causing skip.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        env = self._setup_env()
        bogus_cfg = os.path.join(SIM_PATH, "nonexistent", "taos.cfg")
        cmd = [
            self.taosd_bin, "-c", env["tgt_cfg_dir"],
            "-r", "--mode", "copy",
            "--node-type", "vnode",
            "--source-cfg", bogus_cfg,
            "--vnode", "2",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        # cfgLoad falls back to defaults; vnode is skipped (source dir wrong)
        combined = result.stdout + result.stderr
        tdSql.checkEqual("SKIPPED" in combined or result.returncode != 0, True, "Expected vnode to be skipped or non-zero exit code")

    # --- Multi-disk / multi-tier tests ---

    def _do_test_two_tier_to_single_tier(self, source_host=None):
        """Source has 2 tiers, target has 1 tier — tier folding should work."""
        env = self._setup_env(
            src_disks=[("data_l0_d0", 0, 1), ("data_l1_d0", 1, 0)],
            tgt_disks=[("data_l0_d0", 0, 1)],
        )
        vid = 2
        # Files on level 0 and level 1 of source
        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 1024,
                     "did_level": 1, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        src_primary = env["src_data_dirs"][0][0]
        extra_data_dirs = [(env["src_data_dirs"][1][0], 1)]
        src_contents = make_source_vnode(
            src_primary, vid, fsets, env["dnode_ids"],
            extra_data_dirs=extra_data_dirs)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"stderr: {result.stderr}")

        # All files should end up on target level 0 (the only tier)
        tgt_primary = env["tgt_data_dirs"][0][0]
        tgt_tsdb = os.path.join(tgt_primary, "vnode", f"vnode{vid}", "tsdb")
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing file: {fname}")
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Mismatch: {fname}")

        # Verify current.json has all files remapped to level 0
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))
        for fset in current["fset"]:
            for key in ("head", "data", "sma", "tomb"):
                if key in fset:
                    tdSql.checkEqual(fset[key]["did.level"], 0, f"{key} should be on level 0")

    def test_two_tier_to_single_tier_local(self):
        """Two-tier to single-tier local mode copy test

        1. Copy vnode from 2-tier source to 1-tier target with tier folding (local source).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_two_tier_to_single_tier()

    def test_two_tier_to_single_tier_remote(self):
        """Two-tier to single-tier remote mode copy test

        1. Copy vnode from 2-tier source to 1-tier target with tier folding (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_two_tier_to_single_tier(source_host=self.source_host)

    def _do_test_multi_disk_round_robin(self, source_host=None):
        """Files should be distributed across multiple disks at the same tier."""
        env = self._setup_env(
            src_disks=[("data_l0_d0", 0, 1)],
            tgt_disks=[("data_l0_d0", 0, 1), ("data_l0_d1", 0, 0)],
        )
        vid = 2
        # Create multiple file sets — files should spread across tgt_d0 and tgt_d1
        fsets = []
        for fid in range(1, 5):
            fsets.append({
                "fid": fid, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": fid, "cid": fid * 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": fid, "cid": fid * 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            })
        make_source_vnode(env["src_data_dirs"][0][0], vid, fsets, env["dnode_ids"])

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"stderr: {result.stderr}")

        # Parse generated current.json and check disk distribution
        tgt_primary = env["tgt_data_dirs"][0][0]
        tgt_tsdb = os.path.join(tgt_primary, "vnode", f"vnode{vid}", "tsdb")
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))

        # Collect all did.id values — should see both 0 and 1
        did_ids = set()
        for fset in current["fset"]:
            for key in ("head", "data"):
                if key in fset:
                    did_ids.add(fset[key]["did.id"])
        tdSql.checkEqual(len(did_ids) > 1, True, f"Expected round-robin across disks, got did_ids={did_ids}")

        # Verify files exist on the respective disks
        for fset in current["fset"]:
            for key in ("head", "data"):
                if key in fset:
                    did_id = fset[key]["did.id"]
                    disk_path = env["tgt_data_dirs"][did_id][0]
                    fid = fset["fid"]
                    cid = fset[key]["cid"]
                    fname = tsdb_filename(vid, fid, cid, key)
                    fpath = os.path.join(disk_path, "vnode", f"vnode{vid}", "tsdb", fname)
                    tdSql.checkEqual(os.path.isfile(fpath), True, f"Missing on disk {did_id}: {fname}")

    def test_multi_disk_round_robin_local(self):
        """Multi-disk round-robin local mode copy test

        1. Verify files are distributed across multiple target disks at same tier (local source).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_multi_disk_round_robin()

    def test_multi_disk_round_robin_remote(self):
        """Multi-disk round-robin remote mode copy test

        1. Verify files are distributed across multiple target disks at same tier (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_multi_disk_round_robin(source_host=self.source_host)

    def _do_test_multi_source_disks_same_level(self, source_host=None):
        """Source has multiple disks at level 0 — files from all disks are copied."""
        env = self._setup_env(
            src_disks=[("data_l0_d0", 0, 1), ("data_l0_d1", 0, 0)],
            tgt_disks=[("data_l0_d0", 0, 1)],
        )
        vid = 2
        # Files spread across disk 0 and disk 1 at level 0
        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 1, "lcn": 0},
                ],
            },
            {
                "fid": 2, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 2, "cid": 20, "size": 256,
                     "did_level": 0, "did_id": 1, "lcn": 0},
                    {"type": 1, "fid": 2, "cid": 20, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        src_primary = env["src_data_dirs"][0][0]
        extra_data_dirs = [(env["src_data_dirs"][1][0], 0)]
        src_contents = make_source_vnode(
            src_primary, vid, fsets, env["dnode_ids"],
            extra_data_dirs=extra_data_dirs)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"stderr: {result.stderr}")

        # All files should land on the single target disk
        tgt_primary = env["tgt_data_dirs"][0][0]
        tgt_tsdb = os.path.join(tgt_primary, "vnode", f"vnode{vid}", "tsdb")
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing file: {fname}")
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Mismatch: {fname}")

    def test_multi_source_disks_same_level_local(self):
        """Multi-source-disk same level local mode copy test

        1. Copy files from multiple source disks at same level to single target disk (local source).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_multi_source_disks_same_level()

    def test_multi_source_disks_same_level_remote(self):
        """Multi-source-disk same level remote mode copy test

        1. Copy files from multiple source disks at same level to single target disk (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_multi_source_disks_same_level(source_host=self.source_host)

    def _do_test_s3_warning_lcn_gt_1(self, source_host=None):
        """File sets with lcn > 1 should produce shared storage warning in output."""
        env = self._setup_env(
            src_disks=[("data_l0_d0", 0, 1)],
            tgt_disks=[("data_l0_d0", 0, 1)],
        )
        vid = 2
        # lcn=2 means only the last chunk is local; shared storage warning expected
        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 2},
                ],
            },
        ]
        src_primary = env["src_data_dirs"][0][0]
        # For lcn > 0, the local chunk file name is v{vid}f{fid}ver{cid}.{lcn}.{suffix}
        vnode_dir = os.path.join(src_primary, "vnode", f"vnode{vid}")
        tsdb_dir = os.path.join(vnode_dir, "tsdb")
        os.makedirs(tsdb_dir, exist_ok=True)

        # Create normal head file (lcn=0)
        head_name = tsdb_filename(vid, 1, 10, "head")
        make_fake_file(os.path.join(tsdb_dir, head_name), 256)

        # Create shared storage last-chunk data file: v2f1ver10.2.data
        ss_data_name = f"v{vid}f1ver10.2.data"
        make_fake_file(os.path.join(tsdb_dir, ss_data_name), 512)

        # Write current.json
        write_file(os.path.join(tsdb_dir, "current.json"), make_current_json(fsets))

        # Create supporting vnode files
        make_vnode_json(vnode_dir, vid, env["dnode_ids"])
        sync_dir = os.path.join(vnode_dir, "sync")
        make_raft_config_json(sync_dir, env["dnode_ids"])
        make_raft_store_json(sync_dir)
        write_file(os.path.join(vnode_dir, "wal", "meta-ver0"), "wal")

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"stderr: {result.stderr}")

        # Verify shared storage warning appears in stdout/stderr
        combined_output = result.stdout + result.stderr
        tdSql.checkEqual("shared storage" in combined_output.lower(), True, \
            f"Expected shared storage warning in output, got:\n{combined_output}")

    def test_s3_warning_lcn_gt_1_local(self):
        """Shared storage warning for lcn>1 local mode test

        1. Verify shared storage warning is produced for file sets with lcn > 1 (local source).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_s3_warning_lcn_gt_1()

    def test_s3_warning_lcn_gt_1_remote(self):
        """Shared storage warning for lcn>1 remote mode test

        1. Verify shared storage warning is produced for file sets with lcn > 1 (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_s3_warning_lcn_gt_1(source_host=self.source_host)

    def _do_test_three_tier_to_single_tier(self, source_host=None):
        """Source has 3 tiers, target has 1 tier — all files map to {0,0}."""
        env = self._setup_env(
            src_disks=[("data_l0_d0", 0, 1), ("data_l1_d0", 1, 0), ("data_l2_d0", 2, 0)],
            tgt_disks=[("data_l0_d0", 0, 1)],
        )
        vid = 2
        # Files spread across all three source tiers
        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 1, "did_id": 0, "lcn": 0},
                    {"type": 2, "fid": 1, "cid": 10, "size": 128,
                     "did_level": 2, "did_id": 0, "lcn": 0},
                ],
            },
            {
                "fid": 2, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 2, "cid": 20, "size": 256,
                     "did_level": 2, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 2, "cid": 20, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        src_primary = env["src_data_dirs"][0][0]
        extra_data_dirs = [(env["src_data_dirs"][1][0], 1), (env["src_data_dirs"][2][0], 2)]
        src_contents = make_source_vnode(
            src_primary, vid, fsets, env["dnode_ids"],
            extra_data_dirs=extra_data_dirs)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"stderr: {result.stderr}")

        # All files must land on the single target disk at level 0
        tgt_primary = env["tgt_data_dirs"][0][0]
        tgt_tsdb = os.path.join(tgt_primary, "vnode", f"vnode{vid}", "tsdb")
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing file: {fname}")
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Mismatch: {fname}")

        # Verify current.json has all disk IDs remapped to {0, 0}
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))
        for fset in current["fset"]:
            for key in ("head", "data", "sma", "tomb"):
                if key in fset:
                    tdSql.checkEqual(fset[key]["did.level"], 0, f"fid={fset['fid']} {key} should be on level 0")
                    tdSql.checkEqual(fset[key]["did.id"], 0, f"fid={fset['fid']} {key} should be on disk 0")

    def test_three_tier_to_single_tier_local(self):
        """Three-tier to single-tier local mode copy test

        1. Copy vnode from 3-tier source to 1-tier target, all files map to {0,0} (local source).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        self._do_test_three_tier_to_single_tier()

    def test_three_tier_to_single_tier_remote(self):
        """Three-tier to single-tier remote mode copy test

        1. Copy vnode from 3-tier source to 1-tier target, all files map to {0,0} (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-6 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_three_tier_to_single_tier(source_host=self.source_host)

    # ------------------------------------------------------------------
    # mid (shared storage migration id) tests
    # ------------------------------------------------------------------

    def _do_test_mid_basic_copy(self, source_host=None):
        """Copy a vnode with files having non-zero mid values."""
        env = self._setup_env()
        vid = env["vnode_id"]

        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 5},
                    {"type": 1, "fid": 1, "cid": 10, "size": 1024,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 5},
                    {"type": 2, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 0},
                ],
            },
            {
                "fid": 2, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 2, "cid": 20, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 3},
                    {"type": 1, "fid": 2, "cid": 20, "size": 2048,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 0},
                ],
            },
        ]
        src_contents = make_source_vnode(
            env["src_data"], vid, fsets, env["dnode_ids"], my_index=0)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"taosd failed:\nstdout: {result.stdout}\nstderr: {result.stderr}")

        # Verify TSDB files are copied with correct content and filenames
        tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing file: {fname}")
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Content mismatch: {fname}")

        # Verify mid is preserved in current.json
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))
        fset1 = current["fset"][0]
        tdSql.checkEqual(fset1["head"]["mid"], 5, "mid not preserved for head in fset 1")
        tdSql.checkEqual(fset1["data"]["mid"], 5, "mid not preserved for data in fset 1")
        tdSql.checkEqual(fset1["sma"]["mid"], 0, "mid not preserved for sma in fset 1")
        fset2 = current["fset"][1]
        tdSql.checkEqual(fset2["head"]["mid"], 3, "mid not preserved for head in fset 2")
        tdSql.checkEqual(fset2["data"]["mid"], 0, "mid not preserved for data in fset 2")

    def test_mid_basic_copy_local(self):
        """Shared storage mid field basic copy local test

        1. Verify files with non-zero mid are named correctly (v{vid}f{fid}ver{cid}.m{mid}.{suffix}).
        2. Verify mid is preserved in the output current.json.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-7 Bomin Zhang created
        """
        self._do_test_mid_basic_copy()

    def test_mid_basic_copy_remote(self):
        """Shared storage mid field basic copy remote test

        1. Verify files with non-zero mid are named correctly (remote source via SSH).
        2. Verify mid is preserved in the output current.json.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-7 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_mid_basic_copy(source_host=self.source_host)

    def _do_test_mid_with_lcn(self, source_host=None):
        """Copy files that have both mid and lcn set (combined pattern)."""
        env = self._setup_env(
            src_disks=[("data_l0_d0", 0, 1)],
            tgt_disks=[("data_l0_d0", 0, 1)],
        )
        vid = 2

        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    # mid=0, lcn=0 -> v2f1ver10.head
                    {"type": 0, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 0},
                    # mid=3, lcn=0 -> v2f1ver10.m3.data
                    {"type": 1, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 3},
                    # mid=0, lcn=2 -> v2f1ver10.2.sma (last chunk, no mid)
                    {"type": 2, "fid": 1, "cid": 10, "size": 128,
                     "did_level": 0, "did_id": 0, "lcn": 2, "mid": 0},
                    # mid=7, lcn=3 -> v2f1ver10.m7.3.tomb (both mid and lcn)
                    {"type": 3, "fid": 1, "cid": 10, "size": 64,
                     "did_level": 0, "did_id": 0, "lcn": 3, "mid": 7},
                ],
            },
        ]
        src_contents = make_source_vnode(
            env["src_data"], vid, fsets, env["dnode_ids"], my_index=0)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"taosd failed:\nstdout: {result.stdout}\nstderr: {result.stderr}")

        # Verify each filename variant is correct on disk
        tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")

        # mid=0, lcn=0: plain filename
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb, "v2f1ver10.head")), True,
                         "Missing: v2f1ver10.head")
        # mid=3, lcn=0: .m3 in name
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb, "v2f1ver10.m3.data")), True,
                         "Missing: v2f1ver10.m3.data")
        # mid=0, lcn=2: .2 in name (shared storage warning expected)
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb, "v2f1ver10.2.sma")), True,
                         "Missing: v2f1ver10.2.sma")
        # mid=7, lcn=3: .m7.3 in name
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb, "v2f1ver10.m7.3.tomb")), True,
                         "Missing: v2f1ver10.m7.3.tomb")

        # Verify content matches
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Content mismatch: {fname}")

        # Verify mid and lcn in current.json
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))
        fset = current["fset"][0]
        tdSql.checkEqual(fset["head"]["mid"], 0, "head mid should be 0")
        tdSql.checkEqual(fset["head"]["lcn"], 0, "head lcn should be 0")
        tdSql.checkEqual(fset["data"]["mid"], 3, "data mid should be 3")
        tdSql.checkEqual(fset["data"]["lcn"], 0, "data lcn should be 0")
        tdSql.checkEqual(fset["sma"]["mid"], 0, "sma mid should be 0")
        tdSql.checkEqual(fset["sma"]["lcn"], 2, "sma lcn should be 2")
        tdSql.checkEqual(fset["tomb"]["mid"], 7, "tomb mid should be 7")
        tdSql.checkEqual(fset["tomb"]["lcn"], 3, "tomb lcn should be 3")

    def test_mid_with_lcn_local(self):
        """Mid combined with lcn filename patterns local test

        1. Verify all four filename patterns: plain, mid-only, lcn-only, mid+lcn.
        2. Verify mid and lcn are preserved in output current.json.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-7 Bomin Zhang created
        """
        self._do_test_mid_with_lcn()

    def test_mid_with_lcn_remote(self):
        """Mid combined with lcn filename patterns remote test

        1. Verify all four filename patterns with remote source via SSH.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-7 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_mid_with_lcn(source_host=self.source_host)

    def _do_test_mid_stt_files(self, source_host=None):
        """Copy STT files with non-zero mid values."""
        env = self._setup_env()
        vid = env["vnode_id"]

        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 2},
                    {"type": 5, "fid": 1, "cid": 10, "size": 384,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 4,
                     "sttLevel": 0},
                    {"type": 5, "fid": 1, "cid": 11, "size": 192,
                     "did_level": 0, "did_id": 0, "lcn": 0, "mid": 0,
                     "sttLevel": 0},
                ],
            },
        ]
        src_contents = make_source_vnode(
            env["src_data"], vid, fsets, env["dnode_ids"], my_index=0)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"taosd failed:\nstdout: {result.stdout}\nstderr: {result.stderr}")

        # Verify files
        tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")
        for (fid, cid, suffix, mid, lcn), expected_content in src_contents.items():
            fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
            tgt_path = os.path.join(tgt_tsdb, fname)
            tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing file: {fname}")
            tdSql.checkEqual(read_bin(tgt_path), expected_content, f"Content mismatch: {fname}")

        # Verify mid in STT entries in current.json
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))
        fset = current["fset"][0]
        stt_lvl = fset["stt lvl"][0]
        stt_files = stt_lvl["files"]
        # Find STT file with cid=10 (mid=4) and cid=11 (mid=0)
        mids_by_cid = {int(f["cid"]): int(f["mid"]) for f in stt_files}
        tdSql.checkEqual(mids_by_cid[10], 4, "STT cid=10 mid should be 4")
        tdSql.checkEqual(mids_by_cid[11], 0, "STT cid=11 mid should be 0")

    def test_mid_stt_files_local(self):
        """STT files with mid field local test

        1. Verify STT files with non-zero mid are copied correctly.
        2. Verify mid is preserved in STT section of current.json.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-7 Bomin Zhang created
        """
        self._do_test_mid_stt_files()

    def test_mid_stt_files_remote(self):
        """STT files with mid field remote test

        1. Verify STT files with non-zero mid are copied correctly (remote source via SSH).

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-7 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_mid_stt_files(source_host=self.source_host)

    # --- Retain (hard-link) tests ---

    def _do_test_retain_existing_fsets(self, source_host=None):
        """Target already has fid=1 with intact files; source has fid=1 and fid=2.
        After repair, fid=1 should be hard-linked from backup (same content),
        and fid=2 should be copied from source."""
        env = self._setup_env()
        vid = env["vnode_id"]

        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 1024,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
            {
                "fid": 2, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 2, "cid": 20, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 2, "cid": 20, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        src_contents = make_source_vnode(
            env["src_data"], vid, fsets, env["dnode_ids"])

        # Pre-populate target with fid=1 files and current.json so retain kicks in
        tgt_tsdb = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}", "tsdb")
        os.makedirs(tgt_tsdb, exist_ok=True)
        # Copy the same fid=1 files to target (simulating existing data)
        for (fid, cid, suffix, mid, lcn), content in src_contents.items():
            if fid == 1:
                fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
                write_file(os.path.join(tgt_tsdb, fname), content)
        # Write local current.json with only fid=1
        local_fsets = [fsets[0]]
        write_file(os.path.join(tgt_tsdb, "current.json"), make_current_json(local_fsets))

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        tdSql.checkEqual(result.returncode, 0, f"taosd failed:\nstdout: {result.stdout}\nstderr: {result.stderr}")

        # Verify retained message in output
        combined = result.stdout + result.stderr
        tdSql.checkEqual("retain" in combined.lower() or "hard-link" in combined.lower(), True,
                         "Expected retain/hard-link mention in output")

        # Verify fid=1 files present with correct content (retained via hard-link)
        for (fid, cid, suffix, mid, lcn), expected in src_contents.items():
            if fid == 1:
                fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
                tgt_path = os.path.join(tgt_tsdb, fname)
                tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing retained file: {fname}")
                tdSql.checkEqual(read_bin(tgt_path), expected, f"Content mismatch (retained): {fname}")

        # Verify fid=2 files present with correct content (copied from source)
        for (fid, cid, suffix, mid, lcn), expected in src_contents.items():
            if fid == 2:
                fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
                tgt_path = os.path.join(tgt_tsdb, fname)
                tdSql.checkEqual(os.path.isfile(tgt_path), True, f"Missing copied file: {fname}")
                tdSql.checkEqual(read_bin(tgt_path), expected, f"Content mismatch (copied): {fname}")

        # Verify current.json has both fids
        current = json.loads(read_file(os.path.join(tgt_tsdb, "current.json")))
        fids = sorted([fs["fid"] for fs in current["fset"]])
        tdSql.checkEqual(fids, [1, 2], "current.json should have both fid=1 and fid=2")

        # Verify .bak cleaned up
        bak_dir = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}.bak")
        tdSql.checkEqual(not os.path.exists(bak_dir), True, "Backup dir should be deleted after success")

    def test_retain_existing_fsets_local(self):
        """Retain existing file sets via hard-link local test

        1. Target already has fid=1 with intact files and local current.json.
        2. Source has fid=1 and fid=2.
        3. After repair, fid=1 is retained (hard-linked from backup), fid=2 is copied.
        4. Both file sets are present in the regenerated current.json.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-8 Bomin Zhang created
        """
        self._do_test_retain_existing_fsets()

    def test_retain_existing_fsets_remote(self):
        """Retain existing file sets via hard-link remote test

        1. Target already has fid=1 with intact files and local current.json.
        2. Source has fid=1 and fid=2 (remote source via SSH).
        3. After repair, fid=1 is retained (hard-linked from backup), fid=2 is copied.
        4. Both file sets are present in the regenerated current.json.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-8 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_retain_existing_fsets(source_host=self.source_host)

    # --- Rollback on failure tests ---

    def _do_test_rollback_on_failure(self, source_host=None):
        """When source files are missing, repair should fail and rollback the target vnode.
        After rollback, vnodeN.bak should be restored to vnodeN."""
        env = self._setup_env()
        vid = env["vnode_id"]

        # Create source vnode with current.json referencing files
        fsets = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 1024,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        src_contents = make_source_vnode(
            env["src_data"], vid, fsets, env["dnode_ids"])

        # Pre-populate target with some data so we can verify rollback restores it
        tgt_vnode = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}")
        tgt_tsdb = os.path.join(tgt_vnode, "tsdb")
        os.makedirs(tgt_tsdb, exist_ok=True)
        sentinel_path = os.path.join(tgt_tsdb, "sentinel.txt")
        write_file(sentinel_path, "original target data")

        # Delete one source TSDB file so copy will fail
        for (fid, cid, suffix, mid, lcn) in list(src_contents.keys()):
            if suffix == "data":
                fname = tsdb_filename(vid, fid, cid, suffix, mid=mid, lcn=lcn)
                src_path = os.path.join(env["src_data"], "vnode", f"vnode{vid}", "tsdb", fname)
                os.remove(src_path)
                break

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], str(vid), source_host=source_host)
        # Should fail (exit code 4 since all vnodes failed)
        tdSql.checkEqual(result.returncode, 4, f"Expected exit code 4, got {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")

        # After rollback, vnodeN should be restored from .bak
        tdSql.checkEqual(os.path.isdir(tgt_vnode), True, "vnode dir should be restored after rollback")
        tdSql.checkEqual(os.path.isfile(sentinel_path), True, "sentinel file should be restored after rollback")
        tdSql.checkEqual(read_file(sentinel_path), "original target data", "sentinel content should match")

        # .bak should be cleaned up after rollback
        bak_dir = os.path.join(env["tgt_data"], "vnode", f"vnode{vid}.bak")
        tdSql.checkEqual(not os.path.exists(bak_dir), True, "Backup dir should be cleaned after rollback")

    def test_rollback_on_failure_local(self):
        """Rollback on copy failure local test

        1. Create source vnode with current.json referencing files.
        2. Delete one source TSDB file to force copy failure.
        3. Pre-populate target with a sentinel file.
        4. Run repair — expect exit code 4 (all vnodes failed).
        5. Verify rollback restores the original target data from vnodeN.bak.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-8 Bomin Zhang created
        """
        self._do_test_rollback_on_failure()

    def test_rollback_on_failure_remote(self):
        """Rollback on copy failure remote test

        1. Create source vnode with current.json referencing files (remote source via SSH).
        2. Delete one source TSDB file to force copy failure.
        3. Pre-populate target with a sentinel file.
        4. Run repair — expect exit code 4 (all vnodes failed).
        5. Verify rollback restores the original target data from vnodeN.bak.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-8 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_rollback_on_failure(source_host=self.source_host)

    # --- Partial failure exit code tests ---

    def _do_test_exit_code_partial_failure(self, source_host=None):
        """Repair 2 vnodes: vnode2 has valid source, vnode5 has broken source.
        Expect exit code 3 (partial failure: some succeeded, some failed)."""
        env = self._setup_env()

        # Create valid source for vnode2
        fsets_v2 = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 10, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 10, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        make_source_vnode(env["src_data"], 2, fsets_v2, env["dnode_ids"])

        # Create broken source for vnode5: current.json references files but delete them
        fsets_v5 = [
            {
                "fid": 1, "last_compact": 0, "last_commit": 0,
                "files": [
                    {"type": 0, "fid": 1, "cid": 30, "size": 256,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                    {"type": 1, "fid": 1, "cid": 30, "size": 512,
                     "did_level": 0, "did_id": 0, "lcn": 0},
                ],
            },
        ]
        v5_contents = make_source_vnode(env["src_data"], 5, fsets_v5, env["dnode_ids"])
        # Delete all TSDB files for vnode5 so copy fails
        for (fid, cid, suffix, mid, lcn) in list(v5_contents.keys()):
            fname = tsdb_filename(5, fid, cid, suffix, mid=mid, lcn=lcn)
            fpath = os.path.join(env["src_data"], "vnode", "vnode5", "tsdb", fname)
            os.remove(fpath)

        result = self._run_repair(env["tgt_cfg_dir"], env["src_cfg"], "2,5", source_host=source_host)
        tdSql.checkEqual(result.returncode, 3, f"Expected exit code 3, got {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")

        # Verify vnode2 was repaired successfully
        tgt_tsdb_v2 = os.path.join(env["tgt_data"], "vnode", "vnode2", "tsdb")
        tdSql.checkEqual(os.path.isfile(os.path.join(tgt_tsdb_v2, "current.json")), True,
                         "vnode2 should have current.json after successful repair")

        # Verify FAILED in output for vnode5
        combined = result.stdout + result.stderr
        tdSql.checkEqual("FAILED" in combined, True, "Expected FAILED mention for vnode5")

    def test_exit_code_partial_failure_local(self):
        """Partial failure exit code (code 3) local test

        1. Create valid source for vnode2 and broken source for vnode5.
        2. Run repair with --vnode 2,5.
        3. Expect exit code 3: vnode2 succeeds, vnode5 fails.
        4. Verify vnode2 is repaired and vnode5 failure is reported.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-8 Bomin Zhang created
        """
        self._do_test_exit_code_partial_failure()

    def test_exit_code_partial_failure_remote(self):
        """Partial failure exit code (code 3) remote test

        1. Create valid source for vnode2 and broken source for vnode5 (remote source via SSH).
        2. Run repair with --vnode 2,5.
        3. Expect exit code 3: vnode2 succeeds, vnode5 fails.
        4. Verify vnode2 is repaired and vnode5 failure is reported.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-8 Bomin Zhang created
        """
        if not self._ssh_ok:
            pytest.skip("passwordless SSH to 127.0.0.1 not available")
        self._do_test_exit_code_partial_failure(source_host=self.source_host)
