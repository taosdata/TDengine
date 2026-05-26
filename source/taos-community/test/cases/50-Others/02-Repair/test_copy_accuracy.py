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

"""End-to-end data accuracy tests for vnode copy-mode repair.

Validates that after ``taosd -r --mode copy``, the repaired dnode can
serve queries and return results identical to the pre-repair baseline.

CI invocation examples (cases.task):
  # single-node basic
  ,,y,.,./ci/pytest.sh pytest cases/50-Others/02-Repair/test_copy_accuracy.py
  # 3-node, 3 replica
  ,,y,.,./ci/pytest.sh pytest cases/50-Others/02-Repair/test_copy_accuracy.py -N 3 --replica 3
  # 3-node, 3 replica, 2 tiers, 2 disks
  ,,y,.,./ci/pytest.sh pytest cases/50-Others/02-Repair/test_copy_accuracy.py -N 3 --replica 3 -L 2 -D 2
"""

from new_test_framework.utils import (
    tdLog, tdSql, sc, clusterComCheck, clusterDnodes, tdDnodes, epath
)
import glob
import json
import os
import platform
import shutil
import subprocess
import time

import pytest
import taos


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _taosd_bin():
    """Return the taosd binary path."""
    p = tdDnodes.binPath
    if p and os.path.isfile(p):
        return p
    # fallback: alongside other binaries
    candidate = epath.binFile("taosd")
    if os.path.isfile(candidate):
        return candidate
    return None


def _get_data_dirs(dnode_idx):
    """Return list of pure directory paths for a dnode (strip level/primary)."""
    raw = tdDnodes.dnodes[dnode_idx - 1].dataDir
    dirs = []
    for entry in raw:
        # entry may be "/path/to/data00 0 1" or just "/path/to/data"
        dirs.append(entry.split()[0])
    return dirs


def _get_cfg_dir(dnode_idx):
    """Return the cfg directory (not taos.cfg) for a dnode."""
    return tdDnodes.dnodes[dnode_idx - 1].cfgDir


def _get_cfg_path(dnode_idx):
    """Return the taos.cfg path for a dnode."""
    return tdDnodes.dnodes[dnode_idx - 1].cfgPath


def _get_vnode_ids_on_dnode(dnode_idx):
    """Return list of vnode IDs located on the given dnode via SQL."""
    tdSql.query(f"show vnodes on dnode {dnode_idx}")
    return [int(row[1]) for row in tdSql.queryResult]


def _find_vnode_dirs(dnode_idx, vnode_id):
    """Find all directories named vnode{vnode_id} across the dnode's data dirs."""
    dirs = []
    for data_dir in _get_data_dirs(dnode_idx):
        p = os.path.join(data_dir, "vnode", f"vnode{vnode_id}")
        if os.path.isdir(p):
            dirs.append(p)
    return dirs


def _destroy_vnode_data(dnode_idx, vnode_ids):
    """Remove tsdb/ and wal/ content from each vnode to simulate data loss.

    Destroys both tsdb (data files) and wal (write-ahead log) so data can
    ONLY be recovered through the repair tool, not via WAL replay.
    """
    for vid in vnode_ids:
        for vdir in _find_vnode_dirs(dnode_idx, vid):
            for subdir in ("tsdb", "wal"):
                target = os.path.join(vdir, subdir)
                if os.path.isdir(target):
                    shutil.rmtree(target)
                    tdLog.info(f"destroyed {subdir} data: {target}")


def _destroy_vnode_all(dnode_idx, vnode_ids):
    """Remove entire vnode directories to simulate full data loss."""
    for vid in vnode_ids:
        for vdir in _find_vnode_dirs(dnode_idx, vid):
            shutil.rmtree(vdir)
            tdLog.info(f"destroyed vnode dir: {vdir}")


def _run_copy_repair(taosd_bin, target_dnode_idx, source_dnode_idx, vnode_ids,
                     timeout=120):
    """Execute taosd -r --mode copy and return CompletedProcess."""
    target_cfg_dir = _get_cfg_dir(target_dnode_idx)
    source_cfg_path = _get_cfg_path(source_dnode_idx)
    vnode_str = ",".join(str(v) for v in vnode_ids)

    cmd = [
        taosd_bin,
        "-c", target_cfg_dir,
        "-r",
        "--mode", "copy",
        "--node-type", "vnode",
        "--source-cfg", source_cfg_path,
        "--vnode", vnode_str,
    ]
    tdLog.info(f"repair cmd: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    tdLog.info(f"repair stdout: {result.stdout[-500:]}")
    if result.stderr:
        tdLog.info(f"repair stderr: {result.stderr[-500:]}")
    return result


def _snapshot_queries(queries):
    """Execute queries and return list of (sql, result_rows) tuples."""
    results = []
    for sql in queries:
        tdSql.query(sql)
        # Deep copy the result
        rows = [list(row) for row in tdSql.queryResult] if tdSql.queryResult else []
        results.append((sql, rows))
    return results


def _verify_queries(baseline, tag=""):
    """Re-execute baseline queries and assert results match."""
    for sql, expected_rows in baseline:
        tdSql.query(sql)
        actual = [list(row) for row in tdSql.queryResult] if tdSql.queryResult else []
        if len(actual) != len(expected_rows):
            tdLog.exit(f"{tag} row count mismatch for [{sql}]: "
                       f"expected {len(expected_rows)}, got {len(actual)}")
        for i, (exp, act) in enumerate(zip(expected_rows, actual)):
            if exp != act:
                tdLog.exit(f"{tag} row {i} mismatch for [{sql}]: "
                           f"expected {exp}, got {act}")
    tdLog.success(f"{tag} all {len(baseline)} queries verified")


def _reconnect_tdSql(timeout=30):
    """Reconnect tdSql after a dnode restart (stale connection recovery)."""
    try:
        tdSql.close()
    except Exception:
        pass
    cfg_opt = getattr(tdDnodes.sim, 'cfgDir', None) or tdDnodes.sim.cfgPath
    for i in range(timeout):
        try:
            conn = taos.connect(host="localhost", config=cfg_opt)
            tdSql.init(conn.cursor(), False)
            return
        except Exception:
            time.sleep(1)
    tdLog.exit(f"failed to reconnect tdSql within {timeout}s")


def _wait_db_ready(db, timeout=60):
    """Wait until all vgroups in the db have a leader."""
    for _ in range(timeout):
        try:
            tdSql.query(f"show {db}.vgroups")
        except Exception:
            time.sleep(1)
            continue
        if tdSql.queryResult:
            all_ok = True
            for row in tdSql.queryResult:
                # row structure varies; check that 'leader' appears somewhere
                row_str = str(row)
                if "leader" not in row_str:
                    all_ok = False
                    break
            if all_ok:
                return True
        time.sleep(1)
    tdLog.exit(f"db {db} not ready within {timeout}s")


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestCopyAccuracy:
    """End-to-end data accuracy verification after copy-mode repair."""

    # Attributes injected by conftest.py before_test_class:
    #   self.dnodeNum, self.mnodeNum, self.mLevel, self.mLevelDisk
    #   self.replicaVar

    updatecfgDict = {
        "countAlwaysReturnValue": "1",
    }

    DB = "test_repair"

    @classmethod
    def setup_class(cls):
        if platform.system() == "Windows":
            pytest.skip("copy-mode repair is not supported on Windows")
        cls.taosd_bin = _taosd_bin()
        if cls.taosd_bin is None:
            pytest.skip("taosd binary not found")

    def _need_cluster(self, min_dnodes=3, min_replica=3):
        dn = getattr(self, "dnodeNum", 1)
        rv = getattr(self, "replicaVar", 1)
        if dn < min_dnodes or rv < min_replica:
            pytest.skip(f"requires -N {min_dnodes} --replica {min_replica}, "
                        f"got -N {dn} --replica {rv}")

    def _need_multi_level(self, min_level=2):
        lv = getattr(self, "mLevel", 1)
        if lv < min_level:
            pytest.skip(f"requires -L {min_level}, got -L {lv}")

    def _need_multi_disk(self, min_disk=2):
        dk = getattr(self, "mLevelDisk", 1)
        if dk < min_disk:
            pytest.skip(f"requires -D {min_disk}, got -D {dk}")

    def _create_db(self, db, vgroups=2, replica=None, extra=""):
        """Drop (if exists) and create a database, waiting for readiness."""
        rv = replica if replica is not None else getattr(self, "replicaVar", 1)
        cursor = tdSql.cursor
        cursor.execute(f"drop database if exists {db}")
        time.sleep(2)
        sql = f"create database {db} vgroups {vgroups} replica {rv} {extra}".strip()
        for attempt in range(60):
            try:
                cursor.execute(sql)
                break
            except Exception as e:
                msg = str(e).lower()
                if "creating" in msg or "already exists" in msg:
                    time.sleep(1)
                else:
                    raise
        _wait_db_ready(db)
        tdSql.execute(f"use {db}")

    # -- pick source/target dnode for repair --

    def _pick_repair_pair(self, db):
        """Choose (target_dnode, source_dnode) for repair.

        Picks a non-leader dnode that holds vnodes for db as target,
        and another dnode that also holds vnodes as source.
        Returns (target_idx, source_idx, vnode_ids_on_target).
        """
        # Find which dnodes hold vnodes for this db
        tdSql.query(f"show {db}.vgroups")
        # Collect all dnode IDs from vgroup info
        dnode_vnode_map = {}  # dnode_idx -> list of vnode_ids
        for row in tdSql.queryResult:
            vgroup_id = int(row[0])
            # In 3-replica setup, columns 3,5,7 have dnode IDs; 4,6,8 have roles
            # But column layout can vary. Use show vnodes instead.
            pass

        # Simpler approach: just use show vnodes
        dn = getattr(self, "dnodeNum", 1)
        for dnode_idx in range(1, dn + 1):
            vids = _get_vnode_ids_on_dnode(dnode_idx)
            if vids:
                dnode_vnode_map[dnode_idx] = vids

        if len(dnode_vnode_map) < 2:
            tdLog.exit("need at least 2 dnodes with vnodes for repair test")

        # Pick the last dnode as target, first as source
        target = max(dnode_vnode_map.keys())
        source = min(dnode_vnode_map.keys())
        return target, source, dnode_vnode_map[target]

    # -- baseline query sets --

    def _basic_queries(self, db, tb):
        return [
            f"select count(*) from {db}.{tb}",
            f"select first(ts) from {db}.{tb}",
            f"select last(ts) from {db}.{tb}",
            f"select min(v1) from {db}.{tb}",
            f"select max(v1) from {db}.{tb}",
            f"select sum(v1) from {db}.{tb}",
        ]

    def _stb_queries(self, db, stb):
        return [
            f"select count(*) from {db}.{stb}",
            f"select first(ts) from {db}.{stb}",
            f"select last(ts) from {db}.{stb}",
            f"select min(v_int) from {db}.{stb}",
            f"select max(v_int) from {db}.{stb}",
            f"select sum(v_int) from {db}.{stb}",
            f"select avg(v_float) from {db}.{stb}",
            f"select count(*) from {db}.{stb} group by tbname order by tbname",
            f"select last(v_binary) from {db}.{stb}",
        ]

    # -----------------------------------------------------------------------
    # Single-node tests (run with any -N)
    # -----------------------------------------------------------------------

    def _do_single_node_repair(self, db, setup_fn, query_fn, flush=True,
                               compact=False):
        """Generic single-node repair flow.

        For single-node (no replica), we:
        1. Write data
        2. Flush to disk
        3. Snapshot queries
        4. Stop taosd
        5. Destroy tsdb data for all vnodes on dnode 1
        6. Copy-repair from dnode 1 to dnode 1 (self-repair using backup)

        Since single-node self-repair doesn't have a healthy source, we
        instead backup the vnode data before destroying, and use the backup
        as source. This mimics restoring from a known-good snapshot.

        Actually, for single-node tests we will create a "source" config
        pointing to a backup directory, then repair the real dnode from it.
        """
        dn = getattr(self, "dnodeNum", 1)
        rv = getattr(self, "replicaVar", 1)

        # If we have a cluster, delegate to cluster repair flow
        if dn >= 3 and rv >= 3:
            return self._do_cluster_repair(db, setup_fn, query_fn,
                                           flush=flush, compact=compact)

        # Single-node flow: backup-based repair
        setup_fn(db)

        if flush:
            tdSql.execute(f"flush database {db}")
            time.sleep(2)

        if compact:
            tdSql.execute(f"compact database {db}")
            time.sleep(5)

        queries = query_fn(db)
        baseline = _snapshot_queries(queries)

        # Find a dnode that actually has vnodes (not necessarily dnode 1)
        target_dnode = None
        for d in range(1, dn + 1):
            ids = _get_vnode_ids_on_dnode(d)
            if ids:
                target_dnode = d
                vnode_ids = ids
                break
        if target_dnode is None:
            tdLog.exit("no vnodes found on any dnode")

        # Backup vnode data before destroying
        data_dirs = _get_data_dirs(target_dnode)
        backup_base = os.path.join(os.path.dirname(data_dirs[0]), "data_backup")
        if os.path.exists(backup_base):
            shutil.rmtree(backup_base)
        for data_dir in data_dirs:
            vnode_base = os.path.join(data_dir, "vnode")
            if os.path.isdir(vnode_base):
                dst = os.path.join(backup_base, "vnode")
                shutil.copytree(vnode_base, dst, dirs_exist_ok=True)
        tdLog.info(f"backed up vnode data to {backup_base}")

        # Create a source config pointing to backup
        cfg_dir = _get_cfg_dir(target_dnode)
        src_cfg_dir = os.path.join(os.path.dirname(cfg_dir), "cfg_src")
        os.makedirs(src_cfg_dir, exist_ok=True)
        # Read existing taos.cfg and replace dataDir lines
        with open(_get_cfg_path(target_dnode), "r") as f:
            cfg_content = f.read()
        new_lines = []
        for line in cfg_content.splitlines():
            if line.strip().startswith("dataDir"):
                # Replace with backup path
                new_lines.append(f"dataDir {backup_base} 0 1")
            else:
                new_lines.append(line)
        # Remove duplicate dataDir if multi-level was configured
        seen_data = False
        final_lines = []
        for line in new_lines:
            if line.strip().startswith("dataDir"):
                if seen_data:
                    continue
                seen_data = True
            final_lines.append(line)
        src_cfg_path = os.path.join(src_cfg_dir, "taos.cfg")
        with open(src_cfg_path, "w") as f:
            f.write("\n".join(final_lines) + "\n")

        # Stop taosd
        sc.dnodeStop(target_dnode)
        time.sleep(2)

        # Destroy tsdb data
        _destroy_vnode_data(target_dnode, vnode_ids)

        # Run copy repair from backup source
        cmd = [
            self.taosd_bin,
            "-c", cfg_dir,
            "-r",
            "--mode", "copy",
            "--node-type", "vnode",
            "--source-cfg", src_cfg_path,
            "--vnode", ",".join(str(v) for v in vnode_ids),
        ]
        tdLog.info(f"single-node repair cmd: {' '.join(cmd)}")
        print(f"[REPAIR CMD] {' '.join(cmd)}", flush=True)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        tdLog.info(f"repair rc={result.returncode}")
        print(f"[REPAIR RC] {result.returncode}", flush=True)
        if result.stdout:
            tdLog.info(f"repair stdout: {result.stdout[-500:]}")
            print(f"[REPAIR STDOUT] {result.stdout[-1000:]}", flush=True)
        if result.stderr:
            tdLog.info(f"repair stderr: {result.stderr[-500:]}")
            print(f"[REPAIR STDERR] {result.stderr[-1000:]}", flush=True)
        tdSql.checkEqual(result.returncode, 0,
                         f"single-node repair failed: {result.stderr}")

        # Restart taosd
        sc.dnodeStart(target_dnode)
        _reconnect_tdSql()
        clusterComCheck.checkDnodes(dn)
        time.sleep(3)

        # Verify data
        _verify_queries(baseline, tag="single-node")

        # Cleanup backup
        shutil.rmtree(backup_base, ignore_errors=True)
        shutil.rmtree(src_cfg_dir, ignore_errors=True)

    # -----------------------------------------------------------------------
    # Cluster repair flow (3-node, 3-replica)
    # -----------------------------------------------------------------------

    def _do_cluster_repair(self, db, setup_fn, query_fn, flush=True,
                           compact=False, write_during_down=False,
                           extra_write_fn=None, extra_query_fn=None):
        """Generic cluster repair flow.

        1. setup_fn(db) — create db/tables, insert data
        2. flush / compact
        3. snapshot baseline queries
        4. pick target dnode, stop it
        5. optionally write more data while target is down
        6. destroy target's vnode data
        7. copy-repair from source dnode
        8. restart target
        9. verify queries match (with optional extra data)
        """
        setup_fn(db)

        if flush:
            tdSql.execute(f"flush database {db}")
            time.sleep(2)

        if compact:
            tdSql.execute(f"compact database {db}")
            time.sleep(5)

        queries = query_fn(db)
        baseline = _snapshot_queries(queries)

        target, source, vnode_ids = self._pick_repair_pair(db)
        tdLog.info(f"repair: target=dnode{target}, source=dnode{source}, "
                   f"vnodes={vnode_ids}")

        # Stop target dnode
        sc.dnodeStop(target)
        time.sleep(3)

        # Optionally write more data while target is down
        if write_during_down and extra_write_fn:
            extra_write_fn(db)
            tdSql.execute(f"flush database {db}")
            time.sleep(2)
            # Update baseline with new data
            if extra_query_fn:
                queries = extra_query_fn(db)
            baseline = _snapshot_queries(queries)

        # Destroy target vnode data
        _destroy_vnode_data(target, vnode_ids)

        # Run copy repair
        result = _run_copy_repair(
            self.taosd_bin, target, source, vnode_ids
        )
        tdSql.checkEqual(result.returncode, 0,
                         f"repair failed: {result.stderr}")

        # Restart target
        sc.dnodeStart(target)
        dn = getattr(self, "dnodeNum", 1)
        _reconnect_tdSql()
        clusterComCheck.checkDnodes(dn)
        _wait_db_ready(db)
        time.sleep(3)

        # Verify data
        _verify_queries(baseline, tag=f"cluster(target=dnode{target})")

    # -----------------------------------------------------------------------
    # Test cases
    # -----------------------------------------------------------------------

    def test_basic_accuracy(self):
        """Basic data accuracy after copy repair

        1. Create database with a normal table, insert rows.
        2. Flush database to disk.
        3. Snapshot baseline query results.
        4. Stop target dnode, destroy its vnode tsdb data.
        5. Run copy-mode repair from source dnode.
        6. Restart target dnode.
        7. Verify query results match baseline.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=2)
            tdSql.execute(f"create table {db}.t1 (ts timestamp, v1 int, "
                          f"v2 float, v3 binary(32))")
            for i in range(1000):
                tdSql.execute(
                    f"insert into {db}.t1 values "
                    f"(now - {1000 - i}s, {i}, {i * 0.5}, 'row_{i}')"
                )

        def queries(db):
            return [
                f"select count(*) from {db}.t1",
                f"select first(ts) from {db}.t1",
                f"select last(ts) from {db}.t1",
                f"select min(v1) from {db}.t1",
                f"select max(v1) from {db}.t1",
                f"select sum(v1) from {db}.t1",
                f"select last(v3) from {db}.t1",
            ]

        self._do_single_node_repair(db, setup, queries)

    def test_supertable_accuracy(self):
        """Supertable with multiple child tables accuracy after copy repair

        1. Create database with a supertable and 10 child tables.
        2. Insert rows into each child table.
        3. Flush database, snapshot baseline queries (count, aggregates, group by).
        4. Repair and verify.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=2)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v_int int, v_float float, v_binary binary(64)) "
                f"tags (t_id int, t_name binary(32))"
            )
            for t in range(10):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb "
                    f"tags ({t}, 'tag_{t}')"
                )
                values = []
                for i in range(500):
                    values.append(
                        f"(now - {500 - i}s, {t * 1000 + i}, "
                        f"{(t * 1000 + i) * 0.1}, 'ct{t}_row{i}')"
                    )
                    if len(values) >= 100:
                        tdSql.execute(
                            f"insert into {db}.ct{t} values " + " ".join(values)
                        )
                        values = []
                if values:
                    tdSql.execute(
                        f"insert into {db}.ct{t} values " + " ".join(values)
                    )

        def queries(db):
            return [
                f"select count(*) from {db}.stb",
                f"select min(v_int) from {db}.stb",
                f"select max(v_int) from {db}.stb",
                f"select sum(v_int) from {db}.stb",
                f"select avg(v_float) from {db}.stb",
                f"select count(*) from {db}.stb group by tbname order by tbname",
                f"select last(v_binary) from {db}.stb",
                f"select first(ts) from {db}.stb",
                f"select last(ts) from {db}.stb",
            ]

        self._do_single_node_repair(db, setup, queries)

    def test_null_and_types_accuracy(self):
        """Multiple data types and NULL values accuracy after copy repair

        1. Create table with all column types.
        2. Insert rows including NULL values.
        3. Repair and verify type precision and NULL semantics are preserved.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=2)
            tdSql.execute(
                f"create table {db}.t_types ("
                f"ts timestamp, "
                f"c_tinyint tinyint, "
                f"c_smallint smallint, "
                f"c_int int, "
                f"c_bigint bigint, "
                f"c_utinyint tinyint unsigned, "
                f"c_usmallint smallint unsigned, "
                f"c_uint int unsigned, "
                f"c_ubigint bigint unsigned, "
                f"c_float float, "
                f"c_double double, "
                f"c_bool bool, "
                f"c_binary binary(64), "
                f"c_nchar nchar(64), "
                f"c_varbinary varbinary(64)"
                f")"
            )
            # Insert normal rows
            for i in range(200):
                tdSql.execute(
                    f"insert into {db}.t_types values "
                    f"(now - {200 - i}s, "
                    f"{i % 127}, {i}, {i * 10}, {i * 100}, "
                    f"{i % 255}, {i}, {i * 10}, {i * 100}, "
                    f"{i * 1.5}, {i * 2.5}, {i % 2 == 0}, "
                    f"'bin_{i}', '中文_{i}', '\\x{i:04x}')"
                )
            # Insert rows with NULLs
            for i in range(50):
                tdSql.execute(
                    f"insert into {db}.t_types values "
                    f"(now - {250 + i}s, "
                    f"NULL, NULL, NULL, NULL, "
                    f"NULL, NULL, NULL, NULL, "
                    f"NULL, NULL, NULL, "
                    f"NULL, NULL, NULL)"
                )

        def queries(db):
            return [
                f"select count(*) from {db}.t_types",
                f"select count(c_int) from {db}.t_types",
                f"select count(c_binary) from {db}.t_types",
                f"select min(c_int) from {db}.t_types",
                f"select max(c_bigint) from {db}.t_types",
                f"select sum(c_double) from {db}.t_types",
                f"select avg(c_float) from {db}.t_types",
                f"select first(c_nchar) from {db}.t_types",
                f"select last(c_binary) from {db}.t_types",
            ]

        self._do_single_node_repair(db, setup, queries)

    def test_large_batch_accuracy(self):
        """Large batch (10M rows) accuracy after copy repair

        1. Insert 10,000,000 rows across 50 child tables.
        2. Flush to ensure data is fully on disk in tsdb files.
        3. Repair and verify count/sum/min/max/first/last match.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
            - 2026-5-19 Bomin Zhang increased to 10M rows
        """
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=4)
            tdSql.execute(
                f"create stable {db}.meters "
                f"(ts timestamp, current float, voltage int, phase float) "
                f"tags (location binary(64), group_id int)"
            )
            # 50 child tables × 200,000 rows = 10,000,000 total rows
            num_tables = 50
            rows_per_table = 200000
            batch_size = 5000
            for t in range(num_tables):
                tdSql.execute(
                    f"create table {db}.d{t} using {db}.meters "
                    f"tags ('location_{t:04d}', {t})"
                )
                for batch_start in range(0, rows_per_table, batch_size):
                    values = []
                    batch_end = min(batch_start + batch_size, rows_per_table)
                    for i in range(batch_start, batch_end):
                        ts_offset = t * rows_per_table + i
                        values.append(
                            f"(now - {num_tables * rows_per_table - ts_offset}s, "
                            f"{(i % 30) + 0.5}, {200 + i % 20}, "
                            f"{(i % 360) * 0.01})"
                        )
                    tdSql.execute(
                        f"insert into {db}.d{t} values " + " ".join(values)
                    )
                if (t + 1) % 10 == 0:
                    tdLog.info(f"inserted {(t+1) * rows_per_table} rows "
                               f"({t+1}/{num_tables} tables)")

        def queries(db):
            return [
                f"select count(*) from {db}.meters",
                f"select sum(current) from {db}.meters",
                f"select avg(current) from {db}.meters",
                f"select min(voltage) from {db}.meters",
                f"select max(voltage) from {db}.meters",
                f"select first(ts) from {db}.meters",
                f"select last(ts) from {db}.meters",
                f"select count(*) from {db}.meters group by group_id order by group_id",
                f"select sum(voltage) from {db}.meters where voltage > 210",
                f"select count(*), avg(current) from {db}.meters "
                f"group by location order by location",
            ]

        self._do_single_node_repair(db, setup, queries)

    def test_after_flush_accuracy(self):
        """Data accuracy after flush + copy repair

        1. Insert data, explicitly flush database.
        2. Repair and verify data files (head/data/sma) were copied correctly.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=2)
            tdSql.execute(f"create table {db}.t_flush "
                          f"(ts timestamp, v1 int, v2 double)")
            for i in range(2000):
                tdSql.execute(
                    f"insert into {db}.t_flush values "
                    f"(now - {2000 - i}s, {i}, {i * 3.14})"
                )

        def queries(db):
            return [
                f"select count(*) from {db}.t_flush",
                f"select sum(v1) from {db}.t_flush",
                f"select avg(v2) from {db}.t_flush",
                f"select first(ts) from {db}.t_flush",
                f"select last(ts) from {db}.t_flush",
            ]

        self._do_single_node_repair(db, setup, queries, flush=True)

    def test_compact_then_repair(self):
        """Data accuracy after compact + copy repair

        1. Insert data, flush, then compact database.
        2. Repair and verify compacted data files are correctly copied.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=2)
            tdSql.execute(f"create table {db}.t_compact "
                          f"(ts timestamp, v1 int, v2 binary(32))")
            for i in range(2000):
                tdSql.execute(
                    f"insert into {db}.t_compact values "
                    f"(now - {2000 - i}s, {i}, 'val_{i}')"
                )

        def queries(db):
            return [
                f"select count(*) from {db}.t_compact",
                f"select sum(v1) from {db}.t_compact",
                f"select min(v1) from {db}.t_compact",
                f"select max(v1) from {db}.t_compact",
                f"select last(v2) from {db}.t_compact",
            ]

        self._do_single_node_repair(db, setup, queries,
                                    flush=True, compact=True)

    # -----------------------------------------------------------------------
    # Cluster-specific tests (require -N 3 --replica 3)
    # -----------------------------------------------------------------------

    def test_replica3_single_vnode_repair(self):
        """3-replica cluster: repair single dnode's vnodes

        1. Create 3-replica database, insert data.
        2. Flush database, snapshot baseline queries.
        3. Stop one dnode, destroy its vnode tsdb data.
        4. Copy-repair from a healthy dnode.
        5. Restart the repaired dnode.
        6. Verify all queries return identical results.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        self._need_cluster()
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=4, replica=3)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v int, s binary(32)) "
                f"tags (t_id int)"
            )
            for t in range(5):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb tags ({t})"
                )
                for batch_start in range(0, 1000, 200):
                    values = []
                    for i in range(batch_start, min(batch_start + 200, 1000)):
                        values.append(
                            f"(now - {1000 - i}s, {t * 1000 + i}, 'r{i}')"
                        )
                    tdSql.execute(
                        f"insert into {db}.ct{t} values " + " ".join(values)
                    )

        def queries(db):
            return self._stb_queries(db, "stb")

        self._do_cluster_repair(db, setup, queries)

    def test_replica3_write_during_down(self):
        """3-replica cluster: write data while target dnode is down, then repair

        1. Create 3-replica database, insert initial data.
        2. Stop target dnode.
        3. Insert additional data (2/3 replicas still writable).
        4. Copy-repair the stopped dnode from a healthy source.
        5. Restart repaired dnode.
        6. Verify both old and new data are queryable.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        self._need_cluster()
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=2, replica=3)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v int) tags (t_id int)"
            )
            for t in range(5):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb tags ({t})"
                )
                values = []
                for i in range(500):
                    values.append(f"(now - {1000 - i}s, {t * 500 + i})")
                    if len(values) >= 100:
                        tdSql.execute(
                            f"insert into {db}.ct{t} values "
                            + " ".join(values)
                        )
                        values = []
                if values:
                    tdSql.execute(
                        f"insert into {db}.ct{t} values " + " ".join(values)
                    )

        def extra_write(db):
            for t in range(5):
                values = []
                for i in range(200):
                    values.append(f"(now + {i}s, {90000 + t * 200 + i})")
                tdSql.execute(
                    f"insert into {db}.ct{t} values " + " ".join(values)
                )

        def queries(db):
            return [
                f"select count(*) from {db}.stb",
                f"select sum(v) from {db}.stb",
                f"select min(v) from {db}.stb",
                f"select max(v) from {db}.stb",
                f"select first(ts) from {db}.stb",
                f"select last(ts) from {db}.stb",
                f"select count(*) from {db}.stb group by tbname order by tbname",
            ]

        self._do_cluster_repair(
            db, setup, queries,
            write_during_down=True,
            extra_write_fn=extra_write,
            extra_query_fn=queries,
        )

    def test_replica3_all_vgroups_repair(self):
        """3-replica cluster: repair all vnodes on a dnode across multiple vgroups

        1. Create database with multiple vgroups.
        2. Repair all vnodes at once (--vnode list).
        3. Verify full data accuracy across all vgroups.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        self._need_cluster()
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=8, replica=3)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v int, s binary(16)) tags (t_id int)"
            )
            for t in range(20):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb tags ({t})"
                )
                values = []
                for i in range(200):
                    values.append(
                        f"(now - {200 - i}s, {t * 200 + i}, 'r{i}')"
                    )
                tdSql.execute(
                    f"insert into {db}.ct{t} values " + " ".join(values)
                )

        def queries(db):
            return self._stb_queries(db, "stb")

        self._do_cluster_repair(db, setup, queries)

    def test_replica3_multiple_vnodes_repair(self):
        """3-replica cluster: repair specific vnodes with comma-separated list

        1. Create database with many vgroups, generating multiple vnodes per dnode.
        2. Repair using --vnode list with multiple IDs.
        3. Verify data accuracy for all tables.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        self._need_cluster()
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=6, replica=3)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v int) tags (t_id int)"
            )
            for t in range(12):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb tags ({t})"
                )
                values = []
                for i in range(300):
                    values.append(f"(now - {300 - i}s, {t * 300 + i})")
                    if len(values) >= 100:
                        tdSql.execute(
                            f"insert into {db}.ct{t} values "
                            + " ".join(values)
                        )
                        values = []
                if values:
                    tdSql.execute(
                        f"insert into {db}.ct{t} values " + " ".join(values)
                    )

        def queries(db):
            return [
                f"select count(*) from {db}.stb",
                f"select sum(v) from {db}.stb",
                f"select count(*) from {db}.stb group by tbname order by tbname",
            ]

        self._do_cluster_repair(db, setup, queries)

    # -----------------------------------------------------------------------
    # Multi-level / multi-disk tests (require -L / -D options)
    # -----------------------------------------------------------------------

    def test_multi_level_accuracy(self):
        """Multi-level storage accuracy after copy repair

        1. With -L 2 (2 storage tiers), create database, insert data.
        2. Repair and verify data accuracy — files across tiers are
           correctly copied to the target.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        self._need_cluster()
        self._need_multi_level(2)
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=4, replica=3)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v int, s binary(32)) tags (t_id int)"
            )
            for t in range(10):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb tags ({t})"
                )
                values = []
                for i in range(500):
                    values.append(
                        f"(now - {500 - i}s, {t * 500 + i}, 'v{i}')"
                    )
                    if len(values) >= 100:
                        tdSql.execute(
                            f"insert into {db}.ct{t} values "
                            + " ".join(values)
                        )
                        values = []
                if values:
                    tdSql.execute(
                        f"insert into {db}.ct{t} values " + " ".join(values)
                    )

        def queries(db):
            return self._stb_queries(db, "stb")

        self._do_cluster_repair(db, setup, queries)

    def test_multi_disk_accuracy(self):
        """Multi-disk storage accuracy after copy repair

        1. With -D 2 (2 disks per tier), create database, insert data.
        2. Repair and verify data accuracy — round-robin disk distribution
           is handled correctly.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        self._need_cluster()
        self._need_multi_disk(2)
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=4, replica=3)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v int, s binary(16)) tags (t_id int)"
            )
            for t in range(10):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb tags ({t})"
                )
                values = []
                for i in range(500):
                    values.append(
                        f"(now - {500 - i}s, {t * 500 + i}, 'v{i}')"
                    )
                    if len(values) >= 100:
                        tdSql.execute(
                            f"insert into {db}.ct{t} values "
                            + " ".join(values)
                        )
                        values = []
                if values:
                    tdSql.execute(
                        f"insert into {db}.ct{t} values " + " ".join(values)
                    )

        def queries(db):
            return self._stb_queries(db, "stb")

        self._do_cluster_repair(db, setup, queries)

    def test_multi_level_multi_disk_accuracy(self):
        """Multi-level + multi-disk storage accuracy after copy repair

        1. With -L 2 -D 2 (2 tiers x 2 disks), create database, insert data.
        2. Repair and verify data accuracy — tier folding and disk round-robin
           are both handled correctly.

        Catalog:
            - Others:RepairCopy

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-18 Bomin Zhang created
        """
        self._need_cluster()
        self._need_multi_level(2)
        self._need_multi_disk(2)
        db = self.DB

        def setup(db):
            self._create_db(db, vgroups=4, replica=3)
            tdSql.execute(
                f"create stable {db}.stb "
                f"(ts timestamp, v int, v2 double, s nchar(32)) "
                f"tags (t_id int, t_loc binary(32))"
            )
            for t in range(10):
                tdSql.execute(
                    f"create table {db}.ct{t} using {db}.stb "
                    f"tags ({t}, 'loc_{t}')"
                )
                values = []
                for i in range(500):
                    values.append(
                        f"(now - {500 - i}s, {t * 500 + i}, "
                        f"{(t * 500 + i) * 0.01}, '数据_{i}')"
                    )
                    if len(values) >= 100:
                        tdSql.execute(
                            f"insert into {db}.ct{t} values "
                            + " ".join(values)
                        )
                        values = []
                if values:
                    tdSql.execute(
                        f"insert into {db}.ct{t} values " + " ".join(values)
                    )

        def queries(db):
            return [
                f"select count(*) from {db}.stb",
                f"select sum(v) from {db}.stb",
                f"select avg(v2) from {db}.stb",
                f"select min(v) from {db}.stb",
                f"select max(v) from {db}.stb",
                f"select first(ts) from {db}.stb",
                f"select last(ts) from {db}.stb",
                f"select last(s) from {db}.stb",
                f"select count(*) from {db}.stb group by tbname order by tbname",
            ]

        self._do_cluster_repair(db, setup, queries)
