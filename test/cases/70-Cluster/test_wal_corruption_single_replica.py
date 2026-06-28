import glob
import os
import shutil
import time

from new_test_framework.utils import tdLog, tdSql, sc, tdDnodes, clusterComCheck


def _alter_wal_delete_on_corruption(value: str) -> None:
    """Change walDeleteOnCorruption via SQL. Persists across dnode restarts."""
    tdSql.execute(f"ALTER ALL DNODES 'walDeleteOnCorruption' '{value}'")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGET_DNODE = 1
WAL_ROLL_PERIOD = 1
WRITES_PER_SEGMENT = 200
ROLL_SLEEP = 1.2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wal_dir(vg_id: int) -> str:
    base = tdDnodes.getDnodeDir(TARGET_DNODE)
    return os.path.join(base, "data", "vnode", f"vnode{vg_id}", "wal")


def _wal_log_files(vg_id: int) -> list:
    return sorted(glob.glob(os.path.join(_wal_dir(vg_id), "*.log")))


def _get_vg_id(db: str) -> int:
    tdSql.query(
        f"select vgroup_id from information_schema.ins_vgroups where db_name='{db}'"
    )
    return int(tdSql.queryResult[0][0])


def _accumulate_wal_files(db: str, vg_id: int, min_files: int = 3) -> None:
    tdSql.execute(f"use {db}")
    tdSql.execute("create table if not exists t1 (ts timestamp, v int)")
    for _ in range(30):
        if len(_wal_log_files(vg_id)) >= min_files:
            return
        for i in range(WRITES_PER_SEGMENT):
            ts = int(time.time() * 1000) + i
            tdSql.execute(f"insert into t1 values ({ts}, {i})")
        time.sleep(ROLL_SLEEP)
    raise RuntimeError(f"Could not accumulate {min_files} WAL files for vg {vg_id}")


def _truncate_log_file(path: str) -> None:
    size = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.truncate(max(0, size // 2))
    tdLog.info(f"Truncated WAL file: {path} (was {size} bytes)")


def _delete_log_file(path: str) -> None:
    os.remove(path)
    tdLog.info(f"Deleted WAL file: {path}")


def _pick_target(files: list, position: str) -> str:
    if position == "first":
        return files[0]
    if position == "last":
        return files[-1]
    return files[len(files) // 2]


def _corrupted_dir_exists(vg_id: int) -> bool:
    parent = os.path.join(
        tdDnodes.getDnodeDir(TARGET_DNODE), "data", "vnode", f"vnode{vg_id}"
    )
    return bool(glob.glob(os.path.join(parent, "wal.corrupted.*")))


def _clean_corrupted_dirs(vg_id: int) -> None:
    parent = os.path.join(
        tdDnodes.getDnodeDir(TARGET_DNODE), "data", "vnode", f"vnode{vg_id}"
    )
    for d in glob.glob(os.path.join(parent, "wal.corrupted.*")):
        shutil.rmtree(d, ignore_errors=True)
        tdLog.info(f"Removed backup dir: {d}")


def _setup_db(db: str) -> None:
    tdSql.execute(f"drop database if exists {db}")
    tdSql.execute(
        f"create database {db} replica 1 vgroups 1 "
        f"wal_level 1 wal_roll_period {WAL_ROLL_PERIOD}"
    )


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestWalCorruptionSingleReplica:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_wal_corruption_single_replica(self):
        """taosd starts normally and queries succeed after WAL corruption
           with walDeleteOnCorruption=true on a single-replica database

        1. Check 1 dnode is ready
        2. ALTER ALL DNODES 'walDeleteOnCorruption' '1'  (ONE SQL change;
           persists via mnode across dnode restarts)
        3. For each position (first/middle/last) and corruption type
           (truncate/delete):
           a. Create single-replica database, accumulate >=3 WAL segments
           b. Stop dnode, corrupt the target WAL file
           c. Restart dnode — assert taosd comes back ready
           d. Assert queries on the database succeed
           e. Drop database, clean up wal.corrupted.* dirs

        Since: v3.3.6.x

        Labels: ci,cluster,integration,functional
        Jira: None

        History:
            - 2026-05-18 GitHub Copilot Created

        """
        clusterComCheck.checkDnodes(1)

        _alter_wal_delete_on_corruption("1")

        positions = ["first", "middle", "last"]
        corruptions = [("truncate", _truncate_log_file), ("delete", _delete_log_file)]

        for position in positions:
            for corruption_name, corruption_fn in corruptions:
                db = f"wal_sr_{position[:2]}_{corruption_name[:3]}"
                tdLog.info(f"position={position} corruption={corruption_name}")

                _setup_db(db)
                vg_id = _get_vg_id(db)
                _accumulate_wal_files(db, vg_id)

                sc.dnodeStop(TARGET_DNODE)
                files = _wal_log_files(vg_id)
                assert len(files) >= 3, f"Expected >=3 WAL files, got {len(files)}"
                corruption_fn(_pick_target(files, position))

                sc.dnodeStart(TARGET_DNODE)
                clusterComCheck.checkDnodes(1)

                tdSql.query(f"select count(*) from {db}.t1")
                tdLog.info(f"row count after recovery: {tdSql.queryResult[0][0]}")

                tdSql.execute(f"drop database if exists {db}")
                _clean_corrupted_dirs(vg_id)

        tdLog.info("All sub-scenarios passed")
