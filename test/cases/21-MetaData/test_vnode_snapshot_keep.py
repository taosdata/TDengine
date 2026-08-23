import json
import os
import shutil
import time

from new_test_framework.utils import clusterComCheck, sc, tdCom, tdDnodes, tdLog, tdSql


class TestVnodeSnapshotKeep:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _get_vgroup_leader(self, db_name, vg_id, timeout=30):
        """Return the dnode ID that currently leads the specified vgroup."""
        for attempt in range(timeout):
            tdSql.query(f"show {db_name}.vgroups")
            for row in tdSql.queryResult:
                if row[0] != vg_id:
                    continue
                for index, value in enumerate(row):
                    if value == "leader":
                        leader = row[index - 1]
                        tdLog.info(
                            f"vgroup {vg_id} leader is dnode {leader} after "
                            f"{attempt + 1}s"
                        )
                        return leader
            time.sleep(1)
        raise RuntimeError(f"No leader found for vgroup {vg_id} in {db_name}")

    def _vnode_json_path(self, dnode_id, vg_id):
        """Build the persisted vnode configuration path for a dnode/vgroup."""
        return os.path.join(
            tdDnodes.getDnodeDir(dnode_id),
            "data",
            "vnode",
            f"vnode{vg_id}",
            "vnode.json",
        )

    def _check_persisted_keep(self, dnode_id, vg_id, expected_keep):
        """Assert that vnode.json contains the snapshot-restored KEEP values in minutes."""
        vnode_json = self._vnode_json_path(dnode_id, vg_id)
        for attempt in range(30):
            try:
                with open(vnode_json, encoding="utf-8") as config_file:
                    config = json.load(config_file)["config"]
                actual_keep = tuple(
                    int(config[key]) for key in ("keep0", "keep1", "keep2")
                )
                tdLog.info(
                    f"dnode {dnode_id} vnode {vg_id} persisted KEEP is "
                    f"{actual_keep} minutes (attempt {attempt + 1})"
                )
                if actual_keep != expected_keep:
                    error = (
                        f"dnode {dnode_id} vnode {vg_id} KEEP {actual_keep} != "
                        f"expected {expected_keep}"
                    )
                else:
                    return
            except (FileNotFoundError, json.JSONDecodeError, KeyError) as error:
                error = f"Could not read vnode configuration {vnode_json}: {error}"

            if attempt == 29:
                raise AssertionError(error)
            tdLog.info(
                f"Waiting for vnode KEEP verification: {error} "
                f"(attempt {attempt + 1}/30)"
            )
            time.sleep(1)

    def _remove_follower_wal(self, dnode_id, vg_id):
        """Remove only the stopped follower WAL to require snapshot recovery."""
        wal_dir = os.path.join(
            tdDnodes.getDnodeDir(dnode_id),
            "data",
            "vnode",
            f"vnode{vg_id}",
            "wal",
        )
        assert os.path.isdir(wal_dir), f"Follower WAL directory does not exist: {wal_dir}"
        shutil.rmtree(wal_dir)
        tdLog.info(
            f"Removed follower dnode {dnode_id} WAL directory {wal_dir} to force snapshot"
        )

    def _check_database_keep(self, db_name, expected_keep):
        """Assert the catalog reports the KEEP values committed by ALTER DATABASE in days."""
        tdSql.query(
            "select `keep` from information_schema.ins_databases "
            f"where name = '{db_name}'"
        )
        tdSql.checkRows(1)
        actual_keep = tuple(
            int(value.rstrip("d")) for value in tdSql.queryResult[0][0].split(",")
        )
        tdLog.info(f"Database {db_name} catalog KEEP is {actual_keep} days")
        assert actual_keep == expected_keep, (
            f"Database {db_name} KEEP {actual_keep} != expected {expected_keep}"
        )

    def do_snapshot_keep_recovery(self):
        db_name = "snap_keep"
        expected_keep_days = (60, 60, 60)
        expected_keep_minutes = tuple(days * 24 * 60 for days in expected_keep_days)
        follower_dnode = None
        tdLog.info(
            f"Expected KEEP is {expected_keep_days} days and "
            f"{expected_keep_minutes} minutes in vnode.json"
        )

        clusterComCheck.checkDnodes(4)
        # Keep dnode 1 out of vnode placement so the stopped follower is never dnode 1.
        tdLog.info("Disabling vnode placement on dnode 1")
        tdSql.execute("alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(
            f"create database {db_name} vgroups 1 replica 3 "
            "wal_level 1 wal_retention_period 0 wal_roll_period 1"
        )
        try:
            tdSql.execute(f"create table {db_name}.t1 (ts timestamp, v int)")
            tdSql.query(f"show {db_name}.vgroups")
            tdSql.checkRows(1)
            vg_id = tdSql.queryResult[0][0]
            leader_dnode = self._get_vgroup_leader(db_name, vg_id)
            assert leader_dnode in (2, 3), (
                f"Unexpected vgroup leader dnode {leader_dnode}; "
                "dnode 1 must not host vnodes"
            )
            follower_dnode = 3 if leader_dnode == 2 else 2

            tdLog.info(f"Stopping follower dnode {follower_dnode}")
            sc.dnodeForceStop(follower_dnode)
            clusterComCheck.checkDnodes(3)

            tdLog.info(f"Starting to alter database {db_name}")

            # ALTER is committed while the follower is unavailable.
            tdSql.execute(f"alter database {db_name} keep 60")
            self._check_database_keep(db_name, expected_keep_days)

            # Commit data on the leader before deleting the follower WAL. This
            # guarantees the restarted follower needs the leader's latest vnode state.
            leader_sql = tdCom.newTdSql(port=6030 + (leader_dnode - 1) * 100)
            try:
                leader_sql.execute(f"use {db_name}")
                now_ms = int(time.time() * 1000)
                for batch in range(10):
                    values = ",".join(
                        f"({now_ms + batch * 100 + row}, {row})" for row in range(100)
                    )
                    leader_sql.execute(f"insert into t1 values {values}")
                leader_sql.execute(f"flush database {db_name}")
            finally:
                leader_sql.close()

            self._remove_follower_wal(follower_dnode, vg_id)
            tdLog.info(f"Restarting follower dnode {follower_dnode} for snapshot recovery")
            sc.dnodeStart(follower_dnode)
            follower_dnode = None
            clusterComCheck.checkDnodes(4, timeout=60)

            # The file-level assertion detects the original bug, where the receiver
            # overwrote the leader snapshot configuration with its stale vnode config.
            self._check_persisted_keep(
                3 if leader_dnode == 2 else 2, vg_id, expected_keep_minutes
            )
            self._check_database_keep(db_name, expected_keep_days)

            # A second restart proves that the restored configuration was committed.
            restarted_dnode = 3 if leader_dnode == 2 else 2
            sc.dnodeStop(restarted_dnode)
            sc.dnodeStart(restarted_dnode)
            clusterComCheck.checkDnodes(4, timeout=60)
            self._check_persisted_keep(restarted_dnode, vg_id, expected_keep_minutes)
        finally:
            tdLog.info(f"finally")
            #if follower_dnode is not None:
            #    tdLog.info(f"Restoring stopped follower dnode {follower_dnode}")
            #    sc.dnodeStart(follower_dnode)
            #    clusterComCheck.checkDnodes(4, timeout=60)
            #tdSql.execute(f"drop database if exists {db_name}")

        print("snapshot KEEP recovery ......................... [ passed ]")

    def test_vnode_snapshot_keep(self):
        """Restore KEEP configuration from a vnode snapshot

        1. Disable vnode placement on dnode 1 in the three-node test cluster.
        2. Create a one-vgroup, three-replica database and stop its follower.
        3. Alter KEEP on the leader, write data, and remove the stopped follower WAL.
        4. Restart the follower and require snapshot recovery.
        5. Verify catalog metadata and follower vnode.json KEEP values.
        6. Restart the follower again and verify the persisted KEEP values.

        Catalog:
            - Database:Replication
            - Database:Snapshot

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-08-17 GitHub Copilot Added vnode snapshot KEEP recovery regression.
            - 2026-08-19 Codex Disabled vnode placement on dnode 1 for three-node runs.
            - 2026-08-19 Codex Changed the database replica count to three.

        """
        self.do_snapshot_keep_recovery()
