# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
import json
import os
import struct
import subprocess
import time

from taos.tmq import Consumer

from new_test_framework.utils import tdSql

from backup_restore_base import SingleNodeBackupRestoreBase


class TestSingleNodeIncrementalBackupRestore(SingleNodeBackupRestoreBase):
    """Single-node incremental backup and restore."""

    def _restore_chain_block_counts(self, chain, backup_id, db_name="restore_chain"):
        backup_dir = os.path.join(chain, str(backup_id))
        with open(os.path.join(backup_dir, "backup.json"), encoding="utf-8") as file:
            manifest = json.load(file)
        database = next(db for db in manifest["databases"] if db["name"].split(".")[-1] == db_name)
        vnode_ids = {int(vg_id) for vg_id in database["vnodes"]}
        counts = {1: 0, 7: 0, 8: 0, 15: 0}
        header = struct.Struct("@bb6xqq")
        for vnode in manifest["vnodes"]:
            if int(vnode["id"]) not in vnode_ids:
                continue
            chunks = os.path.join(backup_dir, f"vnode{vnode['id']}")
            data = bytearray()
            for index in range(1, int(vnode["numOfChunks"]) + 1):
                with open(os.path.join(chunks, f"{index:08d}"), "rb") as file:
                    data.extend(file.read())
            offset = 16
            while offset < len(data):
                kind, _, block_index, size = header.unpack_from(data, offset)
                assert block_index > 0 and size >= 0
                if kind in counts:
                    counts[kind] += 1
                offset += header.size + size
            assert offset == len(data)
        return counts

    def do_incremental_restore(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_chain")
        tdSql.execute("CREATE DATABASE restore_chain VGROUPS 2 KEEP 3650 DURATION 1")
        tdSql.execute("CREATE TABLE restore_chain.t (ts TIMESTAMP, v INT, w INT)")
        tdSql.execute("CREATE TABLE restore_chain.old (ts TIMESTAMP, v INT)")
        tdSql.execute("CREATE STABLE restore_chain.st (ts TIMESTAMP, v INT) TAGS (site INT)")
        tdSql.execute("CREATE TABLE restore_chain.ct1 USING restore_chain.st TAGS (10)")
        tdSql.execute("INSERT INTO restore_chain.ct1 VALUES (1735689600000, 7)")
        tdSql.execute("CREATE STABLE restore_chain.st_drop (ts TIMESTAMP, v INT) TAGS (site INT)")
        tdSql.execute("CREATE TABLE restore_chain.ct_drop USING restore_chain.st_drop TAGS (1)")
        tdSql.execute("INSERT INTO restore_chain.ct_drop VALUES (1735689600000, 8)")
        tdSql.execute("INSERT INTO restore_chain.t VALUES (1735689600000, 1, 100) (1738368000000, 2, 200)")
        tdSql.execute("CREATE TOPIC restore_chain_topic AS SELECT * FROM restore_chain.t")
        consumer = Consumer({
            "group.id": "restore_chain_group",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
        })
        try:
            consumer.subscribe(["restore_chain_topic"])
            for _ in range(60):
                message = consumer.poll(1)
                if message is not None:
                    if message.error() is not None:
                        raise message.error()
                    consumer.commit(message)
                    break
            else:
                raise AssertionError("consumer did not receive data")
        finally:
            consumer.close()
        chain = self._start_backup("incremental_restore")
        chain_id = int(os.path.basename(chain))
        full_counts = self._restore_chain_block_counts(chain, chain_id)
        assert full_counts[1] > 0 and full_counts[7] > 0 and full_counts[8] > 0
        backup_dir = os.path.dirname(chain)
        ids = [chain_id]

        def incremental():
            tdSql.query("SHOW BACKUPS")
            baseline = max((tdSql.getData(r, 0) for r in range(tdSql.queryRows)), default=0)
            time.sleep(1.1)
            tdSql.execute(f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} TO '{backup_dir}' {chain_id}")
            backup_id = self._wait_backup_record(baseline)
            self._wait_backup_done(backup_id)
            ids.append(backup_id)

        tdSql.execute("INSERT INTO restore_chain.t VALUES (1735689600000, 99, NULL)")
        tdSql.execute("DELETE FROM restore_chain.t WHERE ts = 1738368000000")
        tdSql.execute("ALTER TABLE restore_chain.t ADD COLUMN z INT")
        tdSql.execute("INSERT INTO restore_chain.t VALUES (1740787200000, 3, 300, 7)")
        tdSql.execute("DROP TABLE restore_chain.old")
        tdSql.execute("CREATE TABLE restore_chain.new_t (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO restore_chain.new_t VALUES (1735689600000, 17) (1740787200000, 18)")
        tdSql.execute("ALTER TABLE restore_chain.ct1 SET TAG site=11")
        tdSql.execute("DROP STABLE restore_chain.st_drop")
        tdSql.execute("DROP TOPIC FORCE restore_chain_topic")
        incremental()
        delta_counts = self._restore_chain_block_counts(chain, ids[1])
        assert delta_counts[1] > 0 and delta_counts[7] == 0 and delta_counts[8] == 0
        tdSql.execute("INSERT INTO restore_chain.t VALUES (1740787200000, 5, 7, 9) (1743465600000, 4, 400, 10)")
        incremental()
        assert self._restore_chain_block_counts(chain, ids[2])[1] > 0

        expected = (
            [(1, 100), (2, 200)],
            [(99, None, None), (3, 300, 7)],
            [(99, None, None), (5, 7, 9), (4, 400, 10)],
        )
        self._stop_taosd()
        for target, kind in ((ids[0], 0), (ids[0], 1), (ids[0], 14), (ids[0], 7), (ids[0], 8), (ids[1], 2)):
            backup_dir = os.path.join(chain, str(target))
            with open(os.path.join(backup_dir, "backup.json"), encoding="utf-8") as file:
                stage_manifest = json.load(file)
            injected_stage = False
            for vnode in stage_manifest["vnodes"]:
                assert int(vnode["numOfChunks"]) == 1
                chunk_path = os.path.join(backup_dir, f"vnode{vnode['id']}", "00000001")
                with open(chunk_path, "rb") as file:
                    original_stage = file.read()
                damaged_stage = bytearray(original_stage)
                offset = 16
                while offset < len(damaged_stage):
                    block_kind, _, block_index, size = struct.unpack_from("@bb6xqq", damaged_stage, offset)
                    if block_kind == kind:
                        struct.pack_into("@q", damaged_stage, offset + 8, block_index + 5)
                        injected_stage = True
                        break
                    offset += 24 + size
                if not injected_stage:
                    continue
                args = [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain,
                        "--verify=0", "--last-backup", str(target)]
                try:
                    with open(chunk_path, "wb") as file:
                        file.write(damaged_stage)
                    failed = subprocess.run(args, input="YES\nYES\n", text=True, capture_output=True, timeout=180)
                    assert failed.returncode != 0 and "failed to restore vnode" in failed.stdout, failed.stdout
                finally:
                    with open(chunk_path, "wb") as file:
                        file.write(original_stage)
                retry = subprocess.run(args, input="YES\nYES\n", text=True, capture_output=True, timeout=180)
                assert retry.returncode == 0, retry.stdout + retry.stderr
                break
            assert injected_stage, f"snapshot block type {kind} not found in backup {target}"
        delta_dir = os.path.join(chain, str(ids[1]))
        with open(os.path.join(delta_dir, "backup.json"), encoding="utf-8") as file:
            manifest = json.load(file)
        injected = False
        for vnode in manifest["vnodes"]:
            assert int(vnode["numOfChunks"]) == 1
            chunk_path = os.path.join(delta_dir, f"vnode{vnode['id']}", "00000001")
            with open(chunk_path, "rb") as file:
                data = file.read()
            damaged = bytearray(data)
            offset = 16
            while offset < len(damaged):
                kind, _, _, size = struct.unpack_from("@bb6xqq", damaged, offset)
                payload = offset + 24
                entry_type = struct.unpack_from("@b", damaged, payload + 12)[0] if kind == 1 else 0
                if entry_type == -3:
                    struct.pack_into("@q", damaged, payload + 13, 9223372036854775807)
                    injected = True
                    break
                offset += 24 + size
            if injected:
                try:
                    with open(chunk_path, "wb") as file:
                        file.write(damaged)
                    result = subprocess.run(
                        [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain,
                         "--verify=0", "--last-backup", str(ids[1])],
                        input="YES\nYES\n", text=True, capture_output=True, timeout=180,
                    )
                    assert result.returncode == 0, result.stdout + result.stderr
                    self._start_taosd()
                    tdSql.query("SHOW restore_chain.TABLES")
                    assert "old" in [tdSql.getData(row, 0) for row in range(tdSql.queryRows)]
                    self._stop_taosd()
                finally:
                    with open(chunk_path, "wb") as file:
                        file.write(data)
                break
        assert injected, "normal table deletion block not found"
        chunk = os.path.join(delta_dir, f"vnode{manifest['vnodes'][0]['id']}", "00000001")
        with open(chunk, "rb") as file:
            original = file.read()
        try:
            damaged = bytearray(original)
            struct.pack_into("@q", damaged, 24, 7)
            with open(chunk, "wb") as file:
                file.write(damaged)
            failed = subprocess.run(
                [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain,
                 "--verify=0", "--last-backup", str(ids[1])],
                input="YES\nYES\n", text=True, capture_output=True, timeout=180,
            )
            assert failed.returncode != 0 and "failed to restore vnode" in failed.stdout
        finally:
            with open(chunk, "wb") as file:
                file.write(original)
        for index, backup_id in enumerate(ids):
            result = subprocess.run(
                [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain, "--last-backup", str(backup_id)],
                input="YES\nYES\n", text=True, capture_output=True, timeout=180,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            self._start_taosd()
            columns = "v,w" if index == 0 else "v,w,z"
            tdSql.query(f"SELECT {columns} FROM restore_chain.t ORDER BY ts")
            assert [tuple(row) for row in tdSql.queryResult] == expected[index]
            tdSql.query("SHOW restore_chain.TABLES")
            names = [tdSql.getData(row, 0) for row in range(tdSql.queryRows)]
            assert ("old" in names) == (index == 0)
            assert ("ct_drop" in names) == (index == 0)
            assert ("new_t" in names) == (index > 0)
            if index > 0:
                tdSql.query("SELECT v FROM restore_chain.new_t ORDER BY ts")
                assert [tuple(row) for row in tdSql.queryResult] == [(17,), (18,)]
            tdSql.query("SELECT site, v FROM restore_chain.ct1")
            assert [tuple(row) for row in tdSql.queryResult] == [(10 if index == 0 else 11, 7)]
            restored_chain = self._start_backup(f"incremental_restored_{index}")
            counts = self._restore_chain_block_counts(restored_chain, int(os.path.basename(restored_chain)))
            assert (counts[7] > 0 and counts[8] > 0) if index == 0 else (counts[7] == 0 and counts[8] == 0)
            if index + 1 < len(ids):
                self._stop_taosd()
        print("incremental restore ............................... [ passed ]")

    def do_incremental_meta_compact(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_compact")
        tdSql.execute("DROP DATABASE IF EXISTS restore_compact_empty")
        tdSql.execute("CREATE DATABASE restore_compact VGROUPS 1 KEEP 3650 DURATION 1")
        tdSql.execute("CREATE DATABASE restore_compact_empty VGROUPS 1 KEEP 3650")
        tdSql.execute("CREATE TABLE restore_compact.gone (ts TIMESTAMP, v INT)")
        tdSql.execute("CREATE TABLE restore_compact_empty.gone (ts TIMESTAMP, v INT)")
        tdSql.execute("CREATE STABLE restore_compact.st (ts TIMESTAMP, v INT) TAGS (site INT)")
        tdSql.execute("CREATE TABLE restore_compact.kept USING restore_compact.st TAGS (1)")
        tdSql.execute("INSERT INTO restore_compact.gone VALUES (1735689600000, 7)")
        tdSql.execute("INSERT INTO restore_compact.kept VALUES (1735689600000, 10) (1738368000000, 20)")
        chain = self._start_backup("incremental_meta_compact")
        chain_id = int(os.path.basename(chain))
        tdSql.execute("DROP TABLE restore_compact.gone")
        tdSql.execute("DROP TABLE restore_compact_empty.gone")
        tdSql.execute("ALTER STABLE restore_compact.st ADD COLUMN w INT")
        tdSql.execute("ALTER TABLE restore_compact.kept SET TAG site=2")
        tdSql.execute("INSERT INTO restore_compact.kept VALUES (1740787200000, 30, 300)")
        tdSql.execute("COMPACT DATABASE restore_compact META_ONLY FORCE")
        deadline = time.monotonic() + 120
        while tdSql.query("SHOW COMPACTS"):
            assert time.monotonic() < deadline, "meta compaction timed out"
            time.sleep(1)
        tdSql.execute("COMPACT DATABASE restore_compact_empty META_ONLY FORCE")
        deadline = time.monotonic() + 120
        while tdSql.query("SHOW COMPACTS"):
            assert time.monotonic() < deadline, "empty meta compaction timed out"
            time.sleep(1)
        tdSql.query("SHOW BACKUPS")
        baseline = max((tdSql.getData(r, 0) for r in range(tdSql.queryRows)), default=0)
        time.sleep(1.1)
        tdSql.execute(f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} TO '{os.path.dirname(chain)}' {chain_id}")
        incremental_id = self._wait_backup_record(baseline)
        self._wait_backup_done(incremental_id)
        counts = self._restore_chain_block_counts(chain, incremental_id, "restore_compact")
        assert counts[1] > 0, counts
        assert self._restore_chain_block_counts(chain, incremental_id, "restore_compact_empty")[1] == 0
        self._stop_taosd()
        code, output = self._restore(self.dnode_id, chain, None)
        assert code == 0, output
        self._start_taosd()
        tdSql.query("SHOW restore_compact.TABLES")
        assert "gone" not in [tdSql.getData(row, 0) for row in range(tdSql.queryRows)]
        tdSql.query("SHOW restore_compact_empty.TABLES")
        assert "gone" not in [tdSql.getData(row, 0) for row in range(tdSql.queryRows)]
        tdSql.query("SELECT v, w, site FROM restore_compact.kept ORDER BY ts")
        assert [tuple(row) for row in tdSql.queryResult] == [(10, None, 2), (20, None, 2), (30, 300, 2)]
        print("incremental meta compact .......................... [ passed ]")

    def test_incremental_meta_compact(self):
        """Replace full metadata after metadata compaction.

        1. Capture a baseline, delete tables and compact empty and non-empty metadata.
        2. Verify the incremental backup contains only surviving metadata.
        3. Verify deleted tables stay absent and surviving schema, tags and rows match.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-05 Replace metadata after compaction
        """
        self.do_incremental_meta_compact()

    def _backup_vnode_details(self, chain, backup_id, db_name):
        backup_dir = os.path.join(chain, str(backup_id))
        with open(os.path.join(backup_dir, "backup.json"), encoding="utf-8") as file:
            manifest = json.load(file)
        database = next(db for db in manifest["databases"] if db["name"].split(".")[-1] == db_name)
        ids = {int(value) for value in database["vnodes"]}
        result = {}
        keys = ("keep0", "keep1", "keep2", "keepTimeOffset", "sstTrigger",
                "wal.retentionPeriod", "wal.fsyncPeriod")
        for vnode in manifest["vnodes"]:
            vg_id = int(vnode["id"])
            if vg_id not in ids:
                continue
            with open(os.path.join(backup_dir, f"vnode{vg_id}", "00000001"), "rb") as file:
                header = file.read(40)
                kind, _, index, size = struct.unpack_from("@bb6xqq", header, 16)
                assert kind == 0 and index == 1 and size > 1
                config = json.loads(file.read(size).rstrip(b"\0"))["config"]
            maxima = {}
            for item in vnode["fileSets"]["sets"]:
                versions = [int(entry["maxVer"]) for entry in item["ranges"]
                            if int(entry["minVer"]) <= int(entry["maxVer"])]
                maxima[int(item["fid"])] = max(versions, default=-1)
            result[vg_id] = {"config": {key: int(config[key]) for key in keys}, "maxima": maxima}
        assert len(result) == len(ids)
        return result

    def do_incremental_config_and_filesets(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_settings")
        tdSql.execute("CREATE DATABASE restore_settings VGROUPS 2 DURATION 1 KEEP 3650 "
                      "STT_TRIGGER 2 WAL_RETENTION_PERIOD 3600 WAL_FSYNC_PERIOD 1000 KEEP_TIME_OFFSET 0h")
        tdSql.execute("CREATE TABLE restore_settings.t (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO restore_settings.t VALUES (1735689600000,1) (1738368000000,2)")
        chain = self._start_backup("incremental_settings")
        ids = [int(os.path.basename(chain))]
        expected = [self._backup_vnode_details(chain, ids[0], "restore_settings")]
        tdSql.execute("ALTER DATABASE restore_settings KEEP 4000")
        tdSql.execute("ALTER DATABASE restore_settings STT_TRIGGER 4")
        tdSql.execute("ALTER DATABASE restore_settings WAL_RETENTION_PERIOD 7200")
        tdSql.execute("ALTER DATABASE restore_settings WAL_FSYNC_PERIOD 2000")
        tdSql.execute("ALTER DATABASE restore_settings KEEP_TIME_OFFSET 3h")
        tdSql.execute("INSERT INTO restore_settings.t VALUES (1735689600000,9) (1740787200000,3)")
        ids.append(self._start_incremental_backup(chain))
        expected.append(self._backup_vnode_details(chain, ids[-1], "restore_settings"))
        tdSql.execute("INSERT INTO restore_settings.t VALUES (1743465600000,4)")
        ids.append(self._start_incremental_backup(chain))
        expected.append(self._backup_vnode_details(chain, ids[-1], "restore_settings"))
        ids.append(self._start_incremental_backup(chain))
        expected.append(self._backup_vnode_details(chain, ids[-1], "restore_settings"))
        for vg_id in expected[0]:
            before, after = expected[0][vg_id]["config"], expected[1][vg_id]["config"]
            assert (before["keep2"], after["keep2"]) == (3650 * 1440, 4000 * 1440)
            assert (before["keepTimeOffset"], after["keepTimeOffset"]) == (0, 3)
            assert (before["sstTrigger"], after["sstTrigger"]) == (2, 4)
            assert (before["wal.retentionPeriod"], after["wal.retentionPeriod"]) == (3600, 7200)
            assert (before["wal.fsyncPeriod"], after["wal.fsyncPeriod"]) == (1000, 2000)
        assert sum(len(v["maxima"]) for v in expected[-1].values()) >= 4
        rows = ([(1,), (2,)], [(9,), (2,), (3,)], [(9,), (2,), (3,), (4,)],
                [(9,), (2,), (3,), (4,)])
        self._stop_taosd()
        for index, backup_id in enumerate(ids):
            result = subprocess.run(
                [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain,
                 "--last-backup", str(backup_id)],
                input="YES\nYES\n", text=True, capture_output=True, timeout=180,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            for vg_id, details in expected[index].items():
                path = os.path.join(self._dnode_data_dir(self.dnode_id), "vnode", f"vnode{vg_id}", "vnode.json")
                with open(path, encoding="utf-8") as file:
                    config = json.load(file)["config"]
                assert {key: int(config[key]) for key in details["config"]} == details["config"]
            self._start_taosd()
            tdSql.query("SELECT v FROM restore_settings.t ORDER BY ts")
            assert [tuple(row) for row in tdSql.queryResult] == rows[index]
            restored_chain = self._start_backup(f"settings_restored_{index}")
            restored = self._backup_vnode_details(
                restored_chain, int(os.path.basename(restored_chain)), "restore_settings"
            )
            for vg_id in expected[index]:
                assert restored[vg_id]["config"] == expected[index][vg_id]["config"]
                assert restored[vg_id]["maxima"] == expected[index][vg_id]["maxima"]
            if index + 1 < len(ids):
                self._stop_taosd()
        print("incremental settings and filesets ................. [ passed ]")

    def test_incremental_config_and_filesets(self):
        """Restore vnode settings and fileset ranges across incremental points.

        1. Alter database settings after a full backup and capture consecutive increments.
        2. Restore each point, including an unchanged increment, and verify vnode settings.
        3. Compare per-fid version maxima and query results after every restore.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify incremental settings and fileset ranges
        """
        self.do_incremental_config_and_filesets()

    def _restore_backup_point(self, chain, backup_id):
        self._stop_taosd()
        result = subprocess.run(
            [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain,
             "--last-backup", str(backup_id)],
            input="YES\nYES\n", text=True, capture_output=True, timeout=300,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        self._start_taosd()

    def do_incremental_virtual_table_restore(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_virtual")
        tdSql.execute("CREATE DATABASE restore_virtual VGROUPS 1 KEEP 3650")
        tdSql.execute("USE restore_virtual")
        tdSql.execute("CREATE TABLE source_data (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO source_data VALUES (1735689600000, 1)")
        tdSql.execute("CREATE STABLE vst (ts TIMESTAMP, v INT) TAGS (site INT) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE vc (v FROM source_data.v) USING vst TAGS (1)")
        chain = self._start_backup("incremental_virtual")
        full_id = int(os.path.basename(chain))
        tdSql.execute("INSERT INTO source_data VALUES (1735689601000, 2)")
        tdSql.execute("CREATE VTABLE vc2 (v FROM source_data.v) USING vst TAGS (2)")
        delta_id = self._start_incremental_backup(chain)
        for backup_id, values, count in ((full_id, [(1,)], 1), (delta_id, [(1,), (2,)], 2)):
            self._restore_backup_point(chain, backup_id)
            tdSql.query("SELECT v FROM restore_virtual.vc ORDER BY ts")
            assert [tuple(row) for row in tdSql.queryResult] == values
            tdSql.query("SELECT COUNT(*) FROM information_schema.ins_tables "
                        "WHERE db_name='restore_virtual' AND table_name IN ('vc', 'vc2')")
            tdSql.checkData(0, 0, count)
        print("incremental virtual table restore ................. [ passed ]")

    def test_incremental_virtual_table_restore(self):
        """Restore virtual table references at two backup points.

        1. Back up a virtual table and its source data.
        2. Add source data and another virtual table, then take an increment.
        3. Restore both points and verify table presence and referenced values.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify incremental virtual table restore
        """
        self.do_incremental_virtual_table_restore()

    def do_incremental_rsma_restore(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_rsma")
        tdSql.execute("CREATE DATABASE restore_rsma VGROUPS 1 KEEP 3650")
        tdSql.execute("USE restore_rsma")
        tdSql.execute("CREATE STABLE restore_rsma.st (ts TIMESTAMP, v INT) TAGS (site INT)")
        tdSql.execute("CREATE TABLE restore_rsma.ct USING restore_rsma.st TAGS (1)")
        tdSql.execute("INSERT INTO restore_rsma.ct VALUES (1735689600000, 1)")
        tdSql.execute("CREATE RSMA r1 ON restore_rsma.st FUNCTION(min(v)) INTERVAL(1m,5m)")
        tdSql.execute("FLUSH DATABASE restore_rsma")
        chain = self._start_backup("incremental_rsma")
        full_id = int(os.path.basename(chain))
        tdSql.execute("INSERT INTO restore_rsma.ct VALUES (1735689660000, 2)")
        tdSql.execute("FLUSH DATABASE restore_rsma")
        delta_id = self._start_incremental_backup(chain)
        for backup_id, values in ((full_id, [(1,)]), (delta_id, [(1,), (2,)])):
            self._restore_backup_point(chain, backup_id)
            tdSql.query("SELECT v FROM restore_rsma.ct ORDER BY ts")
            assert [tuple(row) for row in tdSql.queryResult] == values
            tdSql.query("SELECT COUNT(*) FROM information_schema.ins_rsmas "
                        "WHERE db_name='restore_rsma'")
            tdSql.checkData(0, 0, 1)
        print("incremental RSMA restore .......................... [ passed ]")

    def test_incremental_rsma_restore(self):
        """Restore RSMA metadata and source data at two backup points.

        1. Create an RSMA and back up its flushed source table.
        2. Flush additional source data and take an incremental backup.
        3. Restore each point and check the RSMA and source data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify incremental RSMA restore
        """
        self.do_incremental_rsma_restore()

    def do_incremental_blob_restore(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_blob")
        tdSql.execute("CREATE DATABASE restore_blob VGROUPS 1 KEEP 3650")
        tdSql.execute("CREATE TABLE restore_blob.t (ts TIMESTAMP, b BLOB)")
        tdSql.execute("INSERT INTO restore_blob.t VALUES (1735689600000, 'old')")
        tdSql.execute("FLUSH DATABASE restore_blob")
        tdSql.query("SELECT b FROM restore_blob.t ORDER BY ts")
        assert [tuple(row) for row in tdSql.queryResult] == [(b"old",)]
        chain = self._start_backup("incremental_blob")
        full_id = int(os.path.basename(chain))
        full_bse_blocks = self._restore_chain_block_counts(chain, full_id, "restore_blob")[15]
        tdSql.execute("INSERT INTO restore_blob.t VALUES (1735689600000, 'updated') "
                      "(1735689601000, 'new')")
        tdSql.execute("FLUSH DATABASE restore_blob")
        tdSql.query("SELECT b FROM restore_blob.t ORDER BY ts")
        assert [tuple(row) for row in tdSql.queryResult] == [(b"updated",), (b"new",)]
        delta_id = self._start_incremental_backup(chain)
        delta_bse_blocks = self._restore_chain_block_counts(chain, delta_id, "restore_blob")[15]
        tdSql.execute("INSERT INTO restore_blob.t VALUES (1735689601000, 'newer')")
        tdSql.execute("FLUSH DATABASE restore_blob")
        delta2_id = self._start_incremental_backup(chain)
        delta2_bse_blocks = self._restore_chain_block_counts(chain, delta2_id, "restore_blob")[15]
        for backup_id, values in ((full_id, [(b"old",)]),
                                  (delta_id, [(b"updated",), (b"new",)]),
                                  (delta2_id, [(b"updated",), (b"newer",)])):
            self._restore_backup_point(chain, backup_id)
            tdSql.query("SELECT b FROM restore_blob.t ORDER BY ts")
            assert [tuple(row) for row in tdSql.queryResult] == values
            tdSql.execute("INSERT INTO restore_blob.t VALUES (1735689602000, 'after')")
            tdSql.execute("FLUSH DATABASE restore_blob")
            self._stop_taosd()
            self._start_taosd()
            tdSql.query("SELECT b FROM restore_blob.t ORDER BY ts")
            assert [tuple(row) for row in tdSql.queryResult] == values + [(b"after",)]
        assert full_bse_blocks > 0 and delta_bse_blocks > 0 and delta2_bse_blocks > 0, (
            full_bse_blocks, delta_bse_blocks, delta2_bse_blocks
        )
        print("incremental BSE restore ........................... [ passed ]")

    def test_incremental_blob_restore(self):
        """Restore non-empty BSE snapshots at two backup points.

        1. Back up flushed BLOB data and verify the BSE snapshot block.
        2. Update and add BLOB values across two incremental backups.
        3. Restore both points, compare BLOB values and write again after recovery.
        4. Restart and verify both restored and newly written BLOB values.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify incremental BSE restore
        """
        self.do_incremental_blob_restore()

    def _consume_backup_topic(self, group, topic, expected):
        consumer = Consumer({
            "group.id": group,
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
        })
        values = []
        try:
            consumer.subscribe([topic])
            deadline = time.monotonic() + 60
            while len(values) < len(expected) and time.monotonic() < deadline:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error() is not None:
                    raise message.error()
                for block in message.value():
                    values.extend(row[1] for row in block.fetchall())
                consumer.commit(message)
            assert values == expected, (group, topic, values, expected)
        finally:
            consumer.close()

    def _backup_topic_offsets(self):
        tdSql.query("SHOW SUBSCRIPTIONS")
        rows = [row for row in tdSql.queryResult if row[0] in ("restore_tq_a", "restore_tq_b", "restore_tq_c")]
        offsets = {(row[0], row[1], row[2]): row[6].split("/", 1)[0] for row in rows}
        assert len(rows) == len(offsets) == 3, rows
        assert {(topic, group) for topic, group, _ in offsets} == {
            ("restore_tq_a", "restore_tq_g1"),
            ("restore_tq_a", "restore_tq_g2"),
            ("restore_tq_c", "restore_tq_g3"),
        }, rows
        assert all(offset.startswith("wal:") and int(offset[4:]) > 0 for offset in offsets.values()), offsets
        return offsets

    def do_incremental_tq_offsets(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_tq")
        tdSql.execute("CREATE DATABASE restore_tq VGROUPS 1 KEEP 3650 WAL_RETENTION_PERIOD 3600")
        for table in ("a", "b", "c"):
            tdSql.execute(f"CREATE TABLE restore_tq.{table} (ts TIMESTAMP, v INT)")
            tdSql.execute(f"INSERT INTO restore_tq.{table} VALUES (1735689600000, 1)")
        tdSql.execute("CREATE TOPIC restore_tq_a AS SELECT * FROM restore_tq.a")
        tdSql.execute("CREATE TOPIC restore_tq_b AS SELECT * FROM restore_tq.b")
        self._consume_backup_topic("restore_tq_g1", "restore_tq_a", [1])
        self._consume_backup_topic("restore_tq_g2", "restore_tq_a", [1])
        self._consume_backup_topic("restore_tq_removed", "restore_tq_b", [1])
        chain = self._start_backup("incremental_tq_offsets")
        tdSql.execute("INSERT INTO restore_tq.a VALUES (1735689601000, 2)")
        self._consume_backup_topic("restore_tq_g1", "restore_tq_a", [2])
        tdSql.execute("INSERT INTO restore_tq.a VALUES (1735689602000, 3)")
        tdSql.execute("DROP TOPIC FORCE restore_tq_b")
        tdSql.execute("CREATE TOPIC restore_tq_c AS SELECT * FROM restore_tq.c")
        self._consume_backup_topic("restore_tq_g3", "restore_tq_c", [1])
        tdSql.execute("INSERT INTO restore_tq.c VALUES (1735689601000, 2)")
        # Backups do not preserve ordinary WAL records needed to replay an unconsumed backlog.
        self._consume_backup_topic("restore_tq_g1", "restore_tq_a", [3])
        self._consume_backup_topic("restore_tq_g2", "restore_tq_a", [2, 3])
        self._consume_backup_topic("restore_tq_g3", "restore_tq_c", [2])
        self._start_incremental_backup(chain)
        offsets = self._backup_topic_offsets()
        self._stop_taosd()
        code, output = self._restore(self.dnode_id, chain, None)
        assert code == 0, output
        self._start_taosd()
        tdSql.query("SHOW TOPICS")
        names = [tdSql.getData(row, 0) for row in range(tdSql.queryRows)]
        assert "restore_tq_a" in names and "restore_tq_c" in names and "restore_tq_b" not in names, names
        assert self._backup_topic_offsets() == offsets
        tdSql.execute("INSERT INTO restore_tq.a VALUES (1735689603000, 4)")
        tdSql.execute("INSERT INTO restore_tq.c VALUES (1735689602000, 3)")
        self._consume_backup_topic("restore_tq_g1", "restore_tq_a", [4])
        self._consume_backup_topic("restore_tq_g2", "restore_tq_a", [4])
        self._consume_backup_topic("restore_tq_g3", "restore_tq_c", [3])
        print("incremental topic offsets .......................... [ passed ]")

    def test_incremental_tq_offsets(self):
        """Restore non-empty topic handles and committed offsets.

        1. Capture two topics and multiple consumer groups in a full backup.
        2. Change offsets, remove one topic, add another, and consume pending data before an increment.
        3. Restore and verify the topic handles and committed offsets.
        4. Write new data and verify each consumer group can consume it after restoration.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify incremental topic state and offsets
        """
        self.do_incremental_tq_offsets()

    def test_incremental_restore(self):
        """Restore a full backup and two incremental backups.

        1. Capture data, schema and table deletion across a backup chain.
        2. Restore each backup point and verify updates, deletes and nulls.
        3. Verify tag changes, table creation and deletion, and full TQ replacement.
        4. Inject failures in config, metadata, RAW, TSDB and TQ blocks and retry each restore.
        5. Verify metadata apply errors are nonfatal and malformed blocks fail.
        6. Verify unchanged metadata is included in each incremental backup.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-04 Restore incremental backup chains
        """
        self.do_incremental_restore()
