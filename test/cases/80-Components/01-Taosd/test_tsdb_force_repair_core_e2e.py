import os
import shutil
import time

import pytest

from new_test_framework.utils import tdDnodes, tdLog, tdSql
from test_tsdb_force_repair_base import TsdbForceRepairBase


class TestTsdbForceRepairCoreE2E(TsdbForceRepairBase):
    def test_tsdb_force_repair_missing_head_real_fileset_remains_writable(self):
        """TSDB force repair should keep a real fileset writable after missing-head repair.

        1. Build one real core fixture and remove its head file.
        2. Run force repair for the affected fid with backup enabled.
        3. Verify the database remains readable and writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        backup_root = f"/tmp/tsdb-force-repair-missing-head-{time.time_ns()}"

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(fixture["fileset"]["head"])
            code, output = self._run_force_repair(vnode_id, fid, backup_root=backup_root, timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows >= 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        repair_log = self._find_backup_log_for_fid(backup_root, vnode_id, fid)
        if repair_log is not None and os.path.isfile(repair_log):
            with open(repair_log, "r", encoding="utf-8") as fp:
                log_text = fp.read()
            tdSql.checkEqual("action=drop_core_group" in log_text, True)
        self._assert_database_writable_after_repair(dbname, "pk_after_missing_head", 1001)

    def test_tsdb_force_repair_missing_old_head_in_multi_fileset_keeps_database_writable(self):
        """TSDB force repair should keep a multi-fileset database writable after old-head repair.

        1. Build one vnode fixture with multiple real filesets.
        2. Remove the head file from an older fileset and run force repair.
        3. Verify remaining data stays readable and the table remains writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_multi_fileset_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        target = fixture["filesets"][0]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(target["head"])
            code, output = self._run_force_repair(vnode_id, target["fid"], timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows > 0, True)
        tdSql.checkEqual(repaired_rows < fixture["row_count"], True)
        self._assert_database_writable_after_repair(dbname, "pk_after_multi_missing_head", 2001)

    def test_tsdb_force_repair_rebuilds_core_from_valid_blocks(self):
        """TSDB force repair should rebuild head/data/sma from readable blocks.

        1. Create one vnode with enough rows to form multiple blocks.
        2. Corrupt late bytes in a real `.head` file without changing file size.
        3. Run tsdb force repair and verify it records `rebuild_core_group`.
        4. Restart normally and verify the table still has readable rows.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_rebuild_blocks_{time.time_ns()}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1 minrows 10 maxrows 200")
        tdSql.execute(f"drop table if exists {dbname}.t1")
        tdSql.execute(
            f"create table if not exists {dbname}.t1(ts timestamp, v1 varchar(20) primary key, v2 int)"
        )

        total_rows = 9000
        ts0 = 1700000000000
        self._insert_rebuild_rows(dbname, 0, 8191, ts0)

        tdSql.execute(f"flush database {dbname}")

        self._insert_rebuild_rows(dbname, 8191, total_rows, ts0)

        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, total_rows)

        vnode_id = self._get_vnode_id_for_db(dbname)
        backup_root = f"/tmp/tsdb-force-repair-rebuild-{int(time.time())}"
        data_file = self._wait_for_data_file(dbname, vnode_id, timeout_sec=90)
        if data_file is None:
            pytest.skip("real data file was not materialized in time after async flush")

        candidate = self._find_size_matched_core_fileset(vnode_id)
        if candidate is None:
            pytest.skip("no real core fileset with manifest-matched head/data sizes found")

        fid = candidate["fid"]
        corrupt_target = candidate["head"]
        tdLog.info("block repair corruption target=%s" % corrupt_target)
        keep_backup = False

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._overwrite_middle_bytes(corrupt_target)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, fid, backup_root=backup_root, extra_args="--log-output /dev/null"),
                timeout_sec=10,
            )
            repair_log = self._find_backup_log_for_fid(backup_root, vnode_id, fid)
            tdSql.checkEqual(code, 0)
            if repair_log is not None and os.path.isfile(repair_log):
                with open(repair_log, "r", encoding="utf-8") as fp:
                    log_text = fp.read()
                tdLog.info("rebuild repair.log=%s" % log_text.replace("\n", " | "))
                if "action=rebuild_core_group" not in log_text:
                    keep_backup = True
                    raise Exception(
                        f"unexpected repair.log for block rebuild: {log_text}; backup_root={backup_root}; target={corrupt_target}"
                    )

            self._restart_taosd_and_wait_ready(dbname=dbname)
            tdSql.query(f"select count(*) from {dbname}.t1")
            repaired_rows = tdSql.queryResult[0][0]
            tdSql.checkEqual(repaired_rows > 0, True)
            tdSql.checkEqual(repaired_rows <= total_rows, True)
            self._assert_database_writable_after_repair(dbname, "pk_after_block_rebuild", 4001)
        finally:
            try:
                tdDnodes.stop(1)
            except BaseException:
                pass
            time.sleep(2)
            try:
                tdDnodes.start(1)
                time.sleep(2)
                tdSql.query("select * from information_schema.ins_databases")
            except BaseException:
                pass
            if os.path.exists(backup_root) and not keep_backup:
                shutil.rmtree(backup_root)

    def test_tsdb_force_repair_head_only_rebuild_recovers_real_head_damage(self):
        """TSDB force repair head-only rebuild should recover real head damage.

        1. Build one real core fixture and corrupt a real `.head` file without changing file size.
        2. Run force repair with `head_only_rebuild` for the affected fid.
        3. Verify the original `.data` file remains on disk and the database stays writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        backup_root = f"/tmp/tsdb-force-repair-head-only-{int(time.time())}"
        head_path = fixture["fileset"]["head"]
        data_path = fixture["fileset"]["data"]
        data_size_before = os.path.getsize(data_path)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._overwrite_middle_bytes(head_path)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(
                    vnode_id,
                    fid,
                    strategy="head_only_rebuild",
                    backup_root=backup_root,
                    extra_args="--log-output /dev/null",
                ),
                timeout_sec=10,
            )
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.checkEqual(os.path.isfile(data_path), True)
        tdSql.checkEqual(os.path.getsize(data_path), data_size_before)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows > 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        self._assert_database_writable_after_repair(
            dbname, "pk_after_head_only_rebuild", 3501
        )
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)

    def test_tsdb_force_repair_drop_invalid_only_does_not_fix_head_size_mismatch(self):
        """TSDB force repair default strategy should not fix head size mismatch.

        1. Build one real core fixture and extend its head file to create size mismatch.
        2. Run force repair without an explicit strategy so it uses `drop_invalid_only`.
        3. Verify the head file size remains changed, proving deep repair was not applied.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        head_path = fixture["fileset"]["head"]
        head_size_before = os.path.getsize(head_path)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_size_mismatch(head_path, mode="extend")
            head_size_after_corrupt = os.path.getsize(head_path)
            code, output = self._run_force_repair(
                vnode_id, fid, timeout_sec=10
            )
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.checkEqual(head_size_after_corrupt > head_size_before, True)
        tdSql.checkEqual(os.path.getsize(head_path), head_size_after_corrupt)

    def test_tsdb_force_repair_multi_target_repairs_two_filesets(self):
        """TSDB force repair should repair two targeted filesets in one run.

        1. Build one vnode fixture with multiple real filesets.
        2. Corrupt two targeted heads and run one repair command with two `--repair-target` entries.
        3. Verify the database remains readable and writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_multi_fileset_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        target_a = fixture["filesets"][0]
        target_b = fixture["filesets"][1]
        backup_root = f"/tmp/tsdb-force-repair-multi-target-{int(time.time())}"

        args = (
            f"-r --mode force --node-type vnode --backup-path {backup_root} "
            f"--repair-target {self._tsdb_target(vnode_id, target_a['fid'])} "
            f"--repair-target {self._tsdb_target(vnode_id, target_b['fid'])} "
            f"--log-output /dev/null"
        )

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(target_a["head"])
            self._overwrite_middle_bytes(target_b["head"])
            code, output = self._run_taosd_with_cfg(args, timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows > 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        self._assert_database_writable_after_repair(
            dbname, "pk_after_multi_target_repair", 3601
        )
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)

    def test_tsdb_force_repair_wildcard_target_repairs_all_filesets_in_vnode(self):
        """TSDB force repair wildcard target should repair all filesets in one vnode.

        1. Build one vnode fixture with multiple real filesets.
        2. Corrupt two filesets and run one repair command with `fileid=*`.
        3. Verify the database remains readable and writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_multi_fileset_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        target_a = fixture["filesets"][0]
        target_b = fixture["filesets"][1]
        backup_root = f"/tmp/tsdb-force-repair-wildcard-{time.time_ns()}"

        args = (
            f"-r --mode force --node-type vnode --backup-path {backup_root} "
            f"--repair-target tsdb:vnode={vnode_id}:fileid=* "
            f"--log-output /dev/null"
        )

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(target_a["head"])
            self._overwrite_middle_bytes(target_b["head"])
            code, output = self._run_taosd_with_cfg(args, timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows > 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        self._assert_database_writable_after_repair(
            dbname, "pk_after_wildcard_repair", 3651
        )
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)

    def test_tsdb_force_repair_full_rebuild_recovers_real_head_size_mismatch(self):
        """TSDB force repair full rebuild should recover a real head size mismatch.

        1. Build one real core fixture and extend its head file to create size mismatch.
        2. Run force repair with `full_rebuild` for the affected fid.
        3. Verify rows remain queryable and the database stays writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            # Extend the head file to create a recoverable size mismatch while
            # preserving the original footer and brin metadata.
            self._corrupt_size_mismatch(fixture["fileset"]["head"], mode="extend")
            code, output = self._run_force_repair(
                vnode_id, fid, strategy="full_rebuild", timeout_sec=10
            )
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows > 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        self._assert_database_writable_after_repair(
            dbname, "pk_after_full_rebuild", 3001
        )
