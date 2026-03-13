import os
import shutil
import time

from new_test_framework.utils import tdDnodes, tdSql
from test_tsdb_force_repair_base import TsdbForceRepairBase


class TestTsdbForceRepairSttE2E(TsdbForceRepairBase):
    def test_tsdb_force_repair_missing_stt_real_fileset_remains_writable(self):
        """TSDB force repair should keep the database writable after missing-stt repair.

        1. Build one real stt fixture and remove its stt file.
        2. Run force repair for the affected fid.
        3. Verify the table remains readable and accepts new writes after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_stt_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        table_name = fixture["table_name"]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._corrupt_missing_file(fixture["stt_path"])
            code, output = self._run_force_repair(vnode_id, fid, timeout_sec=10)
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.{table_name}")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows >= 0, True)
        tdSql.checkEqual(repaired_rows <= fixture["row_count"], True)
        tdSql.execute(f"insert into {dbname}.{table_name} values(1700001000100, 3, 2.5)")
        tdSql.execute(f"flush database {dbname}")
        tdSql.query(f"select count(*) from {dbname}.{table_name} where ts=1700001000100")
        tdSql.checkData(0, 0, 1)

    def test_tsdb_force_repair_missing_stt_with_backup_root_remains_writable(self):
        """TSDB force repair should keep missing-stt repair writable with backup-root enabled.

        1. Create a real vnode with an stt file and configure a backup root.
        2. Remove the stt file and run force repair for the affected fid.
        3. Verify the database remains writable after restart and optionally validate repair.log.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_log_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        backup_root = f"/tmp/tsdb-force-repair-log-{int(time.time())}"
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            corrupt_file = self._find_first_tsdb_file(vnode_id, ".stt")
            tdSql.checkEqual(corrupt_file is not None, True)
            repair_fid = self._parse_fid_from_tsdb_path(corrupt_file)
            tdSql.checkEqual(repair_fid is not None, True)
            os.remove(corrupt_file)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(
                    vnode_id,
                    repair_fid,
                    backup_root=backup_root,
                    extra_args="--log-output /dev/null",
                ),
                timeout_sec=5,
            )
            repair_log = self._find_backup_log(backup_root, vnode_id)
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)
            if os.path.exists(backup_root):
                shutil.rmtree(backup_root)

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        repaired_rows = tdSql.queryResult[0][0]
        tdSql.checkEqual(repaired_rows >= 0, True)
        tdSql.checkEqual(repaired_rows <= 1, True)
        tdSql.execute(f"insert into {dbname}.t1 values(1700001000000, 5001)")
        tdSql.execute(f"flush database {dbname}")
        tdSql.query(f"select count(*) from {dbname}.t1 where ts=1700001000000 and v=5001")
        tdSql.checkData(0, 0, 1)
        if repair_log is not None and os.path.isfile(repair_log):
            with open(repair_log, "r", encoding="utf-8") as fp:
                log_text = fp.read()
            self._assert_repair_log_fields(
                log_text, repair_fid, action="drop_stt_file", reason="missing_stt"
            )

    def test_tsdb_force_repair_removes_missing_stt_from_current(self):
        """TSDB force repair should remove missing stt metadata from current.json.

        1. Build one real stt fixture and confirm current.json contains stt entries.
        2. Remove the stt file and run force repair for the affected fid.
        3. Verify current.json no longer references the missing stt after repair.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_stt_fixture()
        vnode_id = fixture["vnode_id"]
        current_json = self._current_json_path(vnode_id)

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            corrupt_file = fixture["stt_path"]
            repair_fid = fixture["fid"]
            before_count = self._count_stt_entries_in_current(current_json)
            tdSql.checkEqual(before_count > 0, True)
            os.remove(corrupt_file)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, repair_fid, extra_args="--log-output /dev/null"),
                timeout_sec=10,
            )
            after_count = self._count_stt_entries_in_current(current_json)
            tdSql.checkEqual(code, 0)
            tdSql.checkEqual(after_count, 0)
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
