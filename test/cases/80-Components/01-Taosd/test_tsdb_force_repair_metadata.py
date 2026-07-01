import os
import tempfile
import time

import pytest

from new_test_framework.utils import tdDnodes, tdSql
from test_tsdb_force_repair_base import TsdbForceRepairBase


class TestTsdbForceRepairMetadata(TsdbForceRepairBase):
    def test_force_repair_fixture_builder_exposes_real_fileset(self):
        """TSDB force repair fixture builder should expose one real core fileset.

        1. Build a real core fixture from on-disk vnode data.
        2. Verify the fixture reports vnode, fid, and row count.
        3. Verify the resolved head and data files exist on disk.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_core_fixture()

        tdSql.checkEqual(fixture["vnode_id"] > 0, True)
        tdSql.checkEqual(fixture["fid"] > 0, True)
        tdSql.checkEqual(fixture["row_count"] > 0, True)
        tdSql.checkEqual(os.path.isfile(fixture["fileset"]["head"]), True)
        tdSql.checkEqual(os.path.isfile(fixture["fileset"]["data"]), True)

    def test_force_repair_multi_fileset_fixture_exposes_multiple_real_filesets(self):
        """TSDB force repair fixture builder should expose multiple real filesets.

        1. Build a real multi-fileset fixture from on-disk vnode data.
        2. Verify the fixture reports vnode and multiple distinct fids.
        3. Verify the resolved file paths exist on disk.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_multi_fileset_core_fixture()

        tdSql.checkEqual(fixture["vnode_id"] > 0, True)
        tdSql.checkEqual(len(fixture["filesets"]) >= 2, True)
        tdSql.checkEqual(
            fixture["filesets"][0]["fid"] != fixture["filesets"][1]["fid"], True
        )
        tdSql.checkEqual(os.path.isfile(fixture["filesets"][0]["head"]), True)
        tdSql.checkEqual(os.path.isfile(fixture["filesets"][1]["data"]), True)

    def test_force_repair_stt_fixture_exposes_real_stt_file(self):
        """TSDB force repair fixture builder should expose one real stt file.

        1. Build a real stt fixture from flushed vnode data.
        2. Verify the fixture resolves vnode, fid, and stt entry count.
        3. Verify the referenced stt file exists on disk.

        Since: v3.4.1.0

        Labels: common,ci
        """
        fixture = self._prepare_stt_fixture()

        tdSql.checkEqual(fixture["vnode_id"] > 0, True)
        tdSql.checkEqual(fixture["fid"] > 0, True)
        tdSql.checkEqual(fixture["stt_entries"] > 0, True)
        tdSql.checkEqual(os.path.isfile(fixture["stt_path"]), True)

    def test_force_repair_corrupt_size_mismatch_changes_file_size(self):
        """TSDB force repair size-mismatch injector should alter file size.

        1. Create a temporary file with known size.
        2. Apply the size-mismatch corruption helper.
        3. Verify the helper reports size-mismatch and the file size changed.

        Since: v3.4.1.0

        Labels: common,ci
        """
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(b"x" * 64)
            path = fp.name

        try:
            before = os.path.getsize(path)
            info = self._corrupt_size_mismatch(path, mode="truncate")
            after = os.path.getsize(path)

            tdSql.checkEqual(info["corruption_type"], "size_mismatch")
            tdSql.checkEqual(after != before, True)
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_tsdb_force_repair_noop_on_healthy_fileset(self):
        """TSDB force repair should leave a healthy fileset readable and writable.

        1. Build one real healthy core fixture.
        2. Run force repair against that fileset without injecting corruption.
        3. Verify row count is unchanged and the table remains writable after restart.

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
            code, output = self._run_force_repair(
                vnode_id, fid, extra_args="", timeout_sec=10
            )
        finally:
            self._restart_taosd_and_wait_ready()

        tdSql.checkEqual(code, 0)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, fixture["row_count"])
        tdSql.execute(f"insert into {dbname}.t1 values(1700000999999, 'pk_after_repair', 999)")
        tdSql.execute(f"flush database {dbname}")
        tdSql.query(f"select count(*) from {dbname}.t1 where v1='pk_after_repair'")
        tdSql.checkData(0, 0, 1)

    def test_tsdb_force_repair_dispatches_in_open_fs(self):
        """TSDB force repair should dispatch inside tsdb open fs.

        1. Create a real vnode.
        2. Run tsdb force repair.
        3. Verify tsdb open fs reports the repair dispatch marker.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_dispatch_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, 1)

        vnode_id = self._get_vnode_id_for_db(dbname)
        candidate = self._find_size_matched_core_fileset(vnode_id)
        if candidate is None:
            pytest.skip("no real core fileset with manifest-matched head/data sizes found")
        repair_fid = candidate["fid"]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            code, output = self._run_taosd_with_cfg(
                self._tsdb_repair_args(vnode_id, repair_fid, extra_args="--log-output /dev/null"),
                timeout_sec=10,
            )
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

        tdSql.checkEqual("tsdb force repair dispatch" in output, True)
        tdSql.checkEqual("repair parameter validation succeeded (phase1)" in output, False)

    def test_tsdb_force_repair_enters_real_execution_path(self):
        """TSDB force repair should not stay on phase1 placeholder path.

        1. Create a real vnode.
        2. Stop taosd and run tsdb force repair against that vnode.
        3. Verify the process no longer reports the phase1 placeholder text.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"tsdb_repair_entry_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1 stt_trigger 1")
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        tdSql.execute(f"insert into {dbname}.t1 values(now, 1)")
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkData(0, 0, 1)

        vnode_id = self._get_vnode_id_for_db(dbname)
        candidate = self._find_size_matched_core_fileset(vnode_id)
        if candidate is None:
            pytest.skip("no real core fileset with manifest-matched head/data sizes found")
        repair_fid = candidate["fid"]

        code, output = self._run_taosd_with_cfg(
            self._tsdb_repair_args(vnode_id, repair_fid, extra_args="--log-output /dev/null")
        )

        tdSql.checkEqual("repair execution is not enabled in this phase" in output, False)
        tdSql.checkEqual(code == 0 and "repair parameter validation succeeded (phase1)" in output, False)
