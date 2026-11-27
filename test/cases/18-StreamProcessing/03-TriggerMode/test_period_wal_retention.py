# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import glob
import json
import os
import re
import time

from new_test_framework.utils import clusterComCheck, sc, tdDnodes, tdLog, tdSql, tdStream


class TestPeriodWalRetention:
    PERIOD = 60
    SOURCE = "period_wal_source"
    RESULT = "period_wal_result"
    STREAMS = ("normal_direct", "normal_rows", "virtual_direct", "virtual_rows")

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    def _prepare(self):
        tdStream.ensureSnode()
        for db in (self.RESULT, self.SOURCE):
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(
                f"create database {db} vgroups 1 precision 'ms' "
                "wal_retention_period 1 wal_retention_size 0 wal_roll_period 1"
            )
        for table in ("normal_input", "left_source", "right_source", "noise"):
            tdSql.execute(f"create table {self.SOURCE}.{table} (ts timestamp, v int)")
        tdSql.execute(
            f"create vtable {self.RESULT}.virtual_input (ts timestamp, "
            f"v int from {self.SOURCE}.left_source.v, "
            f"w int from {self.SOURCE}.right_source.v)"
        )
        for name in self.STREAMS:
            virtual = name.startswith("virtual")
            source = f"{self.RESULT}.virtual_input" if virtual else f"{self.SOURCE}.normal_input"
            query_source = "%%trows" if name.endswith("rows") else source
            aggregate = "sum(v) + sum(w)" if virtual else "sum(v)"
            tdSql.execute(
                f"create stream {self.RESULT}.{name} period({self.PERIOD}s) from {source} "
                "stream_options(ignore_disorder | ignore_nodata_trigger) "
                f"into {self.RESULT}.{name}_out nodelay_create_subtable as "
                f"select cast(_tlocaltime / 1000000 as timestamp) ts, {aggregate} total "
                f"from {query_source}"
            )
        for name in self.STREAMS:
            tdStream.checkStreamStatus(name)
        tdSql.query(f"show {self.SOURCE}.vgroups")
        self.vg_id = tdSql.getData(0, 0)
        self.wal_path = os.path.join(tdDnodes.getDnodeDir(1), "data", "vnode", f"vnode{self.vg_id}", "wal")
        tdSql.execute(f"alter vgroup {self.vg_id} set keep 0")

    def _early_cycle(self):
        deadline = time.monotonic() + self.PERIOD + 5
        while time.monotonic() < deadline:
            now = time.time()
            if 2 <= now % self.PERIOD < 7:
                return (int(now) // self.PERIOD + 1) * self.PERIOD * 1000
            time.sleep(0.2)
        raise AssertionError("did not reach the beginning of a scheduled period")

    def _insert(self, ts, normal, left, right):
        tdSql.execute(f"insert into {self.SOURCE}.normal_input values ({ts}, {normal})")
        tdSql.execute(f"insert into {self.SOURCE}.left_source values ({ts}, {left})")
        tdSql.execute(f"insert into {self.SOURCE}.right_source values ({ts}, {right})")

    def _wal_meta(self):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            files = [path for path in glob.glob(os.path.join(self.wal_path, "meta-ver*")) if not path.endswith(".tmp")]
            for path in sorted(files, reverse=True):
                try:
                    with open(path, encoding="utf-8") as meta_file:
                        return json.load(meta_file)["meta"]
                except FileNotFoundError:
                    continue  # A roll/trim may replace every file from this listing.
            time.sleep(0.05)  # Rescan for the new metadata filename.
        return None

    def _rows(self, name):
        tdSql.query(f"select cast(ts as bigint), total from {self.RESULT}.{name}_out order by ts")
        return list(tdSql.queryResult)

    def _reclaim_before_tick(self, tick, previous_rows):
        # Allow the ordinary polling loop to fetch the early input. Continue checking
        # that collection itself produces no output; inspect WAL metadata for reclamation.
        poll_until = time.monotonic() + 8
        while time.monotonic() < poll_until:
            for name in self.STREAMS:
                assert len(self._rows(name)) == previous_rows, f"{name} fired before its boundary"
            time.sleep(0.5)
        tdSql.execute(f"flush database {self.SOURCE}")
        meta = self._wal_meta()
        assert meta is not None, f"missing WAL metadata in {self.wal_path}"
        target = max(int(meta["lastVer"]), int(meta["commitVer"]))
        assert target >= 0, meta
        while time.time() * 1000 < tick - 5000:
            # This unrelated table rolls the WAL without qualifying as stream input.
            tdSql.execute(f"insert into {self.SOURCE}.noise values (now, 1)")
            tdSql.execute(f"flush database {self.SOURCE}")
            tdSql.execute(f"trim database {self.SOURCE} wal")
            meta = self._wal_meta()
            if meta is not None and int(meta["firstVer"]) > target:
                tdLog.info(f"PERIOD input WAL reclaimed before tick {tick}: target={target}, meta={meta}")
                for name in self.STREAMS:
                    assert len(self._rows(name)) == previous_rows
                return
            time.sleep(0.5)
        raise AssertionError(f"WAL did not reclaim snapshot {target} before tick {tick}: {meta}")

    def _expect(self, tick, totals, row_count):
        deadline = time.monotonic() + max(0, tick / 1000 - time.time()) + 30
        while time.monotonic() < deadline:
            rows = {name: self._rows(name) for name in self.STREAMS}
            if all(len(value) == row_count for value in rows.values()):
                for name, total in zip(self.STREAMS, totals):
                    assert rows[name][-1] == (tick, total), (name, rows[name], tick, total)
                return
            time.sleep(0.5)
        raise AssertionError(f"missing PERIOD output at {tick}: {rows}")

    def _wait_for_binding_patch(self, rounds):
        # Metadata reaches the source Reader asynchronously. Wait for the real
        # SET_TABLE/patch round, so the new row cannot be filtered by the old Reader.
        tdSql.query(
            "select stream_id from information_schema.ins_streams "
            f"where db_name = '{self.RESULT}' and stream_name in ('virtual_direct', 'virtual_rows')"
        )
        stream_ids = {str(row[0]).lower().removeprefix("0x").lstrip("0") for row in tdSql.queryResult}
        assert len(stream_ids) == 2
        deadline = time.monotonic() + 15
        patched = {}
        while sum(count >= rounds for count in patched.values()) < 2 and time.monotonic() < deadline:
            patched.clear()
            for path in glob.glob(os.path.join(tdDnodes.getDnodeDir(1), "log", "taosdlog.*")):
                with open(path, encoding="utf-8", errors="replace") as log_file:
                    for line in log_file:
                        if "finished virtual table patch" in line:
                            match = re.search(r"STREAM:([0-9a-f]+)", line)
                            if match and match.group(1).lstrip("0") in stream_ids:
                                patched[match.group(1)] = patched.get(match.group(1), 0) + 1
            if sum(count >= rounds for count in patched.values()) < 2:
                time.sleep(0.2)
        assert sum(count >= rounds for count in patched.values()) == 2, f"virtual PERIOD patch did not finish: {patched}"

    def test_period_input_survives_wal_reclaim_and_restart(self):
        """PERIOD: continuous WAL input collection and restart

        1. Collect ordinary and cross-database virtual-table input for direct SQL and %%trows.
        2. Verify the input WAL is reclaimed before the scheduled calculation, then check results.
        3. Restart with unfinished input and verify it is not replayed into the next %%trows cycle.
        4. Reclaim fresh input WAL again and check the next scheduled results after restart.

        Catalog:
            - Stream:TriggerMode

        Since: v3.4.3.9

        Labels: common,ci,integration

        History:
            - 2026-11-04 Initial regression for long PERIOD WAL collection.
        """
        self._prepare()
        tick = self._early_cycle()
        event_ts = int(time.time() * 1000) - 100000
        self._insert(event_ts, 7, 11, 13)
        self._reclaim_before_tick(tick, 0)
        self._expect(tick, (7, 7, 24, 24), 1)

        # These rows remain queryable from TSDB but belong to an unfinished runtime
        # cycle. A direct query sees them after restart; %%trows must not replay them.
        unfinished_tick = self._early_cycle()
        self._insert(event_ts + 1, 100, 100, 100)
        self._reclaim_before_tick(unfinished_tick, 1)
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        for name in self.STREAMS:
            tdStream.checkStreamStatus(name)
        tick = self._early_cycle()
        for name in self.STREAMS:
            assert len(self._rows(name)) == 1, f"{name} replayed unfinished input after restart"
        self._insert(event_ts + 2, 9, 17, 19)
        self._reclaim_before_tick(tick, 1)
        self._expect(tick, (116, 9, 260, 36), 2)

    def test_period_rebind_preserves_collected_input(self):
        """PERIOD: preserve input across an active virtual-table binding change

        1. Collect and reclaim the old source WAL within one scheduled period.
        2. Rebind across sources and then within one source, collecting and reclaiming each new batch.
        3. Check one scheduled result with each batch interpreted using its own binding.

        Catalog:
            - Stream:TriggerMode

        Since: v3.4.3.9

        Labels: common,ci,integration

        History:
            - 2026-11-04 Cover active PERIOD binding changes.
        """
        self.PERIOD = 120
        self._prepare()
        tdSql.execute(f"create table {self.SOURCE}.replacement (ts timestamp, v int, w int)")
        tick = self._early_cycle()
        event_ts = int(time.time() * 1000) - 100000
        self._insert(event_ts, 7, 11, 13)
        self._reclaim_before_tick(tick, 0)
        tdSql.execute(
            f"alter vtable {self.RESULT}.virtual_input alter column v set {self.SOURCE}.replacement.v"
        )
        self._wait_for_binding_patch(1)
        tdSql.execute(f"insert into {self.SOURCE}.normal_input values ({event_ts + 1}, 9)")
        tdSql.execute(f"insert into {self.SOURCE}.replacement values ({event_ts + 1}, 17, 170)")
        tdSql.execute(f"insert into {self.SOURCE}.right_source values ({event_ts + 1}, 19)")
        self._reclaim_before_tick(tick, 0)
        tdSql.execute(
            f"alter vtable {self.RESULT}.virtual_input alter column v set {self.SOURCE}.replacement.w"
        )
        self._wait_for_binding_patch(2)
        tdSql.execute(f"insert into {self.SOURCE}.normal_input values ({event_ts + 2}, 5)")
        tdSql.execute(f"insert into {self.SOURCE}.replacement values ({event_ts + 2}, 23, 230)")
        tdSql.execute(f"insert into {self.SOURCE}.right_source values ({event_ts + 2}, 29)")
        self._reclaim_before_tick(tick, 0)
        self._expect(tick, (21, 21, 461, 319), 1)
