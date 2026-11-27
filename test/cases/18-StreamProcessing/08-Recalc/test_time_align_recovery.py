# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import time
from contextlib import closing
from pathlib import Path

import pytest
from taos import new_multi_binds

from new_test_framework.utils import clusterComCheck, sc, tdCom, tdDnodes, tdSql, tdStream


class TestTimeAlignRecovery:
    @classmethod
    def setup_class(cls):
        tdStream.ensureSnode(1)

    def _start(self, database, predicate, include_pressure=False, shared_pressure=False, seed_age_days=0, text=False,
               remote_source=None):
        tdSql.prepare(dbname=database, drop=True, vgroups=1)
        self.base = (int(time.time()) // 60 - 2) * 60000
        pressure_col = ", pressure int" if shared_pressure else ""
        value_type = "varchar(4)" if text else "int"
        temperatures = "temperatures"
        if remote_source:
            source_db = database + "_src"
            tdSql.execute(f"drop database if exists {source_db}")
            tdSql.execute(f"create database {source_db} vgroups 1")
            temperatures = f"{source_db}.temperatures"
        if remote_source == "child":
            tdSql.execute(f"create stable {source_db}.s (ts timestamp, v {value_type}) tags (site int)")
            tdSql.execute(f"create table {temperatures} using {source_db}.s tags (1)")
        else:
            tdSql.execute(f"create table {temperatures} (ts timestamp, v {value_type}{pressure_col})")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        seed_ts = self.base - seed_age_days * 86400000
        if shared_pressure:
            tdSql.execute(f"insert into temperatures values ({seed_ts - 1000}, 7, 17) ({seed_ts}, 10, 20)")
        elif text:
            tdSql.execute(f"insert into {temperatures} values ({seed_ts}, '10')")
        else:
            tdSql.execute(f"insert into temperatures values ({seed_ts - 1000}, 7) ({seed_ts}, 10)")
        fields = f"temp {value_type} from {temperatures}.v, beat int from heartbeats.beat"
        rules = "temp:PREV:_p"
        if shared_pressure:
            fields += ", pressure int from temperatures.pressure"
            rules += ";pressure:PREV:_p"
        if include_pressure:
            tdSql.execute("create table pressures (ts timestamp, v int)")
            tdSql.execute(f"insert into pressures values ({self.base}, 20)")
            fields += ", pressure int from pressures.v"
            rules += ";pressure:PREV:_p"
        tdSql.execute(f"create vtable meter (ts timestamp, {fields})")
        tdSql.execute(
            f"create stream heartbeat count_window(1) from table(time_align(table meter, true, '{rules}')) "
            f"stream_options(pre_filter({predicate})) into beats_out "
            "as select _twstart as ws, _twrownum as n from probe"
        )
        tdStream.checkStreamStatus("heartbeat")
        tdSql.query(f"select stream_id from information_schema.ins_streams where db_name='{database}' "
                    "and stream_name='heartbeat'")
        self.stream = format(int(str(tdSql.queryResult[0][0]), 16), "x")
        self.offsets = {}

    def teardown_method(self):
        tdSql.execute("drop stream if exists heartbeat")

    def _wait_log(self, *messages):
        directory = Path(tdDnodes.getDnodeDir(1)) / "log"
        next_message = 0
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            for path in sorted(directory.glob("taosdlog.[0-9]*"), key=lambda p: p.stat().st_mtime_ns):
                with path.open("rb") as log:
                    stat = path.stat()
                    offset = self.offsets.get(stat.st_ino, 0)
                    log.seek(offset if offset <= stat.st_size else 0)
                    for line in log:
                        if f"STREAM:{self.stream},".encode() in line and messages[next_message].encode() in line:
                            next_message += 1
                            if next_message == len(messages):
                                self.offsets[stat.st_ino] = log.tell()
                                return line.decode(errors="replace")
                    self.offsets[stat.st_ino] = log.tell()
            time.sleep(0.2)
        raise AssertionError(f"stream {self.stream} did not reach {messages[next_message]}")

    def _wait_binding_patch(self):
        # An earlier cache recheck can also finish a patch. Wait for the explicit
        # table-change WAL record and the completion that follows it.
        self._wait_log("found runtime virtual table add, uid:", "finished virtual table patch, externalEntries:")

    def _heartbeat(self, second):
        tdSql.execute(f"insert into heartbeats values ({self.base + second * 1000}, {second})")

    def _wait_output(self, second):
        tdSql.checkResultsByFunc(
            sql=f"select n from beats_out where ws={self.base + second * 1000}",
            func=lambda: list(tdSql.queryResult) == [(1,)],
            retry=40,
        )

    def _first_round(self):
        self._heartbeat(1)
        self._wait_output(1)
        # STRIGGER_PULL_TIME_ALIGN_RELEASE follows actual result submission.
        self._wait_log("tbname: beats_out, createTb:", "vnodeProcessStreamReaderMsg start, type:31,")

    def _expect_outputs(self, seconds):
        self._wait_output(seconds[-1])
        tdSql.query("select cast(ws as bigint), n from beats_out order by ws")
        assert list(tdSql.queryResult) == [(self.base + second * 1000, 1) for second in seconds]

    def test_normal_close_keeps_consumed_prev_after_ordinary_delete(self):
        """Use the consumed temperature on a new heartbeat after a normal round close.

        Wait for the first result and source release, then delete its ordinary
        temperature with deletion recalculation disabled. The second heartbeat
        must still see 10. The third independently demonstrates forward progress.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._start("ta_recovery_live", "beat=3 or (beat in (1, 2) and temp_p=10)")
        self._first_round()
        tdSql.execute(f"delete from temperatures where ts={self.base}")
        self._heartbeat(2)
        self._heartbeat(3)
        self._expect_outputs([1, 2, 3])

    @pytest.mark.parametrize("change, expected", [("delete", 7), ("update", 55)])
    def test_recovery_reads_current_history(self, change, expected):
        """Rebuild the previous value from visible history after a node restart.

        The first round observes temperature 10. Delete or overwrite it before
        restarting the node. The second heartbeat requires the older temperature
        7 after a delete, or 55 after an update. Seed rows produce no windows.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._start("ta_recovery_restart", f"(beat=1 and temp_p=10) or (beat=2 and temp_p={expected}) or beat=3")
        self._first_round()
        if change == "delete":
            tdSql.execute(f"delete from temperatures where ts={self.base}")
        else:
            tdSql.execute(f"insert into temperatures values ({self.base}, {expected})")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdStream.checkStreamStatus("heartbeat")
        self._heartbeat(2)
        self._heartbeat(3)
        self._expect_outputs([1, 2, 3])

    def test_restart_after_predecessor_expires(self):
        """Restart a stream after its consumed predecessor falls outside data retention.

        1. Flush two old temperatures and confirm a recent heartbeat still sees the last one.
        2. Shorten retention and verify a fresh query sees no temperature at the heartbeat.
        3. Restart the dedicated node and check both the NULL predecessor and a new valid reading.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        db = "ta_recovery_expired"
        self._start(db, "(beat=1 and temp_p=10) or (beat=2 and temp_p is null) or (beat=3 and temp_p=22)",
                    seed_age_days=600)
        tdSql.execute(f"flush database {db}")
        self._first_round()
        tdSql.execute(f"alter database {db} keep 365")
        tdSql.checkResultsByFunc(
            sql=("select temp_p from table(time_align(table meter, true, 'temp:PREV:_p')) "
                 f"where ts={self.base + 1000}"),
            func=lambda: list(tdSql.queryResult) == [(None,)],
            retry=45,
        )
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdStream.checkStreamStatus("heartbeat")
        self._heartbeat(2)
        self._wait_output(2)
        tdSql.execute(f"insert into temperatures values ({self.base + 2500}, 22)")
        self._heartbeat(3)
        self._expect_outputs([1, 2, 3])

    def test_rebind_and_set_null(self):
        """Rebind one column and clear it while keeping the other column's PREV.

        An ordinary deletion leaves an already consumed pressure of 20. Only
        temperature is rebound to a table containing 30, then another column in
        that table containing 45, then set to NULL. Verify all five heartbeats.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._start("ta_recovery_rebind",
                    "beat=5 or (pressure_p=20 and ((beat=1 and temp_p=10) or (beat=2 and temp_p=30) "
                    "or (beat=3 and temp_p=45) or (beat=4 and temp_p is null)))", include_pressure=True)
        self._first_round()
        tdSql.execute(f"delete from pressures where ts={self.base}")
        tdSql.execute("create table replacement (ts timestamp, v int, alternate int)")
        tdSql.execute(f"insert into replacement values ({self.base}, 30, 45)")
        tdSql.execute("alter vtable meter alter column temp set replacement.v")
        self._wait_binding_patch()
        self._heartbeat(2)
        self._wait_output(2)
        tdSql.execute("alter vtable meter alter column temp set replacement.alternate")
        self._wait_binding_patch()
        self._heartbeat(3)
        self._wait_output(3)
        tdSql.execute("alter vtable meter alter column temp set null")
        self._wait_binding_patch()
        self._heartbeat(4)
        self._heartbeat(5)
        self._expect_outputs([1, 2, 3, 4, 5])

    def test_manual_recalc_uses_rebound_source(self):
        """Recalculate a previously filtered heartbeat using the current reference.

        The original temperature of 10 rejects heartbeat 2. After the meter
        starts using a source with value 20, drop the old table. Heartbeat 4 passes immediately.
        Requesting a replay over the old range must now produce heartbeat 2.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._start("ta_history_rebind", "(beat=1 and temp_p=10) or beat=3 or (beat in (2,4) and temp_p=20)",
                    include_pressure=True)
        self._first_round()
        self._heartbeat(2)
        self._heartbeat(3)
        self._expect_outputs([1, 3])

        tdSql.execute("alter vtable meter alter column temp set pressures.v")
        self._wait_binding_patch()
        tdSql.execute("drop table temperatures")
        self._heartbeat(4)
        self._expect_outputs([1, 3, 4])

        tdSql.execute(f"recalculate stream ta_history_rebind.heartbeat from {self.base + 1000} to {self.base + 3000}")
        self._wait_output(2)

    def test_rebind_wide_prev(self):
        """Use the current flat virtual binding for a wide seed and live PREV.

        Create a stream while its string source is four bytes wide. After the
        reference changes, the next heartbeat must see the replacement seed.
        A later heartbeat must see the longer value inserted into that source.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._start("ta_recovery_wide", "(beat=1 and length(temp_p)=2) or (beat=2 and length(temp_p)=1200) "
                    "or (beat=3 and length(temp_p)=1400)", text=True)
        self._first_round()
        tdSql.execute("create table replacement (ts timestamp, v varchar(2048))")
        tdSql.execute(f"insert into replacement values ({self.base}, '{'s' * 1200}')")
        tdSql.execute("alter vtable meter alter column temp set replacement.v")
        self._wait_binding_patch()
        self._heartbeat(2)
        self._wait_output(2)
        tdSql.execute(f"insert into replacement values ({self.base + 2500}, '{'r' * 1400}')")
        self._heartbeat(3)
        self._expect_outputs([1, 2, 3])

    @pytest.mark.parametrize("source, transaction", [("normal", False), ("child", False), ("normal", True)],
                             ids=["normal", "child", "commit"])
    def test_widen_physical_prev_in_place(self, source, transaction):
        """Retain the previous value and use the wider physical column after MODIFY COLUMN.

        Widen the original table on another vgroup while the stream is active.
        Check its consumed predecessor, the first long reading, the following
        heartbeat, and a separately requested recalculation of that heartbeat.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = "ta_widen_commit" if transaction else f"ta_widen_{source}"
        table = f"{db}_src.temperatures"
        self._start(db, "(beat in (1,2) and temp_p='10') or "
                    "((beat is null or beat=4) and length(temp_p)=1200)", text=True, remote_source=source)
        self._first_round()
        tdSql.execute(f"delete from {table} where ts={self.base}")
        if transaction:
            tdSql.execute("begin")
        if source == "child":
            tdSql.execute(f"alter stable {db}_src.s modify column v varchar(2048)")
        else:
            tdSql.execute(f"alter table {table} modify column v varchar(2048)")
        if transaction:
            tdSql.execute("commit")
        self._heartbeat(2)
        self._wait_output(2)
        tdSql.execute(f"insert into {table} values ({self.base + 3000}, '{'v' * 1200}')")
        self._wait_output(3)
        self._heartbeat(4)
        self._expect_outputs([1, 2, 3, 4])

        tdSql.execute(f"delete from beats_out where ws={self.base + 4000}")
        tdSql.query(f"select n from beats_out where ws={self.base + 4000}")
        tdSql.checkRows(0)
        tdSql.execute(f"recalculate stream {db}.heartbeat from {self.base + 4000} to {self.base + 4001}")
        self._expect_outputs([1, 2, 3, 4])

    @pytest.mark.parametrize("virtual_width", [64, 128])
    def test_geometry_history_and_recalc(self, virtual_width):
        """Check the PREV geometry value during history fill and manual recalculation.

        Map two points from a remote vgroup into an equal or wider virtual column.
        Both heartbeats must select the right previous point in each history pass.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = f"ta_geometry_{virtual_width}"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        self.base = (int(time.time()) // 60 - 2) * 60000
        src = db + "_src"
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src} vgroups 1")
        tdSql.execute(f"create table {src}.shapes (ts timestamp, v geometry(64))")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        tdSql.execute(f"create vtable meter (ts timestamp, shape geometry({virtual_width}) from {src}.shapes.v, "
                      "beat int from heartbeats.beat)")
        tdSql.execute(f"insert into {src}.shapes values ({self.base + 500}, 'POINT (1 2)') "
                      f"({self.base + 2500}, 'POINT (3 4)')")
        self._heartbeat(1)
        self._heartbeat(3)
        tdSql.execute(
            "create stream heartbeat count_window(1) from table(time_align(table meter, true, 'shape:PREV:_p')) "
            f"stream_options(fill_history({self.base})|pre_filter(beat=1 and "
            "st_equals(shape_p, st_geomfromtext('POINT (1 2)'))=true or "
            "beat=3 and st_equals(shape_p, st_geomfromtext('POINT (3 4)'))=true)) into beats_out "
            "as select _twstart as ws, _twrownum as n from probe"
        )
        tdStream.checkStreamStatus("heartbeat")
        self._expect_outputs([1, 3])
        tdSql.execute(f"delete from beats_out where ws>={self.base + 1000}")
        tdSql.query("select ws from beats_out")
        tdSql.checkRows(0)
        tdSql.execute(f"recalculate stream {db}.heartbeat from {self.base} to {self.base + 3001}")
        self._expect_outputs([1, 3])

    def test_realtime_preserves_equal_logical_timestamps(self):
        """Keep the non-NULL sample when a live nanosecond source repeats a millisecond.

        Warm the running stream with a heartbeat. Submit both source tables in one
        vnode write, then compare the accepted heartbeats with a fresh history fill.
        The last heartbeat requires raw columns from both sources at the same tick.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = "ta_live_equal_ticks"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        self.base = (int(time.time()) // 60 - 2) * 60000
        src = db + "_ns"
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src} vgroups 1 precision 'ns'")
        tdSql.execute(f"create table {src}.samples (ts timestamp, v int, seq int)")
        tdSql.execute(f"create table {src}.beats (ts timestamp, v int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        tdSql.execute(f"create vtable meter (ts timestamp, sample int from {src}.samples.v, "
                      f"seq int from {src}.samples.seq, beat int from {src}.beats.v)")

        def start(options=""):
            tdSql.execute(
                "create stream heartbeat count_window(1) from table(time_align(table meter, true, 'sample:PREV:_p')) "
                f"stream_options({options}pre_filter(beat=0 or beat=2 and sample_p=37 or beat=3 and seq=3)) "
                "into beats_out as select _twstart as ws, _twrownum as n from probe"
            )
            tdStream.checkStreamStatus("heartbeat")

        start()
        ns = self.base * 1000000
        tdSql.execute(f"insert into {src}.beats values ({ns}, 0)")
        self._expect_outputs([0])
        tdSql.execute(f"insert into {src}.samples values ({ns + 1000000001}, 37, 1) "
                      f"({ns + 1000000002}, null, 2) ({ns + 3000000000}, null, 3) "
                      f"{src}.beats values ({ns + 2000000000}, 2) ({ns + 3000000000}, 3)")
        self._expect_outputs([0, 2, 3])
        tdSql.execute("drop stream heartbeat")
        tdSql.execute("drop table beats_out")
        start(f"fill_history({self.base})|")
        self._expect_outputs([0, 2, 3])

    def test_cross_wal_source_rows_and_updates(self):
        """Pair same-tick rows from two shards after distinct writes and an update.

        Hold a millisecond tick behind the watermark while writing two nanosecond
        samples. Correct one physical timestamp, preserve the other, and compare
        the filtered live count windows with a fresh history fill.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = "ta_wal_row_keys"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        self.base = (int(time.time()) // 60 - 2) * 60000
        src = db + "_ns"
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src} vgroups 1 precision 'ns'")
        tdSql.execute(f"create table {src}.samples (ts timestamp, v int, seq int)")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        tdSql.execute(f"create vtable meter (ts timestamp, sample int from {src}.samples.v, "
                      f"seq int from {src}.samples.seq, beat int from heartbeats.beat)")

        def start(options=""):
            tdSql.execute(
                "create stream heartbeat count_window(1) from table(time_align(table meter, true, 'sample:PREV:_p')) "
                f"stream_options({options}watermark(5s)|"
                "pre_filter(beat=0 or beat=11 and sample_p=37 or beat=20 and sample=37 "
                "or beat=21 and sample=38 or beat=30)) into beats_out as select _twstart as ws, _twrownum as n from probe"
            )
            tdStream.checkStreamStatus("heartbeat")

        start()
        self._heartbeat(0)
        self._heartbeat(6)
        self._wait_output(0)
        tick = (self.base + 10000) * 1000000
        tdSql.execute(f"insert into {src}.samples values ({tick + 1}, 36, 1)")
        tdSql.execute(f"insert into {src}.samples values ({tick + 1}, 37, 1)")
        tdSql.execute(f"insert into {src}.samples values ({tick + 2}, null, 2)")
        self._heartbeat(11)
        tick = (self.base + 20000) * 1000000
        tdSql.execute(f"insert into {src}.samples values ({tick + 1}, 37, 1) ({tick + 2}, 38, 2) "
                      f"({tick + 1000000001}, 37, 1) ({tick + 1000000002}, 38, 2)")
        for second in (20, 21, 30, 36):
            self._heartbeat(second)

        def output_seconds():
            self._wait_output(30)
            tdSql.query("select cast(ws as bigint), n from beats_out order by ws")
            return [(ts - self.base, n) for ts, n in tdSql.queryResult]

        live = output_seconds()
        # Both ticks contain identical physical rows. Exactly one complementary filter can pass.
        assert live in ([(0, 1), (11000, 1), (20000, 1), (30000, 1)],
                        [(0, 1), (11000, 1), (21000, 1), (30000, 1)])

        tdSql.execute("drop stream heartbeat")
        tdSql.execute("drop table beats_out")
        start(f"fill_history({self.base})|")
        assert output_seconds() == live

    def test_partial_wal_row_matches_history(self):
        """Merge independent updates to one physical row before applying PREV.

        1. Establish an earlier window and hold subsequent writes behind a watermark.
        2. Omit the sample in one update and explicitly clear it in another.
        3. Compare the following heartbeats with a fresh history fill.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = "ta_partial_wal"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        self.base = (int(time.time()) // 60 - 2) * 60000
        tdSql.execute("create table samples (ts timestamp, v int, seq int)")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        tdSql.execute("create vtable meter (ts timestamp, sample int from samples.v, "
                      "seq int from samples.seq, beat int from heartbeats.beat)")

        def start(options=""):
            tdSql.execute("create stream heartbeat count_window(1) from table(time_align(table meter, true, "
                          f"'sample:PREV:_p;seq:PREV:_p')) stream_options({options}watermark(5s)|"
                          "pre_filter(beat in (0, 30) or (beat in (11, 21) and sample_p=37 and seq_p=2))) "
                          "into beats_out as select _twstart as ws, _twrownum as n from probe")
            tdStream.checkStreamStatus("heartbeat")

        start()
        self._heartbeat(0)
        self._heartbeat(6)
        self._wait_output(0)
        for second, value in ((10, 37), (20, 99)):
            ts = self.base + second * 1000
            tdSql.execute(f"insert into samples values ({ts}, {value}, 1)")
            if second == 10:
                tdSql.execute(f"insert into samples (ts, seq) values ({ts}, 2)")
            else:
                tdSql.execute(f"insert into samples values ({ts}, null, 2)")
            self._heartbeat(second + 1)
        self._heartbeat(30)
        self._heartbeat(36)
        self._expect_outputs([0, 11, 21, 30])
        tdSql.execute("drop stream heartbeat")
        tdSql.execute("drop table beats_out")
        start(f"fill_history({self.base})|")
        self._expect_outputs([0, 11, 21, 30])

    @pytest.mark.parametrize("writer", ["sql", "stmt"])
    @pytest.mark.parametrize("same_cell", [False, True], ids=["neighbor", "exact"])
    def test_late_null_uses_physical_timestamp(self, writer, same_cell):
        """Probe the exact nanosecond of a retained sample on a millisecond stream.

        Consume and delete the sample, then write NULL at its timestamp or the next
        nanosecond. A later heartbeat distinguishes retaining it from falling back
        to the older visible sample. Exercise row and column submits.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = f"ta_ns_probe_{writer}_{int(same_cell)}"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        self.base = (int(time.time()) // 60 - 2) * 60000
        src = db + "_ns"
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src} vgroups 1 precision 'ns'")
        tdSql.execute(f"create table {src}.samples (ts timestamp, v int)")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        tdSql.execute(f"create vtable meter (ts timestamp, sample int from {src}.samples.v, "
                      "beat int from heartbeats.beat)")
        ts = self.base * 1000000 + 123
        tdSql.execute(f"insert into {src}.samples values ({ts - 1000000000}, 7)")
        expected = 7 if same_cell else 37
        tdSql.execute("create stream heartbeat count_window(1) from table(time_align(table meter, true, "
                      "'sample:PREV:_p')) stream_options(pre_filter(beat=0 or beat=1 and sample_p=37 or "
                      f"beat=2 and sample_p={expected} or beat=3)) into beats_out "
                      "as select _twstart as ws, _twrownum as n from probe")
        tdStream.checkStreamStatus("heartbeat")
        tdSql.query(f"select stream_id from information_schema.ins_streams where db_name='{db}' "
                    "and stream_name='heartbeat'")
        self.stream = format(int(str(tdSql.queryResult[0][0]), 16), "x")
        self.offsets = {}
        self._heartbeat(-1)
        tdSql.execute(f"insert into {src}.samples values ({ts}, 37)")
        self._first_round()
        tdSql.execute(f"delete from {src}.samples where ts={ts}")
        target = ts if same_cell else ts + 1
        timestamps = [ts - 500000000, target, ts + 500000000]
        if writer == "sql":
            values = " ".join(f"({t}, null)" for t in timestamps)
            tdSql.execute(f"insert into {src}.samples values {values}")
        else:
            with closing(tdCom.newcon(database=src)) as conn, closing(conn.statement(
                    "insert into samples values (?, ?)")) as stmt:
                params = new_multi_binds(2)
                params[0].timestamp(timestamps, precision=2)
                params[1].int([None] * len(timestamps))
                stmt.bind_param_batch(params)
                stmt.execute()
                assert stmt.affected_rows == len(timestamps)
        tdSql.execute(f"delete from {src}.samples where ts={ts}")
        self._heartbeat(2)
        self._heartbeat(3)
        self._expect_outputs([1, 2, 3])

    @pytest.mark.parametrize("value_row", [0, 4096], ids=["first-row", "first-row-of-next-page"])
    def test_history_preserves_equal_logical_timestamps(self, value_row):
        """Use the only non-NULL nanosecond sample on the next millisecond heartbeat.

        The first heartbeat opens the forward scan before the sample. Place its value at
        the start of a three-row page or just after a full page in the same logical tick.
        Check the following heartbeat during both history fill and manual recalculation.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = f"ta_equal_ticks_{value_row}"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        self.base = (int(time.time()) // 60 - 2) * 60000
        src = db + "_ns"
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src} vgroups 1 maxrows 4096 precision 'ns'")
        tdSql.execute(f"create table {src}.samples (ts timestamp, v int, seq int)")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        tdSql.execute(f"create vtable meter (ts timestamp, sample int from {src}.samples.v, "
                      "beat int from heartbeats.beat)")
        ns = (self.base + 1000) * 1000000
        for start in range(0, value_row + 3, 256):
            values = " ".join(f"({ns + idx}, {37 if idx == value_row else 'null'}, {idx})"
                              for idx in range(start, min(value_row + 3, start + 256)))
            tdSql.execute(f"insert into {src}.samples values {values}")
        self._heartbeat(0)
        self._heartbeat(2)
        tdSql.execute(
            "create stream heartbeat count_window(1) from table(time_align(table meter, true, 'sample:PREV:_p')) "
            f"stream_options(fill_history({self.base})|pre_filter(beat=2 and sample_p=37)) into beats_out "
            "as select _twstart as ws, _twrownum as n from probe"
        )
        tdStream.checkStreamStatus("heartbeat")
        self._expect_outputs([2])
        tdSql.execute(f"delete from beats_out where ws={self.base + 2000}")
        tdSql.query("select ws from beats_out")
        tdSql.checkRows(0)
        tdSql.execute(f"recalculate stream {db}.heartbeat from {self.base} to {self.base + 2001}")
        self._expect_outputs([2])

    def test_calendar_seed_reads_only_eligible_source_pages(self):
        """Count physical Reader pages for an all-NULL PREV source over two years.

        The unrelated column has 4500 samples at 600 days, 4500 at 100 days,
        and three at one day before the heartbeat. A second rule shares the
        target column. Count source-page requests through the first result.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        db = "ta_calendar_seed_pages"
        src = db + "_src"
        tdSql.prepare(dbname=db, drop=True, vgroups=1)
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src} vgroups 1 minrows 100 maxrows 256")
        tdSql.execute(f"create table {src}.samples (ts timestamp, v int, seq int)")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute("create table probe (ts timestamp, v int)")
        tdSql.execute(f"create vtable meter (ts timestamp, sample int from {src}.samples.v, "
                      "beat int from heartbeats.beat)")
        self.base = (int(time.time()) // 60 - 2) * 60000
        tdSql.execute(f"insert into probe values ({self.base}, 1)")
        for days, rows in [(600, 4500), (100, 4500), (1, 3)]:
            beginning = self.base - days * 86400000
            for start in range(0, rows, 256):
                values = " ".join(f"({beginning + i}, null, {i})" for i in range(start, min(rows, start + 256)))
                tdSql.execute(f"insert into {src}.samples values {values}")
        tdSql.query(f"show {src}.vgroups")
        vg = tdSql.queryResult[0][0]
        logs = Path(tdDnodes.getDnodeDir(1)) / "log"
        for beat, span, pages in [(1, "(1n)", 1), (2, "(1y)", 2), (3, "", 3)]:
            name = f"seed_pages_{beat}"
            tdSql.execute(
                f"create stream {name} count_window(1) from table(time_align(table meter, true, "
                f"'sample:PREV{span}:_p;sample:PREV(1s):_short')) "
                f"stream_options(pre_filter(beat={beat} and sample_p is null and sample_short is null)) "
                f"into {name}_out as select _twstart as ws, _twrownum as n from probe"
            )
            try:
                tdStream.checkStreamStatus(name)
                tdSql.query(f"select stream_id from information_schema.ins_streams where db_name='{db}' "
                            f"and stream_name='{name}'")
                stream = format(int(str(tdSql.queryResult[0][0]), 16), "x")
                self._heartbeat(beat)
                tdSql.checkResultsByFunc(
                    sql=f"select cast(ws as bigint), n from {name}_out",
                    func=lambda: list(tdSql.queryResult) == [(self.base + beat * 1000, 1)],
                    retry=40,
                )
                # On the remote vgroup there is no new WAL sample or forward history body.
                # These are its predecessor DATA and DATA_NEXT requests for this stream.
                counts = {26: 0, 27: 0}
                for path in logs.glob("taosdlog.[0-9]*"):
                    with path.open(errors="replace") as log:
                        for line in log:
                            if f"STREAM:{stream}," in line:
                                for kind in counts:
                                    if f"vgId:{vg} vnodeProcessStreamReaderMsg start, type:{kind}," in line:
                                        counts[kind] += 1
                assert counts == {26: 1, 27: pages - 1}, (span, vg, counts)
            finally:
                tdSql.execute(f"drop stream if exists {name}")

    def test_rebind_keeps_wal_projection_bounded(self):
        """Keep the Reader projection at its initial width across eight column changes.

        Each replacement column contains a distinct temperature. Compare the width
        of the installed Reader blocks and check every emitted heartbeat, including
        the pressure predicate on a binding that remains active throughout.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-22 OpenAI Created
        """
        self._start("ta_recovery_projection", "pressure_p=20 and ((beat=1 and temp_p=10) or "
                    "(beat between 2 and 9 and temp_p=beat+100))", include_pressure=True)
        marker = "virtual table reader is ready after set table, triggerWalCols:"
        width = self._wait_log(marker).split(marker)[1].strip()
        raw_columns = int(width.split(",")[0])
        self._first_round()
        columns = ", ".join(f"v{n} int" for n in range(2, 10))
        values = ", ".join(str(n + 100) for n in range(2, 10))
        tdSql.execute(f"create table replacement (ts timestamp, {columns})")
        tdSql.execute(f"insert into replacement values ({self.base}, {values})")
        for n in range(2, 10):
            tdSql.execute(f"alter vtable meter alter column temp set replacement.v{n}")
            installed = self._wait_log("found runtime virtual table add, uid:", marker)
            assert installed.split(marker)[1].strip() == width
            self._wait_log("finished virtual table patch, externalEntries:")
            self._heartbeat(n)
            self._wait_log(f", walCols:{raw_columns}")
            self._wait_output(n)
        self._expect_outputs(list(range(1, 10)))

    def test_inflight_query_keeps_snapshot(self):
        """Keep the original physical column for a query opened before a rebind.

        Read the first row from the aligned virtual table, rebind its temperature
        column on a second connection, then consume the rest of the first result.
        A newly executed query must use the replacement source for every row.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        database = "ta_recovery_query"
        tdSql.prepare(dbname=database, drop=True, vgroups=1, keep=36500)
        base = 1767225600000  # 2026-01-01 00:00:00 UTC
        tdSql.execute("create table original (ts timestamp, v int)")
        tdSql.execute("create table replacement (ts timestamp, v int)")
        tdSql.execute("create table heartbeats (ts timestamp, beat int)")
        tdSql.execute(f"insert into original values ({base}, 10)")
        tdSql.execute(f"insert into replacement values ({base}, 30)")
        tdSql.execute("create vtable meter (ts timestamp, temp int from original.v, beat int from heartbeats.beat)")
        count = 12000
        for start in range(0, count, 500):
            values = " ".join(f"({base + i}, {i})" for i in range(start, start + 500))
            tdSql.execute(f"insert into heartbeats values {values}")

        query = "select beat, temp_p from table(time_align(table meter, true, 'temp:PREV:_p')) order by ts"
        with closing(tdCom.newcon(database=database)) as conn, closing(conn.cursor()) as cur:
            cur.execute(query)
            assert next(cur) == (0, 10)
            tdSql.execute("alter vtable meter alter column temp set replacement.v")
            for i in range(1, count):
                assert next(cur) == (i, 10)
            assert next(cur, None) is None
        tdSql.query(query)
        assert list(tdSql.queryResult) == [(i, 30) for i in range(count)]

    @pytest.mark.parametrize("writer", ["sql", "stmt"])
    @pytest.mark.parametrize("changed", ["sparse", "temp", "pressure"])
    def test_sparse_disorder(self, changed, writer):
        """Invalidate only the PREV cells actually rewritten by a late submit.

        Two aligned columns share a physical row. After consuming and deleting
        it, submit NULLs on either side of its timestamp. An additional NULL at
        that timestamp clears only the named column. The heartbeat predicates
        expose the cached and older values. A second ordinary delete leaves the
        same visible storage state for all three submissions.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        temp, pressure = {"sparse": (10, 20), "temp": (7, 20), "pressure": (10, 17)}[changed]
        database = f"ta_sparse_{changed}_{writer}"
        self._start(database,
                    f"(beat=1 and temp_p=10 and pressure_p=20) or "
                    f"(beat=2 and temp_p={temp} and pressure_p={pressure}) or beat=3",
                    shared_pressure=True)
        self._first_round()
        tdSql.execute(f"delete from temperatures where ts={self.base}")
        column = "pressure" if changed == "pressure" else "v"
        offsets = [-500, 500] if changed == "sparse" else [-500, 0, 500]
        timestamps = [self.base + offset for offset in offsets]
        insert = f"insert into temperatures (ts, {column}) values"
        if writer == "sql":
            tdSql.execute(insert + " " + " ".join(f"({ts}, null)" for ts in timestamps))
        else:
            with closing(tdCom.newcon(database=database)) as conn, closing(conn.statement(insert + "(?, ?)")) as stmt:
                params = new_multi_binds(2)
                params[0].timestamp(timestamps)
                params[1].int([None] * len(timestamps))
                stmt.bind_param_batch(params)
                stmt.execute()
                assert stmt.affected_rows == len(timestamps)
        tdSql.execute(f"delete from temperatures where ts={self.base}")
        self._heartbeat(2)
        self._heartbeat(3)
        self._expect_outputs([1, 2, 3])
