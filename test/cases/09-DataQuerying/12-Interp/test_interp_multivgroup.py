# -*- coding: utf-8 -*-
#
# INTERP with a pushed-down RANGE over a super table, on both the
# single-vgroup (local merge scan) and multi-vgroup (central Merge) shapes.
#
# Behaviour under test:
#   - The scan runs ONE continuous execution emitting MAIN(ASC) -> NEXT(ASC)
#     -> PREV(DESC raw rows), returning NULL only at the very end (the sub-task
#     completes exactly once -- NO dynamicTask, NO keep-alive). The scan never
#     judges "validity"; it only emits raw rows in the requested window/dir.
#   - Each block carries its phase in SDataBlockInfo.scanPhase (packed into the
#     wire flagSegment bit 30-29). The coordinator Exchange demuxes by scanPhase
#     and Merge rebuilds a per-phase losers tree (PREV=DESC). interp judges
#     reference validity (IGNORE NULL / expression / SURROUNDING) itself.
#
# Scope of this case:
#   - Literal result group and timestamp grids for optimized interp plans.
#   - The target single- and multi-vgroup plan shapes via EXPLAIN.
#   - The remote phase barrier and early-stop bridge, including all
#     single-source Exchanges, pending delivery, remote consumption, PREV
#     descending order, phase-pure output, and one final completion per task.
#
# Determinism note: rows sharing a sort-key timestamp have UNDEFINED candidate
# order. This case gives every source row a UNIQUE timestamp (per-table
# millisecond offset) so the interpolation source is unambiguous.
#
# Env note: taosd must be startable (port 6030 free). The framework fixture owns
# cluster bring-up (multi-vnode); this file only defines the case.

import re
import time
from datetime import datetime, timedelta

from new_test_framework.utils import ci_scaled_retry, tdLog, tdSql
from new_test_framework.utils.log_reader import LogReader
from new_test_framework.utils.srvCtl import sc

# Runtime protocol coverage requires exactly three logical vgroups. CI runs
# three dnodes as well so the vgroups exercise cross-dnode Exchange traffic.
RUNTIME_VGROUPS = 3


class TestInterpMultiVgroup:
    updatecfgDict = {
        "asynclog": 0,
        "cDebugFlag": 131,
        "dDebugFlag": 131,
        "debugFlag": 131,
        "jniDebugFlag": 131,
        "mDebugFlag": 131,
        "qDebugFlag": 132,
        "rpcDebugFlag": 131,
        "sDebugFlag": 131,
        "smaDebugFlag": 131,
        "stDebugFlag": 131,
        "tmrDebugFlag": 131,
        "tqDebugFlag": 131,
        "uDebugFlag": 131,
        "vDebugFlag": 131,
        "wDebugFlag": 131,
    }

    STB = "stb"
    NCHILD = 6
    BASE = "2020-02-01 00:00:"
    # Scalar-function argument. The physical planner extracts abs(c1 - 30)
    # into InterpFuncPhysiNode pExprs and pushes its output slot into the
    # child's block descriptor, so the plan shape must not change. c1 spans
    # 5..55 and folds at 30, which falls inside the RANGE gap (20s..35s) --
    # applying abs after interpolation would give different values, so this
    # also pins the per-source-row evaluation order.
    EXPR = "interp(abs(c1 - 30))"
    COLUMN_PARTITION = "c1 % 2"
    MIXED_TAG_PARTITION = "grp, c1 % 2"
    MIXED_TBNAME_PARTITION = "tbname, c1 % 2"
    RUNTIME_DB = "t0_remote"
    RUNTIME_SPARSE_DB = "t0_remote_sparse"
    RUNTIME_PREV_DB = "t0_remote_prev"
    RUNTIME_NEXT_ROWS = 90000
    RUNTIME_PREV_ROWS = 180000
    RUNTIME_PREV_START = datetime(2020, 1, 31, 23, 57, 20)
    RUNTIME_BATCH_ROWS = 1000
    # Edge-region seconds only, so RANGE(20s,35s) has lead/trail holes whose
    # references live strictly in the pruned PREV/NEXT side windows.
    EDGE_SECS = (5, 10, 15, 40, 45, 50)
    # Both ordinary-column groups must exist in MAIN before side references
    # can contribute to the phased result.
    MAIN_SECS = (26, 27)

    def _setup_db(self, db, vgroups):
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups {vgroups} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create stable {self.STB} (ts timestamp, c1 int, c2 double) "
            f"tags (grp int)"
        )
        for i in range(self.NCHILD):
            tdSql.execute(f"create table ct{i} using {self.STB} tags ({i % 2})")
            for s in self.EDGE_SECS + self.MAIN_SECS:
                # Unique per-row timestamp avoids ties across subtables.
                ts = f"{self.BASE}{s:02d}.{i:03d}"
                tdSql.execute(
                    f"insert into ct{i} values ('{ts}', {s + i}, {float(s + i)})"
                )

    @staticmethod
    def _runtime_next_timestamp(row):
        total_ms = 40000 + row
        minute = total_ms // 60000
        second = (total_ms // 1000) % 60
        millis = total_ms % 1000
        return (
            f"2020-02-01 00:{minute:02d}:{second:02d}."
            f"{millis:03d}"
        )

    @classmethod
    def _runtime_prev_timestamp(cls, row):
        ts = cls.RUNTIME_PREV_START + timedelta(milliseconds=row)
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _insert_runtime_side_rows(self, table, row_count, timestamp_fn):
        for start in range(0, row_count, self.RUNTIME_BATCH_ROWS):
            end = min(start + self.RUNTIME_BATCH_ROWS, row_count)
            values = []
            for row in range(start, end):
                ts = timestamp_fn(row)
                values.append(f"('{ts}', {row}, {float(row)})")
            tdSql.execute(
                f"insert into {table} values " + " ".join(values)
            )

    def _insert_runtime_rows(self, table, early_stop_phase):
        if early_stop_phase == 2:
            tdSql.execute(
                f"insert into {table} values "
                "('2020-02-01 00:00:26.000', 26, 26.0) "
                "('2020-02-01 00:00:27.000', 27, 27.0) "
                "('2020-02-01 00:00:40.000', 40, 40.0)"
            )
            row_count = self.RUNTIME_PREV_ROWS
            timestamp_fn = self._runtime_prev_timestamp
        else:
            tdSql.execute(
                f"insert into {table} values "
                "('2020-02-01 00:00:15.000', -1, -1.0) "
                "('2020-02-01 00:00:26.000', 26, 26.0) "
                "('2020-02-01 00:00:27.000', 27, 27.0)"
            )
            row_count = self.RUNTIME_NEXT_ROWS
            timestamp_fn = self._runtime_next_timestamp

        self._insert_runtime_side_rows(
            table, row_count, timestamp_fn
        )

    @staticmethod
    def _runtime_table_grp(table_name):
        """The grp tag value _setup_runtime_db gave table rt<index>."""
        return int(table_name[len("rt"):]) % 2

    def _runtime_tables_by_vgroup(self, db):
        tdSql.query(
            "select table_name, vgroup_id from "
            "information_schema.ins_tables "
            f"where db_name = '{db}' and stable_name = '{self.STB}' "
            "order by vgroup_id, table_name"
        )
        tables_by_vgroup = {}
        for table_name, vgroup_id in tdSql.queryResult:
            tables_by_vgroup.setdefault(
                int(vgroup_id), []
            ).append(str(table_name))
        assert len(tables_by_vgroup) == RUNTIME_VGROUPS, (
            f"runtime dataset must span {RUNTIME_VGROUPS} vgroups, got "
            f"{tables_by_vgroup}"
        )
        return tables_by_vgroup

    def _setup_runtime_db(self, empty_source=False, early_stop_phase=1):
        if early_stop_phase == 2:
            db = self.RUNTIME_PREV_DB
        else:
            db = self.RUNTIME_SPARSE_DB if empty_source else self.RUNTIME_DB
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} vgroups {RUNTIME_VGROUPS} keep 36500 "
            "minrows 10 maxrows 200"
        )
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create stable {self.STB} "
            "(ts timestamp, c1 int, c2 double) tags (grp int)"
        )
        for table_index in range(30):
            tdSql.execute(
                f"create table rt{table_index} using {self.STB} "
                f"tags ({table_index % 2})"
            )

        tables_by_vgroup = self._runtime_tables_by_vgroup(db)
        # Populate one table per vgroup, alternating the grp tag. Table
        # placement is hash driven, so picking each vgroup's first table
        # would leave the number of outer groups up to the hash and make
        # the mixed-partition scenarios silently degenerate to one group.
        populated_tables = []
        for ordinal, vgroup_id in enumerate(sorted(tables_by_vgroup)):
            tables = tables_by_vgroup[vgroup_id]
            wanted_grp = ordinal % 2
            populated_tables.append(next(
                (name for name in tables
                 if self._runtime_table_grp(name) == wanted_grp),
                tables[0]
            ))
        if empty_source:
            populated_tables.pop()
        populated_grps = {
            self._runtime_table_grp(name) for name in populated_tables
        }
        assert len(populated_grps) == 2, (
            "runtime dataset must populate both grp values so mixed "
            f"partitions span two outer groups, got {populated_tables}"
        )
        for table in populated_tables:
            self._insert_runtime_rows(table, early_stop_phase)
        row_count = (
            self.RUNTIME_PREV_ROWS
            if early_stop_phase == 2
            else self.RUNTIME_NEXT_ROWS
        )
        phase_name = "PREV" if early_stop_phase == 2 else "NEXT"
        tdLog.info(
            "runtime dataset ready: "
            f"{row_count} {phase_name} rows on each of "
            f"{len(populated_tables)} populated vgroups, "
            f"{len(tables_by_vgroup) - len(populated_tables)} empty"
        )
        return len(tables_by_vgroup), len(populated_tables)

    def _make_runtime_main_complete(self, db):
        tdSql.execute(f"use {db}")
        tables_by_vgroup = self._runtime_tables_by_vgroup(db)
        for tables in tables_by_vgroup.values():
            self._insert_runtime_side_rows(
                tables[0], self.RUNTIME_NEXT_ROWS,
                self._runtime_next_timestamp
            )

        # One vgroup supplies global MAIN boundaries; every vgroup keeps
        # large side inputs so both notifications must reach every scan.
        boundary_tables = next(iter(tables_by_vgroup.values()))
        assert len(boundary_tables) >= 2, boundary_tables
        for group, table in enumerate(boundary_tables[:2]):
            tdSql.execute(
                f"insert into {table} values "
                f"('2020-02-01 00:00:20.000', {group}, {float(group)}) "
                f"('2020-02-01 00:00:35.000', {group}, {float(group)})"
            )
        tdLog.info(
            "runtime MAIN-complete dataset ready: both column groups are "
            f"bounded in MAIN and {self.RUNTIME_NEXT_ROWS} NEXT rows exist "
            f"on each of {len(tables_by_vgroup)} vgroups"
        )

    @staticmethod
    def _collapsed(values):
        result = []
        for value in values:
            if not result or result[-1] != value:
                result.append(value)
        return result

    def _check_remote_runtime_evidence(
        self, evidence, source_count, notify_source_count,
        notify_phases, propagated_phases, expected_output_phases,
        interp_mode, expected_target_hops, outer_epoch_count
    ):
        runtime_re = re.compile(
            r"(TID:0x[0-9a-f]+ QID:0x[0-9a-f]+) "
            rf"TimeSlice runtime interpMode={interp_mode} "
            r"scanKind=other scanInterpMode=-1"
        )
        runtime = runtime_re.search(evidence)
        assert runtime is not None, (
            f"missing coordinator interpMode={interp_mode} route"
        )
        task_id = runtime.group(1)
        qid = task_id.split(" ")[1]

        if interp_mode == 0:
            task_lines = [line for line in evidence.splitlines()
                          if qid in line]
            assert not any("MAIN_FIRST early-stop" in line
                           for line in task_lines), task_lines
            phase_re = re.compile(
                re.escape(task_id)
                + r" opened merge phase:(\d+) order:(asc|desc) "
                + r"sources:(\d+)"
            )
            assert phase_re.findall(evidence) == [
                ("0", "asc", str(source_count))
            ], task_lines
            return []

        early_stop_re = re.compile(
            re.escape(task_id)
            + r" TimeSlice MAIN_FIRST early-stop phase=(\d+) "
            + rf"target=merge_exchange hops={expected_target_hops} "
            + r"notifyTs=-?\d+ scanGroupId=0 outerEpoch=(\d+)"
        )
        observed_notifications = [
            (int(phase), int(epoch))
            for phase, epoch in early_stop_re.findall(evidence)
        ]
        observed_notify_phases = [
            phase for phase, _ in observed_notifications
        ]
        expected_notify_phases = (
            list(notify_phases) * outer_epoch_count
        )
        assert observed_notify_phases == expected_notify_phases, (
            "unexpected coordinator early-stop phases: "
            f"{observed_notify_phases}"
        )
        outer_epochs = []
        phases_per_epoch = len(notify_phases)
        for index in range(outer_epoch_count):
            begin = index * phases_per_epoch
            end = begin + phases_per_epoch
            epoch_values = {
                epoch
                for _, epoch in observed_notifications[begin:end]
            }
            assert len(epoch_values) == 1, (
                "one outer group used multiple epochs: "
                f"{observed_notifications[begin:end]}"
            )
            outer_epochs.append(epoch_values.pop())
        if outer_epoch_count == 1:
            assert outer_epochs == [0], (
                f"non-outer scan used epoch: {outer_epochs}"
            )
        else:
            assert all(outer_epochs), (
                f"mixed outer group used zero epoch: {outer_epochs}"
            )
            assert len(set(outer_epochs)) == outer_epoch_count, (
                f"outer groups reused an epoch: {outer_epochs}"
            )

        phase_re = re.compile(
            re.escape(task_id)
            + r" opened merge phase:(\d+) order:(asc|desc) "
            + r"sources:(\d+)"
        )
        phases = [
            (int(phase), order, int(sources))
            for phase, order, sources in phase_re.findall(evidence)
        ]
        phase_cycle = [
            (0, "asc", source_count),
            (1, "asc", source_count),
            (2, "desc", source_count),
        ]
        expected_phases = phase_cycle * outer_epoch_count
        if outer_epoch_count > 1:
            # outerGroupPhaseMode opens a MAIN handle at operator open with
            # a placeholder epoch, before any source reveals the first real
            # outer group. The first cursor advance discards that bootstrap
            # handle and reopens MAIN for the real epoch, so one extra
            # phase-0 barrier precedes the per-epoch cycles.
            expected_phases.insert(0, phase_cycle[0])
        assert phases == expected_phases, (
            f"unexpected merge phase barriers: {phases}"
        )

        output_re = re.compile(
            re.escape(task_id)
            + r" merged phase:(\d+) groupId:0x[0-9a-f]+ rows:(\d+)"
        )
        output_phases = [
            int(phase)
            for phase, rows in output_re.findall(evidence)
            if int(rows) > 0
        ]
        expected_outputs = (
            list(expected_output_phases) * outer_epoch_count
        )
        assert self._collapsed(output_phases) == expected_outputs, (
            f"merge output phases are not pure and monotonic: {output_phases}"
        )

        final_null = (
            f"{task_id} merge phases complete final-null phase:2"
        )
        assert evidence.count(final_null) == 1, (
            "coordinator Merge must return one final NULL"
        )

        phase_counts = []
        consumed_total = 0
        consumed_by_remote_phase = {}
        for outer_epoch in outer_epochs:
            for notify_phase in propagated_phases:
                stored_re = re.compile(
                    re.escape(task_id)
                    + r" Exchange stored notify exchange:"
                    + r"(0x[0-9a-f]+) target:table_merge "
                    + rf"outerEpoch:{outer_epoch} "
                    + rf"phase:{notify_phase} scanGroupId:0 pending:1"
                )
                stored = stored_re.findall(evidence)
                stored_exchanges = set(stored)
                if outer_epoch_count == 1:
                    assert len(stored_exchanges) == notify_source_count, (
                        "not all Exchanges stored phase "
                        f"{notify_phase}: {stored}"
                    )
                else:
                    # An outer group need not span every vgroup, so only the
                    # Exchanges holding its data store the notification.
                    assert (
                        0 < len(stored_exchanges) <= notify_source_count
                    ), (
                        "unexpected Exchange stores for epoch "
                        f"{outer_epoch} phase {notify_phase}: {stored}"
                    )
                assert len(stored) == len(stored_exchanges), (
                    "an Exchange stored a notification more than once: "
                    f"{stored}"
                )

                attached_re = re.compile(
                    re.escape(task_id)
                    + r" Exchange attached notify exchange:"
                    + r"(0x[0-9a-f]+) target:table_merge "
                    + rf"outerEpoch:{outer_epoch} "
                    + rf"phase:{notify_phase} scanGroupId:0 "
                    + r"pending:0 source:0 remoteTask:(0x[0-9a-f]+)"
                )
                attached = attached_re.findall(evidence)
                attached_exchanges = {
                    exchange for exchange, _ in attached
                }
                assert len(attached) == len(attached_exchanges), (
                    "an Exchange attached a notification more than once: "
                    f"{attached}"
                )
                assert attached_exchanges.issubset(stored_exchanges), (
                    "an Exchange attached a notification it did not store"
                )
                remote_tasks = {task for _, task in attached}
                assert len(remote_tasks) == len(attached), (
                    f"expected one remote task per Exchange: {attached}"
                )

                consumed_count = 0
                for remote_task in remote_tasks:
                    remote_id = f"TID:{remote_task} {qid}"
                    consumed_re = re.compile(
                        re.escape(remote_id)
                        + r" MAIN_FIRST early-stop consumed "
                        + r"target=table_merge notifyTs=-?\d+ "
                        + rf"outerEpoch={outer_epoch} "
                        + rf"phase={notify_phase} scanGroupId=0"
                    )
                    consumed = len(consumed_re.findall(evidence))
                    assert consumed <= 1, (
                        "remote notification was consumed more than once: "
                        f"{remote_id}"
                    )
                    remote_phase = (remote_id, notify_phase)
                    consumed_by_remote_phase[remote_phase] = (
                        consumed_by_remote_phase.get(remote_phase, 0)
                        + consumed
                    )
                    consumed_count += consumed

                if interp_mode == 2 and outer_epoch_count == 1:
                    assert len(attached) == notify_source_count, (
                        "not every Exchange attached the multi-group notify"
                    )
                    assert consumed_count == notify_source_count, (
                        "not every remote MergeScan consumed the notification"
                    )
                elif outer_epoch_count > 1:
                    assert attached_exchanges == stored_exchanges, (
                        "a mixed-group notification remained pending"
                    )

                consumed_total += consumed_count
                phase_counts.append(
                    (
                        outer_epoch, notify_phase,
                        len(stored_exchanges), len(attached),
                        consumed_count
                    )
                )

        # The mixed skip log has no epoch, and one remote task can serve
        # several epochs. Compare exact counts only after aggregating epochs.
        group_part = "" if interp_mode == 2 else " group"
        scan_group_part = (
            "" if interp_mode == 2 else " scanGroupId:0"
        )
        for remote_phase, consumed_count in (
            consumed_by_remote_phase.items()
        ):
            remote_id, notify_phase = remote_phase
            skipped = (
                f"{remote_id} MAIN_FIRST early-stop skipped "
                f"table_merge{group_part} phase:{notify_phase}"
                f"{scan_group_part}"
            )
            assert evidence.count(skipped) == consumed_count, (
                "remote consume/skip evidence differs: "
                f"{remote_id} phase {notify_phase}"
            )

        if outer_epoch_count > 1:
            assert consumed_total > 0, (
                "no nonzero outer epoch reached a remote MergeScan"
            )

        return phase_counts

    def _assert_remote_runtime_protocol(
        self, db, source_count, notify_source_count,
        fill="linear", notify_phases=(1,), output_phases=(0, 1, 2),
        partition=None, interp_mode=1, target_hops=0,
        propagated_phases=None, outer_epoch_count=1
    ):
        if propagated_phases is None:
            propagated_phases = notify_phases
        log_patterns = [
            sc.dnodeLogPath(index) + "/taosdlog.*"
            for index in range(1, self.dnodeNum + 1)
        ]
        log_reader = LogReader(log_patterns)
        tdSql.query(self._interp_sql(db, fill, partition))
        assert tdSql.queryRows > 0, "runtime protocol query returned no rows"

        deadline = time.monotonic() + ci_scaled_retry(10)
        evidence = ""
        failure = "runtime evidence not available"
        while time.monotonic() < deadline:
            evidence += log_reader.read()
            try:
                phase_counts = self._check_remote_runtime_evidence(
                    evidence, source_count, notify_source_count,
                    notify_phases, propagated_phases, output_phases,
                    interp_mode, target_hops, outer_epoch_count
                )
                counts = ", ".join(
                    f"epoch {epoch} phase {phase}="
                    f"{stored}/{attached}/{consumed}"
                    for epoch, phase, stored, attached, consumed
                    in phase_counts
                )
                if interp_mode == 0:
                    tdLog.info(
                        "distributed partition runtime OK: LEGACY merge "
                        "without phased notifications"
                    )
                else:
                    tdLog.info(
                        "remote runtime protocol OK: three phase barriers, "
                        f"notifications stored/attached/consumed: {counts}"
                    )
                return
            except AssertionError as error:
                failure = str(error)
            time.sleep(0.1)

        raise AssertionError(
            f"{failure}\nlog patterns: {log_patterns}\n"
            f"last log access error: {log_reader.last_error or 'none'}\n"
            f"evidence bytes: {len(evidence)}\n"
            f"query log tail:\n{evidence[-8000:]}"
        )

    def _interp_sql(self, db, fill, partition, expr="interp(c1)"):
        # Project the partition key so literal result groups are identifiable;
        # RANGE spans the middle gap (20s..35s).
        if partition:
            proj = f"{partition}, _irowts, {expr}"
            part = f"partition by {partition} "
        else:
            proj = f"_irowts, {expr}"
            part = ""
        return (
            f"select {proj} from {db}.{self.STB} {part}"
            f"range('{self.BASE}20', '{self.BASE}35') every(3s) fill({fill})"
        )

    @staticmethod
    def _normalized(rows):
        # Order-independent multiset: main-first emits groups in completion order.
        return sorted([tuple(str(c) for c in r) for r in rows])

    def _assert_three_node_cluster(self):
        assert self.dnodeNum == RUNTIME_VGROUPS, (
            "remote early-stop E2E must run with pytest -N 3; "
            f"got {self.dnodeNum} dnodes"
        )

    def _query_rows(self, db, fill, partition, expr="interp(c1)"):
        tdSql.query(self._interp_sql(db, fill, partition, expr))
        return self._normalized(tdSql.queryResult)

    def _assert_result_grid(
        self, db, fill, partition, expr="interp(c1)"
    ):
        rows = self._query_rows(db, fill, partition, expr)
        expected_groups = {
            None: {()},
            "grp": {("0",), ("1",)},
            self.COLUMN_PARTITION: {("0.0",), ("1.0",)},
            self.MIXED_TAG_PARTITION: {
                ("0", "0.0"),
                ("0", "1.0"),
                ("1", "0.0"),
                ("1", "1.0"),
            },
            self.MIXED_TBNAME_PARTITION: {
                ("ct0", "0.0"),
                ("ct0", "1.0"),
                ("ct1", "0.0"),
                ("ct1", "1.0"),
                ("ct2", "0.0"),
                ("ct2", "1.0"),
                ("ct3", "0.0"),
                ("ct3", "1.0"),
                ("ct4", "0.0"),
                ("ct4", "1.0"),
                ("ct5", "0.0"),
                ("ct5", "1.0"),
            },
        }[partition]
        key_columns = 0 if partition is None else partition.count(",") + 1
        expected_times = [
            "2020-02-01 00:00:20",
            "2020-02-01 00:00:23",
            "2020-02-01 00:00:26",
            "2020-02-01 00:00:29",
            "2020-02-01 00:00:32",
            "2020-02-01 00:00:35",
        ]
        actual = {}
        for row in rows:
            key = tuple(row[:key_columns])
            actual.setdefault(key, []).append(row[key_columns])
        assert set(actual) == expected_groups, (
            f"[{db}] fill({fill}) partition({partition}) {expr}: "
            f"expected groups {expected_groups}, got {set(actual)}"
        )
        for group, times in actual.items():
            assert times == expected_times, (
                f"[{db}] group {group}: expected {expected_times}, "
                f"got {times}"
            )
        tdLog.info(
            f"result grid OK: [{db}] fill({fill}) "
            f"partition({partition}) {expr}, {len(rows)} rows"
        )

    @staticmethod
    def _plan_line_index(lines, needle):
        return next(
            (i for i, line in enumerate(lines) if needle in line), -1
        )

    def _assert_central_merge_plan(self, db, expr="interp(c1)"):
        # Multi-vgroup no-partition/BY-tags target shape is
        #   Interp <- Merge <- Exchange <- MergeScan
        # i.e. Interp runs on the COORDINATOR, ABOVE the Exchange. The BY-tbname
        # pushdown shape instead has Interp on the vnode, BELOW the Exchange.
        # Discriminators: (a) Exchange present (multi-vnode), (b) Interp appears
        # ABOVE (before, nearer root) the Exchange in the explain output.
        tdSql.query(f"explain {self._interp_sql(db, 'linear', None, expr)}")
        lines = [str(r[0]) for r in tdSql.queryResult]
        plan = "\n".join(lines)
        assert "Exchange" in plan, f"expected Exchange in multi-vgroup plan:\n{plan}"
        interp_idx = self._plan_line_index(lines, "Interp")
        exch_idx = self._plan_line_index(lines, "Exchange")
        assert interp_idx != -1 and exch_idx != -1 and interp_idx < exch_idx, (
            "expected coordinator Interp ABOVE Exchange "
            f"(central Merge shape), got:\n{plan}"
        )
        # TODO: once the central-Merge operator's explain token is confirmed,
        # assert it precisely (it is NOT 'Table Merge Scan').
        tdLog.info(f"plan shape OK: coordinator Interp above Exchange ({expr})")

    def _assert_central_partition_plan(
        self, db, expr="interp(c1)", partition=None
    ):
        partition = partition or self.COLUMN_PARTITION
        sql = self._interp_sql(
            db, "linear", partition, expr
        )
        tdSql.query(f"explain {sql}")
        lines = [str(row[0]) for row in tdSql.queryResult]
        plan = "\n".join(lines)
        interp_idx = self._plan_line_index(lines, "Interp")
        partition_idx = self._plan_line_index(lines, "Partition")
        merge_idx = self._plan_line_index(lines, "-> Merge ")
        exchange_idx = self._plan_line_index(lines, "Exchange")
        indices = (interp_idx, partition_idx, merge_idx, exchange_idx)
        assert all(index >= 0 for index in indices) and (
            interp_idx < partition_idx < merge_idx < exchange_idx
        ), (
            "expected Interp <- Partition <- Merge <- Exchange, got:\n"
            f"{plan}"
        )
        assert "Table Merge Scan" in plan, (
            f"expected remote Table Merge Scan, got:\n{plan}"
        )
        tdLog.info(
            "plan shape OK: coordinator Interp above Partition and Exchange "
            f"({expr})"
        )

    def _assert_single_vgroup_plan(
        self, db, expr="interp(c1)", partition=None
    ):
        # Single-vgroup shape: Interp above TableMergeScan, and NO Exchange.
        sql = self._interp_sql(db, "linear", partition, expr)
        tdSql.query(f"explain {sql}")
        plan = "\n".join(str(r[0]) for r in tdSql.queryResult)
        assert "Exchange" not in plan, (
            f"single-vgroup plan should have no Exchange:\n{plan}"
        )
        assert "Interp" in plan, f"expected Interp in plan:\n{plan}"
        if partition:
            assert "Partition" in plan, (
                f"expected local Partition in plan:\n{plan}"
            )
        tdLog.info(f"plan shape OK: single-vgroup, no Exchange ({expr})")

    def test_interp_multivgroup(self):
        """Verify INTERP over single- and multi-vgroup super tables

        1. Assert the single-vgroup plan keeps Interp above a local scan
        2. Assert the multi-vgroup plan keeps Interp above the Exchange
        3. Assert literal result grids for every FILL and partition shape
        4. Repeat plan and result checks with an interp expression arg
        5. Verify remote phase barriers and observable notify propagation
        6. Verify all remote multi-group early-stop phase combinations
        7. Verify an empty Exchange source closes every phase barrier
        8. Verify PREV early-stop after an empty NEXT phase
        9. Split mixed tag/tbname and ordinary-column partition keys

        Catalog:
            - Query

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-19 OpenAI Add multi-vgroup A/B and plan-shape baseline
            - 2026-08-25 OpenAI Register in CI and cover expression args
            - 2026-08-26 OpenAI Cover the remote phased runtime protocol
            - 2026-09-01 OpenAI Cover distributed column partitions
            - 2026-09-01 OpenAI Cover mixed outer and column partitions
            - 2026-09-01 OpenAI Pin the runtime dataset to two outer groups

        """
        fills = ("prev", "next", "linear", "near", "null", "value, -1")
        self._assert_three_node_cluster()
        # Single vgroup: local merge scan path (no Exchange/central Merge).
        self._setup_db("t0_single", vgroups=1)
        self._assert_single_vgroup_plan("t0_single")
        # A pushed-down interp expression must not alter the shape.
        self._assert_single_vgroup_plan("t0_single", self.EXPR)
        self._assert_single_vgroup_plan(
            "t0_single", partition=self.MIXED_TAG_PARTITION
        )
        self._assert_single_vgroup_plan(
            "t0_single", partition=self.MIXED_TBNAME_PARTITION
        )
        for fill in fills:
            self._assert_result_grid("t0_single", fill, None)
            self._assert_result_grid("t0_single", fill, "grp")
            self._assert_result_grid(
                "t0_single", fill, None, self.EXPR
            )
            self._assert_result_grid(
                "t0_single", fill, "grp", self.EXPR
            )
            self._assert_result_grid(
                "t0_single", fill, self.MIXED_TAG_PARTITION
            )
            self._assert_result_grid(
                "t0_single", fill, self.MIXED_TBNAME_PARTITION
            )

        # Multi-vgroup: central Interp<-Merge<-Exchange<-MergeScan.
        self._setup_db("t0_multi", vgroups=RUNTIME_VGROUPS)
        self._assert_central_merge_plan("t0_multi")
        self._assert_central_merge_plan("t0_multi", self.EXPR)
        self._assert_central_partition_plan("t0_multi")
        self._assert_central_partition_plan("t0_multi", self.EXPR)
        self._assert_central_partition_plan(
            "t0_multi", partition=self.MIXED_TAG_PARTITION
        )
        self._assert_central_partition_plan(
            "t0_multi", partition=self.MIXED_TBNAME_PARTITION
        )
        for fill in fills:
            self._assert_result_grid("t0_multi", fill, None)
            self._assert_result_grid("t0_multi", fill, "grp")
            self._assert_result_grid(
                "t0_multi", fill, self.COLUMN_PARTITION
            )
            self._assert_result_grid(
                "t0_multi", fill, None, self.EXPR
            )
            self._assert_result_grid(
                "t0_multi", fill, "grp", self.EXPR
            )
            self._assert_result_grid(
                "t0_multi", fill, self.COLUMN_PARTITION, self.EXPR
            )
            self._assert_result_grid(
                "t0_multi", fill, self.MIXED_TAG_PARTITION
            )
            self._assert_result_grid(
                "t0_multi", fill, self.MIXED_TBNAME_PARTITION
            )

        self._assert_result_grid(
            "t0_multi", "linear", self.MIXED_TAG_PARTITION, self.EXPR
        )
        self._assert_result_grid(
            "t0_multi", "linear", self.MIXED_TBNAME_PARTITION,
            self.EXPR
        )

        source_count, notify_source_count = self._setup_runtime_db()
        # Its one-row PREV phase finishes before the late phase-2 notify
        # can propagate, so only phase 1 is remote-delivery evidence.
        self._assert_remote_runtime_protocol(
            self.RUNTIME_DB, source_count, notify_source_count,
            notify_phases=(1, 2), propagated_phases=(1,)
        )
        self._assert_remote_runtime_protocol(
            self.RUNTIME_DB, source_count, notify_source_count,
            partition=self.COLUMN_PARTITION, interp_mode=2,
            notify_phases=(1,), target_hops=1
        )
        self._assert_remote_runtime_protocol(
            self.RUNTIME_DB, source_count, notify_source_count,
            partition=self.MIXED_TAG_PARTITION, interp_mode=2,
            notify_phases=(1,), target_hops=1,
            outer_epoch_count=2
        )

        sparse_source_count, sparse_notify_source_count = (
            self._setup_runtime_db(empty_source=True)
        )
        self._assert_remote_runtime_protocol(
            self.RUNTIME_SPARSE_DB, sparse_source_count,
            sparse_notify_source_count, notify_phases=(1, 2),
            propagated_phases=(1,)
        )

        prev_source_count, prev_notify_source_count = (
            self._setup_runtime_db(early_stop_phase=2)
        )
        self._assert_remote_runtime_protocol(
            self.RUNTIME_PREV_DB, prev_source_count,
            prev_notify_source_count, fill="prev", notify_phases=(2,),
            output_phases=(0, 2)
        )
        self._assert_remote_runtime_protocol(
            self.RUNTIME_PREV_DB, prev_source_count,
            prev_notify_source_count, fill="prev",
            notify_phases=(2,), output_phases=(0, 2),
            partition=self.COLUMN_PARTITION, interp_mode=2,
            target_hops=1
        )

        self._make_runtime_main_complete(self.RUNTIME_PREV_DB)
        self._assert_result_grid(
            self.RUNTIME_PREV_DB, "linear", self.COLUMN_PARTITION
        )
        self._assert_remote_runtime_protocol(
            self.RUNTIME_PREV_DB, prev_source_count,
            prev_notify_source_count, notify_phases=(1, 2),
            partition=self.COLUMN_PARTITION, interp_mode=2,
            target_hops=1
        )

        tdLog.success(
            "interp multi-vgroup: result, plan, and runtime checks passed"
        )
