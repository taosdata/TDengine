from collections import Counter
from datetime import datetime, timedelta
import re
import subprocess
import time

from new_test_framework.utils import ci_scaled_retry, etool, tdLog, tdSql
from new_test_framework.utils.log_reader import LogReader
from new_test_framework.utils.srvCtl import sc


class TestInterpMainFirst:
    updatecfgDict = {
        "asynclog": 0,
        "qDebugFlag": 135,
    }
    DB = "interp_main_first"
    COLUMN_DB = "interp_column_group"
    SPILL_DB = "interp_result_spill"
    SPILL_COLUMNS = 16
    SPILL_GROUPS = 2
    SPILL_POINTS_PER_GROUP = 100001
    RESULT_STORE_LIMIT_KB = 10 * 1024
    DENSE_RESULT_ROWS = 4201
    DENSE_RANGE_END = "2020-01-01 00:01:04.200"
    DENSE_TRAILING_REFERENCE_MS = DENSE_RESULT_ROWS
    RANGE = (
        "range('2020-01-01 00:00:03', "
        "'2020-01-01 00:00:09') every(1s)"
    )
    # Arguments the physical planner extracts into InterpFuncPhysiNode
    # pExprs and the interp operator evaluates per source row.
    # abs(c1 - 6) is deliberately NOT an elementwise transform of
    # interp(c1): the source series folds at c1 = 6, so evaluating the
    # expression after interpolation would yield 0 at 00:00:06 instead
    # of 2. Expected values are keyed by fill mode over RANGE.
    EXPR_CASES = (
        (
            "interp(abs(c1 - 6))",
            {
                "prev": [6, 2, 2, 2, 2, 2, 2],
                "next": [2, 2, 2, 2, 2, 2, 6],
                "linear": [3, 2, 2, 2, 2, 2, 3],
            },
        ),
        (
            "interp(c1 + 1)",
            {
                "prev": [1, 5, 5, 5, 5, 9, 9],
                "next": [5, 5, 9, 9, 9, 9, 13],
                "linear": [4, 5, 6, 7, 8, 9, 10],
            },
        ),
        (
            "interp(cast(c1 as double) / 2)",
            {
                "prev": [0, 2, 2, 2, 2, 4, 4],
                "next": [2, 2, 4, 4, 4, 4, 6],
                "linear": [1.5, 2, 2.5, 3, 3.5, 4, 4.5],
            },
        ),
    )
    # Phase sequence and early-stop phases each fill mode must show once
    # the whole RANGE is resolved. Mirrors do_all_fill_mode_early_stop.
    EXPR_FILL_PHASES = (
        ("prev", [0, 2], [2]),
        ("next", [0, 1], [1]),
        ("linear", [0, 1, 2], [1, 2]),
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def prepare(self):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB} vgroups 1 maxrows 200")
        tdSql.execute(f"use {self.DB}")

        tdSql.execute("create table ntb (ts timestamp, c1 int)")
        tdSql.execute(
            "insert into ntb values "
            "('2020-01-01 00:00:00', 0) "
            "('2020-01-01 00:00:04', 4) "
            "('2020-01-01 00:00:08', 8) "
            "('2020-01-01 00:00:12', 12)"
        )

        tdSql.execute("create table left_missing (ts timestamp, c1 int)")
        tdSql.execute(
            "insert into left_missing values "
            "('2020-01-01 00:00:04', 4) "
            "('2020-01-01 00:00:08', 8) "
            "('2020-01-01 00:00:12', 12)"
        )

        tdSql.execute("create table right_missing (ts timestamp, c1 int)")
        tdSql.execute(
            "insert into right_missing values "
            "('2020-01-01 00:00:00', 0) "
            "('2020-01-01 00:00:04', 4) "
            "('2020-01-01 00:00:08', 8)"
        )

        tdSql.execute("create table empty_main (ts timestamp, c1 int)")
        tdSql.execute(
            "insert into empty_main values "
            "('2020-01-01 00:00:00', 0) "
            "('2020-01-01 00:00:12', 12)"
        )

        tdSql.execute("create table ignore_null (ts timestamp, c1 int)")
        tdSql.execute(
            "insert into ignore_null values "
            "('2020-01-01 00:00:00', 0) "
            "('2020-01-01 00:00:04', null) "
            "('2020-01-01 00:00:08', 8) "
            "('2020-01-01 00:00:12', 12)"
        )
        # Exceed maxrows on both sides to cross external-reader data blocks.
        null_rows = []
        for total_ms in list(range(500, 3000, 10)) + list(
            range(9010, 12000, 10)
        ):
            seconds, millis = divmod(total_ms, 1000)
            null_rows.append(
                "('2020-01-01 00:00:"
                f"{seconds:02d}.{millis:03d}', null)"
            )
        tdSql.execute(
            "insert into ignore_null values " + " ".join(null_rows)
        )

        tdSql.execute("create table no_data (ts timestamp, c1 int)")
        tdSql.execute("create table all_null (ts timestamp, c1 int)")
        tdSql.execute(
            "insert into all_null values "
            "('2020-01-01 00:00:00', null) "
            "('2020-01-01 00:00:04', null) "
            "('2020-01-01 00:00:08', null) "
            "('2020-01-01 00:00:12', null)"
        )

        tdSql.execute("create table dense_main (ts timestamp, c1 int)")
        dense_rows = []
        # The inclusive range emits 0..4200 ms. Keep the 4201 ms sample as its
        # trailing interpolation reference without adding another output row.
        for total_ms in range(0, self.DENSE_TRAILING_REFERENCE_MS + 1):
            seconds, millis = divmod(total_ms, 1000)
            dense_rows.append(
                "('2020-01-01 00:01:"
                f"{seconds:02d}.{millis:03d}', {total_ms})"
            )
        for start in range(0, len(dense_rows), 500):
            tdSql.execute(
                "insert into dense_main values "
                + " ".join(dense_rows[start : start + 500])
            )

        tdSql.execute("create table wide_hole (ts timestamp, c1 int)")
        tdSql.execute(
            "insert into wide_hole values "
            "('2030-01-01 00:00:00.000', 1)"
        )

        tdSql.execute(
            "create stable stb (ts timestamp, c1 int) tags (gid int)"
        )
        tdSql.execute("create table ct0 using stb tags (0)")
        tdSql.execute("create table ct1 using stb tags (1)")
        tdSql.execute("create table ct2 using stb tags (2)")
        tdSql.execute("create table ct3 using stb tags (3)")
        tdSql.execute(
            "insert into ct0 values "
            "('2020-01-01 00:00:00', 0) "
            "('2020-01-01 00:00:04', 4) "
            "('2020-01-01 00:00:08', 8) "
            "('2020-01-01 00:00:12', 12)"
        )
        tdSql.execute(
            "insert into ct1 values "
            "('2020-01-01 00:00:00', 100) "
            "('2020-01-01 00:00:12', 112)"
        )
        tdSql.execute(
            "insert into ct2 values "
            "('2020-01-01 00:00:12', 212)"
        )
        tdSql.execute(
            "insert into ct3 values "
            "('2020-01-01 00:00:00', 300)"
        )

        tdSql.execute(
            "create stable merge_stb "
            "(ts timestamp, c1 int) tags (gid int)"
        )
        tdSql.execute("create table merge0 using merge_stb tags (0)")
        tdSql.execute("create table merge1 using merge_stb tags (1)")
        tdSql.execute(
            "insert into merge0 values "
            "('2020-01-01 00:00:00', 0) "
            "('2020-01-01 00:00:08', 8)"
        )
        tdSql.execute(
            "insert into merge1 values "
            "('2020-01-01 00:00:04', 4) "
            "('2020-01-01 00:00:12', 12)"
        )

        tdSql.execute(
            "create stable merge_empty_stb "
            "(ts timestamp, c1 int) tags (gid int)"
        )
        tdSql.execute(
            "create table merge_empty0 using merge_empty_stb tags (0)"
        )
        tdSql.execute(
            "create table merge_empty1 using merge_empty_stb tags (1)"
        )
        tdSql.execute(
            "insert into merge_empty0 values "
            "('2020-01-01 00:00:00', 0)"
        )
        tdSql.execute(
            "insert into merge_empty1 values "
            "('2020-01-01 00:00:12', 12)"
        )

        tdSql.execute(
            "create stable merge_group_stb "
            "(ts timestamp, c1 int) tags (gid int)"
        )
        for table, gid in (
            ("merge_g0a", 0),
            ("merge_g0b", 0),
            ("merge_g1a", 1),
            ("merge_g1b", 1),
            ("merge_g2a", 2),
            ("merge_g2b", 2),
        ):
            tdSql.execute(
                f"create table {table} using merge_group_stb tags ({gid})"
            )
        tdSql.execute(
            "insert into merge_g0a values "
            "('2020-01-01 00:00:04.000', 4)"
        )
        tdSql.execute(
            "insert into merge_g0b values "
            "('2020-01-01 00:00:00.000', 0) "
            "('2020-01-01 00:00:12.000', 12)"
        )
        tdSql.execute(
            "insert into merge_g1a values "
            "('2020-01-01 00:00:12.000', 112)"
        )
        tdSql.execute(
            "insert into merge_g1b values "
            "('2020-01-01 00:00:12.001', 113)"
        )
        tdSql.execute(
            "insert into merge_g2a values "
            "('2020-01-01 00:00:00.000', 200)"
        )
        tdSql.execute(
            "insert into merge_g2b values "
            "('2020-01-01 00:00:00.001', 201)"
        )

        tdSql.execute(
            "create stable dup_stb "
            "(ts timestamp, c1 int, c2 int) tags (gid int)"
        )
        tdSql.execute("create table dup0 using dup_stb tags (0)")
        tdSql.execute("create table dup1 using dup_stb tags (1)")
        tdSql.execute(
            "insert into dup0 values "
            "('2020-01-01 00:00:00', 0, 0) "
            "('2020-01-01 00:00:04', 4, 40) "
            "('2020-01-01 00:00:08', 8, 80)"
        )
        tdSql.execute(
            "insert into dup1 values "
            "('2020-01-01 00:00:00', 100, 1000) "
            "('2020-01-01 00:00:04', 104, 1040) "
            "('2020-01-01 00:00:08', 108, 1080)"
        )

    def prepare_column_groups(self):
        tdSql.execute(f"drop database if exists {self.COLUMN_DB}")
        tdSql.execute(
            f"create database {self.COLUMN_DB} vgroups 1 "
            "maxrows 200 precision 'us'"
        )
        tdSql.execute(f"use {self.COLUMN_DB}")
        tdSql.execute(
            "create table column_groups "
            "(ts timestamp, c1 int, gid int)"
        )
        tdSql.execute(
            "insert into column_groups values "
            "('2020-01-01 00:00:00', 0, 1) "
            "('2020-01-01 00:00:04', 4, 1) "
            "('2020-01-01 00:00:08', 8, 1) "
            "('2020-01-01 00:00:12', 12, 1) "
            "('2020-01-01 00:00:01', 21, 2) "
            "('2020-01-01 00:00:11', 31, 3)"
        )
        tdSql.execute(
            "create stable column_group_stb "
            "(ts timestamp, c1 int, gid int) tags (tid int)"
        )
        tdSql.execute(
            "create table column_group_ct0 using column_group_stb tags (0)"
        )
        tdSql.execute(
            "create table column_group_ct1 using column_group_stb tags (1)"
        )
        tdSql.execute(
            "insert into column_group_ct0 values "
            "('2020-01-01 00:00:00', 0, 1) "
            "('2020-01-01 00:00:01', 21, 2) "
            "('2020-01-01 00:00:04', 4, 1)"
        )
        tdSql.execute(
            "insert into column_group_ct1 values "
            "('2020-01-01 00:00:08', 8, 1) "
            "('2020-01-01 00:00:11', 31, 3) "
            "('2020-01-01 00:00:12', 12, 1)"
        )

        null_side_rows = []
        # The outer ranges leave more than 16 physical blocks after each valid
        # reference. The inner ranges repeat an emitted group across batches.
        side_timestamps = (
            list(range(1001, 1701001, 500))
            + list(range(9001001, 10701001, 500))
            + list(range(12001001, 13702001, 100))
        )
        for total_us in side_timestamps:
            seconds, micros = divmod(total_us, 1000000)
            null_side_rows.append(
                "('2020-01-01 00:00:"
                f"{seconds:02d}.{micros:06d}', null, 1)"
            )
        for total_us in range(58299000, 60000000, 100):
            seconds, micros = divmod(total_us, 1000000)
            null_side_rows.append(
                "('2019-12-31 23:59:"
                f"{seconds:02d}.{micros:06d}', null, 1)"
            )
        for start in range(0, len(null_side_rows), 200):
            values = " ".join(null_side_rows[start : start + 200])
            tdSql.execute(
                f"insert into column_groups values {values} "
                f"column_group_ct0 values {values}"
            )
        tdSql.execute(f"flush database {self.COLUMN_DB}")

    def prepare_large_result_spill(self):
        tdSql.execute(f"drop database if exists {self.SPILL_DB}")
        tdSql.execute(
            f"create database {self.SPILL_DB} vgroups 1 "
            "maxrows 200 precision 'ms'"
        )
        tdSql.execute(f"use {self.SPILL_DB}")
        columns = ", ".join(
            f"c{index} bigint" for index in range(self.SPILL_COLUMNS)
        )
        tdSql.execute(
            f"create table spill_groups (ts timestamp, gid int, {columns})"
        )

        sample_offsets_ms = (-1000, 1000, 99000, 101000)
        range_start = datetime(2020, 1, 1)
        values = []
        for gid in range(1, self.SPILL_GROUPS + 1):
            for sample_offset_ms in sample_offsets_ms:
                offset_ms = sample_offset_ms + gid - 1
                timestamp = range_start + timedelta(milliseconds=offset_ms)
                timestamp_text = timestamp.strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-3]
                row_values = [
                    gid * 1000000 + column * 100000 + offset_ms
                    for column in range(self.SPILL_COLUMNS)
                ]
                values.append(
                    f"('{timestamp_text}', {gid}, "
                    + ", ".join(str(value) for value in row_values)
                    + ")"
                )
        tdSql.execute("insert into spill_groups values " + " ".join(values))
        tdSql.execute(f"flush database {self.SPILL_DB}")

    def query_rows(self, sql):
        tdSql.query(sql)
        return [tuple(row) for row in tdSql.queryResult]

    def query_with_result_store_spill(self, sql):
        log_pattern = sc.dnodeLogPath(1) + "/taosdlog.*"
        log_reader = LogReader([log_pattern])

        tdSql.query(sql)
        rows = [tuple(row) for row in tdSql.queryResult]
        close_re = re.compile(
            r"(?P<thread>\d+) E UTL DEBUG Paged buffer closed, "
            r"total:(?P<total>[\d.]+) Kb \((?P<pages>\d+) Pages\), "
            r"inmem size:(?P<inmem>[\d.]+) Kb "
            r"\((?P<inmem_pages>\d+) Pages\), "
            r"file size:(?P<file>[\d.]+) Kb, "
            r"page size:(?P<page>[\d.]+) Kb, interp-result-store"
        )
        stats_template = (
            r"{thread} E UTL DEBUG Get/Release pages:"
            r"(?P<get_pages>\d+)/(?P<release_pages>\d+), "
            r"flushToDisk:(?P<flush>[\d.]+) Kb "
            r"\((?P<flush_pages>\d+) Pages\), "
            r"loadFromDisk:(?P<load>[\d.]+) Kb "
            r"\((?P<load_pages>\d+) Pages\)"
        )
        evidence_parts = []
        evidence = ""
        deadline = time.monotonic() + ci_scaled_retry(10)
        while time.monotonic() < deadline:
            chunk = log_reader.read()
            if chunk:
                evidence_parts.append(chunk)

            evidence = "".join(evidence_parts)
            for close in reversed(list(close_re.finditer(evidence))):
                stats_re = re.compile(
                    stats_template.format(thread=close.group("thread"))
                )
                stats = stats_re.search(evidence, close.end())
                if stats is None:
                    continue
                return rows, {
                    "total_kb": float(close.group("total")),
                    "pages": int(close.group("pages")),
                    "inmem_kb": float(close.group("inmem")),
                    "inmem_pages": int(close.group("inmem_pages")),
                    "file_kb": float(close.group("file")),
                    "page_kb": float(close.group("page")),
                    "get_pages": int(stats.group("get_pages")),
                    "release_pages": int(stats.group("release_pages")),
                    "flush_kb": float(stats.group("flush")),
                    "flush_pages": int(stats.group("flush_pages")),
                    "load_kb": float(stats.group("load")),
                    "load_pages": int(stats.group("load_pages")),
                }
            time.sleep(0.1)

        raise AssertionError(
            "missing interp result-store spill evidence\n"
            f"log tail:\n{evidence[-4000:]}"
        )

    def query_with_runtime_evidence(
        self,
        sql,
        interp_mode,
        scan_kind,
        scan_mode,
        phases=None,
        phase_group_counts=None,
        natural_eof=False,
        early_stop_phases=None,
        notify_target=None,
        notify_hops=None,
        recurring_phase=None,
    ):
        log_pattern = sc.dnodeLogPath(1) + "/taosdlog.*"
        log_reader = LogReader([log_pattern])
        tdSql.query(sql)
        rows = [tuple(row) for row in tdSql.queryResult]

        task_pattern = r"(TID:0x[0-9a-f]+ QID:0x[0-9a-f]+)"
        runtime_re = re.compile(
            task_pattern
            + r" "
            r"TimeSlice runtime interpMode=(-?\d+) "
            r"scanKind=([a-z_]+) scanInterpMode=(-?\d+)"
        )
        phase_re = re.compile(
            task_pattern + r" TimeSlice MAIN_FIRST input phase=(\d+)"
        )
        phase_group_re = re.compile(
            task_pattern
            + r" TimeSlice MAIN_FIRST input phase=(\d+) groupId=(\d+)"
        )
        natural_eof_re = re.compile(
            task_pattern + r" TimeSlice MAIN_FIRST natural EOF"
        )
        stop_re = re.compile(
            task_pattern
            + r" TimeSlice MAIN_FIRST early-stop phase=(\d+) "
            + r"target=([a-z_]+) hops=(\d+) notifyTs=(-?\d+)"
        )
        consumed_re = re.compile(
            task_pattern
            + r" MAIN_FIRST early-stop consumed "
            + r"target=([a-z_]+) notifyTs=(-?\d+)"
        )
        deadline = time.monotonic() + ci_scaled_retry(5)
        evidence_parts = []
        evidence = ""
        while time.monotonic() < deadline:
            previous_part_count = len(evidence_parts)
            chunk = log_reader.read()
            if chunk:
                evidence_parts.append(chunk)

            if len(evidence_parts) == previous_part_count:
                time.sleep(0.1)
                continue
            evidence = "".join(evidence_parts)

            candidate_tasks = []
            seen_tasks = set()
            for match in runtime_re.finditer(evidence):
                task_id = match.group(1)
                actual = (
                    int(match.group(2)),
                    match.group(3),
                    int(match.group(4)),
                )
                if actual != (interp_mode, scan_kind, scan_mode):
                    continue
                if task_id not in seen_tasks:
                    candidate_tasks.append(task_id)
                    seen_tasks.add(task_id)

            if not candidate_tasks:
                time.sleep(0.1)
                continue

            phase_sequences = {}
            for task_id, phase in phase_re.findall(evidence):
                phase = int(phase)
                observed = phase_sequences.setdefault(task_id, [])
                if not observed or observed[-1] != phase:
                    observed.append(phase)

            phase_group_pairs_by_task = {}
            phase_groups_by_task = {}
            for task_id, phase, group_id in phase_group_re.findall(
                evidence
            ):
                phase = int(phase)
                group_id = int(group_id)
                phase_group_pairs_by_task.setdefault(task_id, []).append(
                    (phase, group_id)
                )
                phase_groups_by_task.setdefault(task_id, {}).setdefault(
                    phase, set()
                ).add(group_id)

            natural_eof_tasks = set(natural_eof_re.findall(evidence))
            stops_by_task = {}
            for task_id, phase, target, hops, notify_ts in stop_re.findall(
                evidence
            ):
                stops_by_task.setdefault(task_id, []).append(
                    (int(phase), target, int(hops), int(notify_ts))
                )

            consumed_by_task = {}
            for task_id, target, notify_ts in consumed_re.findall(evidence):
                consumed_by_task.setdefault(task_id, Counter())[
                    (target, int(notify_ts))
                ] += 1

            for task_id in candidate_tasks:
                observed = phase_sequences.get(task_id, [])
                observed_groups = phase_groups_by_task.get(task_id, {})
                phase_group_pairs = phase_group_pairs_by_task.get(
                    task_id, []
                )
                if interp_mode == 3:
                    closed_groups = set()
                    active_group = None
                    last_phase = -1
                    for phase, group in phase_group_pairs:
                        if group != active_group:
                            assert group not in closed_groups, (
                                "group-first scan revisited a closed group",
                                phase_group_pairs,
                            )
                            if active_group is not None:
                                closed_groups.add(active_group)
                            active_group = group
                            last_phase = -1
                        assert phase >= last_phase, phase_group_pairs
                        last_phase = phase
                    observed = sorted(set(observed))
                if phases is not None and observed != phases:
                    continue
                actual_group_counts = {
                    phase: len(groups)
                    for phase, groups in observed_groups.items()
                }
                if (
                    phase_group_counts is not None
                    and actual_group_counts != phase_group_counts
                ):
                    continue
                if recurring_phase is not None:
                    main_groups = observed_groups.get(0, set())
                    occurrences = Counter(phase_group_pairs)
                    if not any(
                        occurrences[(recurring_phase, group_id)] > 1
                        for group_id in main_groups
                    ):
                        continue
                if natural_eof and task_id not in natural_eof_tasks:
                    continue
                stops = stops_by_task.get(task_id, [])
                if early_stop_phases is not None and [
                    stop[0] for stop in stops
                ] != early_stop_phases:
                    continue
                if notify_target is not None and any(
                    stop[1] != notify_target for stop in stops
                ):
                    continue
                if notify_hops is not None and any(
                    stop[2] != notify_hops for stop in stops
                ):
                    continue
                if early_stop_phases is not None:
                    consumed = consumed_by_task.get(
                        task_id, Counter()
                    )
                    required = Counter(
                        (target, notify_ts)
                        for _, target, _, notify_ts in stops
                    )
                    if any(
                        consumed[key] < count
                        for key, count in required.items()
                    ):
                        continue
                return rows
            time.sleep(0.1)

        raise AssertionError(
            "missing task-correlated MAIN_FIRST runtime evidence: "
            f"mode={interp_mode}, scan={scan_kind}/{scan_mode}, "
            f"phases={phases}, groups={phase_group_counts}, "
            f"natural_eof={natural_eof}, "
            f"recurring_phase={recurring_phase}, "
            f"early_stops={early_stop_phases}, "
            f"target={notify_target}, hops={notify_hops}\n"
            f"log tail:\n{evidence[-4000:]}"
        )

    def check_values(self, table, fill, expected, expr="interp(c1)",
                     suffix=""):
        sql = (
            f"select _irowts, {expr} from {table} "
            f"{self.RANGE} fill({fill}){suffix}"
        )
        rows = self.query_rows(sql)
        actual = [row[1] for row in rows]
        assert actual == expected, (
            f"{table} fill({fill}) expected {expected}, got {actual}"
        )

    # --- impl ---

    def do_normal_table_fill_matrix(self):
        expected = {
            "prev": [0, 4, 4, 4, 4, 8, 8],
            "next": [4, 4, 8, 8, 8, 8, 12],
            "linear": [3, 4, 5, 6, 7, 8, 9],
            "near": [4, 4, 4, 4, 8, 8, 8],
            "null": [None, 4, None, None, None, 8, None],
            "value, -1": [-1, 4, -1, -1, -1, 8, -1],
        }
        for fill, values in expected.items():
            self.check_values("ntb", fill, values)
        print("normal table fill matrix ............... [passed]")

    def do_child_table_fill_matrix(self):
        expected = {
            ("ct0", "prev"): [0, 4, 4, 4, 4, 8, 8],
            ("ct0", "next"): [4, 4, 8, 8, 8, 8, 12],
            ("ct0", "linear"): [3, 4, 5, 6, 7, 8, 9],
            ("ct0", "near"): [4, 4, 4, 4, 8, 8, 8],
            ("ct1", "linear"): [103, 104, 105, 106, 107, 108, 109],
        }
        for (table, fill), values in expected.items():
            self.check_values(table, fill, values)
        print("child table fill matrix ................ [passed]")

    def do_merge_scan_fill_matrix(self):
        expected = {
            "prev": [0, 4, 4, 4, 4, 8, 8],
            "next": [4, 4, 8, 8, 8, 8, 12],
            "linear": [3, 4, 5, 6, 7, 8, 9],
            "near": [4, 4, 4, 4, 8, 8, 8],
            "null": [None, 4, None, None, None, 8, None],
            "value, -1": [-1, 4, -1, -1, -1, 8, -1],
        }
        for fill, values in expected.items():
            self.check_values("merge_stb", fill, values)
        self.check_values(
            "merge_empty_stb", "linear", [3, 4, 5, 6, 7, 8, 9]
        )
        print("merge scan fill matrix ................. [passed]")

    def do_merge_scan_partition_groups(self):
        def check(fill, expected_rows, phases, phase_group_counts):
            sql = (
                "select gid, _irowts, interp(c1) from merge_group_stb "
                f"partition by gid {self.RANGE} fill({fill})"
            )
            tdSql.query(f"explain {sql}")
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            assert "Table Merge Scan" in plan, plan
            assert "Exchange" not in plan, plan

            rows = self.query_with_runtime_evidence(
                sql, 1, "table_merge", 1,
                phases=phases,
                phase_group_counts=phase_group_counts,
            )
            assert Counter(rows) == Counter(expected_rows), (
                f"fill({fill}) expected {expected_rows}, got {rows}"
            )

        next_rows = [
            (0, datetime(2020, 1, 1, 0, 0, 3), 4),
            (0, datetime(2020, 1, 1, 0, 0, 4), 4),
            (0, datetime(2020, 1, 1, 0, 0, 5), 12),
            (0, datetime(2020, 1, 1, 0, 0, 6), 12),
            (0, datetime(2020, 1, 1, 0, 0, 7), 12),
            (0, datetime(2020, 1, 1, 0, 0, 8), 12),
            (0, datetime(2020, 1, 1, 0, 0, 9), 12),
            (1, datetime(2020, 1, 1, 0, 0, 3), 112),
            (1, datetime(2020, 1, 1, 0, 0, 4), 112),
            (1, datetime(2020, 1, 1, 0, 0, 5), 112),
            (1, datetime(2020, 1, 1, 0, 0, 6), 112),
            (1, datetime(2020, 1, 1, 0, 0, 7), 112),
            (1, datetime(2020, 1, 1, 0, 0, 8), 112),
            (1, datetime(2020, 1, 1, 0, 0, 9), 112),
        ]
        prev_rows = [
            (0, datetime(2020, 1, 1, 0, 0, 3), 0),
            (0, datetime(2020, 1, 1, 0, 0, 4), 4),
            (0, datetime(2020, 1, 1, 0, 0, 5), 4),
            (0, datetime(2020, 1, 1, 0, 0, 6), 4),
            (0, datetime(2020, 1, 1, 0, 0, 7), 4),
            (0, datetime(2020, 1, 1, 0, 0, 8), 4),
            (0, datetime(2020, 1, 1, 0, 0, 9), 4),
            (2, datetime(2020, 1, 1, 0, 0, 3), 201),
            (2, datetime(2020, 1, 1, 0, 0, 4), 201),
            (2, datetime(2020, 1, 1, 0, 0, 5), 201),
            (2, datetime(2020, 1, 1, 0, 0, 6), 201),
            (2, datetime(2020, 1, 1, 0, 0, 7), 201),
            (2, datetime(2020, 1, 1, 0, 0, 8), 201),
            (2, datetime(2020, 1, 1, 0, 0, 9), 201),
        ]
        check("next", next_rows, [0, 1], {0: 1, 1: 2})
        check("prev", prev_rows, [0, 2], {0: 1, 2: 2})
        print("merge scan partition groups ............ [passed]")

    def do_empty_main_fill_matrix(self):
        expected = {
            "prev": [0, 0, 0, 0, 0, 0, 0],
            "next": [12, 12, 12, 12, 12, 12, 12],
            "linear": [3, 4, 5, 6, 7, 8, 9],
            "near": [0, 0, 0, 0, 12, 12, 12],
            "null": [None, None, None, None, None, None, None],
            "value, -1": [-1, -1, -1, -1, -1, -1, -1],
        }
        for fill, values in expected.items():
            self.check_values("empty_main", fill, values)
        print("empty MAIN fill matrix ................. [passed]")

    def do_empty_side_phase_cases(self):
        self.check_values(
            "left_missing", "prev", [4, 4, 4, 4, 8, 8]
        )
        self.check_values(
            "right_missing", "next", [4, 4, 8, 8, 8, 8]
        )
        self.check_values(
            "left_missing", "linear", [4, 5, 6, 7, 8, 9]
        )
        self.check_values(
            "right_missing", "linear", [3, 4, 5, 6, 7, 8]
        )
        print("empty side phase cases ................. [passed]")

    def do_ignore_null_across_blocks(self):
        self.check_values(
            "ignore_null", "linear", [3, 4, 5, 6, 7, 8, 9],
            "interp(c1, 1)",
        )
        print("ignore null reference search ........... [passed]")

    def do_empty_and_all_null_inputs(self):
        self.check_values("no_data", "linear", [])
        self.check_values(
            "all_null", "value, -7", [-7] * 7,
            "interp(c1, 1)",
        )
        self.check_values(
            "all_null", "linear", [], "interp(c1, 1)"
        )
        print("empty and all-null inputs .............. [passed]")

    def do_multi_block_result_store(self):
        dense_range = (
            "range('2020-01-01 00:01:00.000', "
            f"'{self.DENSE_RANGE_END}') every(1a)"
        )
        sql = (
            "select _irowts, interp(c1) from dense_main "
            f"{dense_range} fill(linear)"
        )
        rows = self.query_rows(sql)
        assert len(rows) == self.DENSE_RESULT_ROWS, len(rows)
        assert rows[0][1] == 0, rows[0]
        assert rows[-1][1] == 4200, rows[-1]
        print("multi-block result store ............... [passed]")

    def do_surrounding_boundary(self):
        self.check_values(
            "ntb", "prev", [-99, 4, 4, 4, -99, 8, 8],
            suffix=" surround(2s, -99)",
        )
        print("surrounding boundary ................... [passed]")

    def do_empty_main_by_tbname(self):
        def query_values(fill):
            sql = (
                "select tbname, _irowts, interp(c1) from stb "
                f"partition by tbname {self.RANGE} fill({fill})"
            )
            actual = {}
            for table, _, value in self.query_rows(sql):
                actual.setdefault(table, []).append(value)
            return actual

        expected = {
            "ct0": [3, 4, 5, 6, 7, 8, 9],
            "ct1": [103, 104, 105, 106, 107, 108, 109],
        }
        actual = query_values("linear")
        assert actual == expected, f"expected {expected}, got {actual}"

        expected = {
            "ct0": [4, 4, 8, 8, 8, 8, 12],
            "ct1": [112, 112, 112, 112, 112, 112, 112],
            "ct2": [212, 212, 212, 212, 212, 212, 212],
        }
        actual = query_values("next")
        assert actual == expected, f"expected {expected}, got {actual}"

        expected = {
            "ct0": [0, 4, 4, 4, 4, 8, 8],
            "ct1": [100, 100, 100, 100, 100, 100, 100],
            "ct3": [300, 300, 300, 300, 300, 300, 300],
        }
        actual = query_values("prev")
        assert actual == expected, f"expected {expected}, got {actual}"
        print("empty MAIN and side-only groups ......... [passed]")

    def do_column_group_main_membership(self):
        cases = {
            "prev": ([0, 4, 4, 4, 4, 8, 8], [0, 2], {0: 1, 2: 2}),
            "next": ([4, 4, 8, 8, 8, 8, 12], [0, 1], {0: 1, 1: 2}),
        }
        expected_times = [
            datetime(2020, 1, 1, 0, 0, second)
            for second in range(3, 10)
        ]
        sources = (
            ("column_groups", "Table Scan", True, "table"),
            (
                "column_group_stb",
                "Table Merge Scan",
                False,
                "table_merge",
            ),
        )
        for source, scan_name, expect_recurrence, notify_target in sources:
            for fill, (expected_values, phases, group_counts) in cases.items():
                sql = (
                    f"select gid, _irowts, interp(c1, 1) from {source} "
                    f"partition by gid {self.RANGE} fill({fill})"
                )
                tdSql.query(f"explain {sql}")
                plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
                assert "Partition" in plan, plan
                assert scan_name in plan, plan
                assert "Exchange" not in plan, plan

                rows = self.query_with_runtime_evidence(
                    sql, 2, "other", -1,
                    phases=phases,
                    phase_group_counts=group_counts,
                    natural_eof=True,
                    early_stop_phases=[phases[-1]],
                    notify_target=notify_target,
                    notify_hops=1,
                    recurring_phase=(
                        phases[-1] if expect_recurrence else None
                    ),
                )
                assert {row[0] for row in rows} == {1}, rows
                rows.sort(key=lambda row: row[1])
                assert [row[1] for row in rows] == expected_times, rows
                assert [row[2] for row in rows] == expected_values, rows
        print("column-group MAIN membership ........... [passed]")

    def do_large_column_group_result_spill(self):
        expressions = ", ".join(
            f"interp(c{index})" for index in range(self.SPILL_COLUMNS)
        )
        sql = (
            f"select gid, _irowts, {expressions} from spill_groups "
            "partition by gid "
            "range('2020-01-01 00:00:00.000', "
            "'2020-01-01 00:01:40.000') every(1a) fill(linear)"
        )
        tdSql.query(f"explain {sql}")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert "Partition" in plan, plan
        assert "Table Scan" in plan, plan
        assert "Exchange" not in plan, plan

        rows, spill = self.query_with_result_store_spill(sql)
        expected_rows = self.SPILL_GROUPS * self.SPILL_POINTS_PER_GROUP
        assert len(rows) == expected_rows, (len(rows), expected_rows)
        rows.sort(key=lambda row: (row[0], row[1]))
        start = datetime(2020, 1, 1)
        for row_index, row in enumerate(rows):
            group_index, offset_ms = divmod(
                row_index, self.SPILL_POINTS_PER_GROUP
            )
            gid = group_index + 1
            assert row[0] == gid, (row_index, row)
            assert row[1] == start + timedelta(milliseconds=offset_ms), (
                row_index,
                row,
            )
            for column in range(self.SPILL_COLUMNS):
                expected = gid * 1000000 + column * 100000 + offset_ms
                assert row[column + 2] == expected, (
                    row_index,
                    column,
                    row[column + 2],
                    expected,
                )

        assert spill["total_kb"] > self.RESULT_STORE_LIMIT_KB, spill
        assert spill["inmem_kb"] <= self.RESULT_STORE_LIMIT_KB, spill
        assert spill["file_kb"] > 0, spill
        assert spill["flush_kb"] > 0, spill
        assert spill["flush_pages"] > 0, spill
        assert spill["load_kb"] > 0, spill
        assert spill["load_pages"] > 0, spill
        print("large column-group result spill ........ [passed]")

    def do_duplicate_timestamp_candidates(self):
        sql = (
            "select _irowts, interp(c1), interp(c2) from dup_stb "
            f"{self.RANGE} fill(prev)"
        )
        expected_times = [
            datetime(2020, 1, 1, 0, 0, second)
            for second in range(3, 10)
        ]
        legal_pairs = [
            {(0, 0), (100, 1000)},
            {(4, 40), (104, 1040)},
            {(4, 40), (104, 1040)},
            {(4, 40), (104, 1040)},
            {(4, 40), (104, 1040)},
            {(8, 80), (108, 1080)},
            {(8, 80), (108, 1080)},
        ]
        rows = self.query_rows(sql)
        assert [row[0] for row in rows] == expected_times, rows
        for row, candidates in zip(rows, legal_pairs):
            assert row[1:] in candidates, (
                f"row={row} candidates={candidates}"
            )
        print("duplicate timestamp candidates .......... [passed]")

    def do_emitted_group_dedup(self):
        sql = (
            "select tbname, _irowts, interp(c1) from stb "
            f"partition by tbname {self.RANGE} fill(null)"
        )
        rows = self.query_rows(sql)
        ct1_rows = [row for row in rows if row[0] == "ct1"]
        assert len(ct1_rows) == 7, ct1_rows
        assert len({row[1] for row in ct1_rows}) == 7, ct1_rows
        print("emitted group dedup ..................... [passed]")

    def do_runtime_mode_and_phase_protocol(self):
        sql = (
            "select _irowts, interp(c1) from ntb "
            f"{self.RANGE} fill(linear)"
        )
        rows = self.query_with_runtime_evidence(
            sql, 3, "table", 3, phases=[0, 1, 2]
        )
        assert [row[1] for row in rows] == list(range(3, 10))

        child_sql = (
            "select _irowts, interp(c1) from ct0 "
            f"{self.RANGE} fill(linear)"
        )
        self.query_with_runtime_evidence(
            child_sql, 3, "table", 3, phases=[0, 1, 2]
        )

        group_sql = (
            "select tbname, _irowts, interp(c1) from stb "
            f"partition by tbname {self.RANGE} fill(linear)"
        )
        self.query_with_runtime_evidence(
            group_sql, 3, "table", 3, phases=[0, 1, 2]
        )

        jump_sql = (
            "select _irowts, interp(c1) from right_missing "
            f"{self.RANGE} fill(prev)"
        )
        self.query_with_runtime_evidence(
            jump_sql, 3, "table", 3, phases=[0, 2]
        )

        merge_sql = (
            "select _irowts, interp(c1) from merge_stb "
            f"{self.RANGE} fill(linear)"
        )
        tdSql.query(f"explain {merge_sql}")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert "Table Merge Scan" in plan, plan
        assert "Exchange" not in plan, plan

        merge_rows = self.query_with_runtime_evidence(
            merge_sql, 1, "table_merge", 1,
            phases=[0, 1, 2],
        )
        assert [row[1] for row in merge_rows] == list(range(3, 10))

        empty_merge_sql = (
            "select _irowts, interp(c1) from merge_empty_stb "
            f"{self.RANGE} fill(linear)"
        )
        empty_merge_rows = self.query_with_runtime_evidence(
            empty_merge_sql, 1, "table_merge", 1,
            phases=[1, 2], natural_eof=True,
        )
        assert [row[1] for row in empty_merge_rows] == list(range(3, 10))

        eof_sql = (
            "select _irowts, interp(c1) from no_data "
            f"{self.RANGE} fill(linear)"
        )
        self.query_with_runtime_evidence(
            eof_sql, 3, "table", 3, natural_eof=True
        )
        print("runtime mode and phase protocol ......... [passed]")

    def do_all_fill_mode_early_stop(self):
        fill_cases = (
            ("prev", [0, 2], [2]),
            ("next", [0, 1], [1]),
            ("linear", [0, 1, 2], [1, 2]),
            ("near", [0, 1, 2], [1, 2]),
            ("null", [0, 1, 2], [1, 2]),
            ("null_f", [0, 1, 2], [1, 2]),
            ("value, -1", [0, 1, 2], [1, 2]),
            ("value_f, -1", [0, 1, 2], [1, 2]),
        )
        sources = (
            ("ntb", "table"),
            ("merge_stb", "table_merge"),
        )
        main_range = (
            "range('2020-01-01 00:00:00', "
            "'2020-01-01 00:00:09') every(1d)"
        )
        main_where = (
            "where ts >= '2020-01-01 00:00:00' "
            "and ts <= '2020-01-01 00:00:09'"
        )

        for source, target in sources:
            mode = 3 if target == "table" else 1
            for fill, side_phases, stop_phases in fill_cases:
                main_sql = (
                    f"select _irowts, interp(c1) from {source} "
                    f"{main_where} {main_range} fill({fill})"
                )
                rows = self.query_with_runtime_evidence(
                    main_sql, mode, target, mode,
                    phases=[0], early_stop_phases=[0],
                    notify_target=target, notify_hops=0,
                )
                assert len(rows) == 1, rows
                assert rows[0][1] == 0, rows

                side_sql = (
                    f"select _irowts, interp(c1) from {source} "
                    f"{self.RANGE} fill({fill})"
                )
                self.query_with_runtime_evidence(
                    side_sql, mode, target, mode,
                    phases=side_phases,
                    early_stop_phases=stop_phases,
                    notify_target=target, notify_hops=0,
                )

        none_sql = (
            "select _irowts, interp(c1) from empty_main "
            f"{self.RANGE} fill(none)"
        )
        tdSql.error(none_sql)
        print("all fill-mode early-stop ............... [passed]")

    def do_intermediate_operator_early_stop(self):
        sources = (
            ("ntb", "table"),
            ("merge_stb", "table_merge"),
        )
        for source, target in sources:
            sql = (
                "select _irowts, interp(v) from "
                f"(select ts, c1 + 0 v from {source}) "
                f"{self.RANGE} fill(linear)"
            )
            tdSql.query(f"explain {sql}")
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            assert "Projection" in plan, plan
            scan_name = (
                "Table Scan" if target == "table" else "Table Merge Scan"
            )
            assert scan_name in plan, plan
            assert "Exchange" not in plan, plan

            rows = self.query_with_runtime_evidence(
                sql, 3 if target == "table" else 1, "other", -1,
                phases=[0, 1, 2], early_stop_phases=[1, 2],
                notify_target=target, notify_hops=1,
            )
            assert [row[1] for row in rows] == list(range(3, 10))
        print("intermediate early-stop forwarding ..... [passed]")

    def do_expression_arg_matrix(self):
        # ntb, ct0 and merge_stb all expose the same 0/4/8/12 source
        # series, so one expected table covers the three scan shapes.
        for table in ("ntb", "ct0", "merge_stb"):
            for expr, expected in self.EXPR_CASES:
                for fill, values in expected.items():
                    self.check_values(table, fill, values, expr=expr)

        # Plan shape and early-stop per scan kind. notify_hops=0 proves
        # the pushed-down expression slot did not insert an operator
        # between TimeSlice and the scan.
        routing = (
            ("ntb", "table", "Table Scan"),
            ("ct0", "table", "Table Scan"),
            ("merge_stb", "table_merge", "Table Merge Scan"),
        )
        for table, scan_kind, scan_name in routing:
            mode = 3 if scan_kind == "table" else 1
            for expr, _ in self.EXPR_CASES:
                for fill, phases, stop_phases in self.EXPR_FILL_PHASES:
                    sql = (
                        f"select _irowts, {expr} from {table} "
                        f"{self.RANGE} fill({fill})"
                    )
                    tdSql.query(f"explain {sql}")
                    plan = "\n".join(
                        str(row[0]) for row in tdSql.queryResult
                    )
                    assert scan_name in plan, plan
                    assert "Exchange" not in plan, plan

                    rows = self.query_with_runtime_evidence(
                        sql, mode, scan_kind, mode,
                        phases=phases,
                        early_stop_phases=stop_phases,
                        notify_target=scan_kind, notify_hops=0,
                    )
                    assert len(rows) == 7, rows

        # Partitioned super table: group-sort TableScan over many groups.
        # ct2 and ct3 hold a single row each, so LINEAR emits no rows for
        # them and only the two-sided groups survive.
        group_sql = (
            "select tbname, _irowts, interp(abs(c1 - 6)) from stb "
            f"partition by tbname {self.RANGE} fill(linear)"
        )
        tdSql.query(f"explain {group_sql}")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        assert "Table Scan" in plan, plan
        assert "Exchange" not in plan, plan

        rows = self.query_with_runtime_evidence(
            group_sql, 3, "table", 3, phases=[0, 1, 2]
        )
        grouped = {}
        for table, _, value in rows:
            grouped.setdefault(table, []).append(value)
        expected_groups = {
            "ct0": [3, 2, 2, 2, 2, 2, 3],
            "ct1": [97, 98, 99, 100, 101, 102, 103],
        }
        assert grouped == expected_groups, (
            f"expected {expected_groups}, got {grouped}"
        )
        print("expression argument matrix ............. [passed]")

    def do_wide_hole_watchdog(self):
        sql = (
            f"use {self.DB}; "
            "select _irowts, interp(c1) from wide_hole "
            "range('2020-01-01 00:00:00.000', "
            "'2030-01-01 00:00:00.000') every(1a) fill(linear);"
        )
        try:
            completed = subprocess.run(
                [etool.taosFile(), "-s", sql],
                capture_output=True,
                text=True,
                timeout=ci_scaled_retry(10),
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise AssertionError(
                "wide MAIN_FIRST lead hole exceeded the hard timeout"
            ) from error
        assert completed.returncode == 0, (
            f"taos failed: stdout={completed.stdout}\n"
            f"stderr={completed.stderr}"
        )
        print("wide lead-hole watchdog ................. [passed]")

    # --- main ---

    def test_interp_main_first(self):
        """Verify MAIN_FIRST interp over TableScan and TableMergeScan

        1. Cover all FILL modes on ordinary and child tables
        2. Cover all FILL modes and empty MAIN on a single-vgroup stable
        3. Cover SURROUNDING, empty MAIN, and side-only BY tbname groups
        4. Cover BY tags multi-groups and later side-only groups
        5. Cover duplicate timestamp rows with legal-candidate assertions
        6. Cover empty/all-null input and a multi-block result store
        7. Assert literal MAIN_FIRST results and runtime routes
        8. Prove all supported FILL modes send and consume early-stop
        9. Prove Project preserves phases and forwards early-stop to scans
        10. Cover scalar-expression interp arguments on every scan shape
        11. Bound a wide LINEAR hole with a process-level hard timeout

        Catalog:
            - Query

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-14 OpenAI Add the cache-gated interp baseline
            - 2026-08-17 OpenAI Enable MAIN_FIRST vs LEGACY A/B coverage
            - 2026-08-24 OpenAI Cover merge-scan phases and early-stop
            - 2026-08-25 OpenAI Cover interp expression arguments

        """
        self.prepare()
        self.do_normal_table_fill_matrix()
        self.do_child_table_fill_matrix()
        self.do_merge_scan_fill_matrix()
        self.do_merge_scan_partition_groups()
        self.do_empty_main_fill_matrix()
        self.do_empty_side_phase_cases()
        self.do_ignore_null_across_blocks()
        self.do_empty_and_all_null_inputs()
        self.do_multi_block_result_store()
        self.do_surrounding_boundary()
        self.do_empty_main_by_tbname()
        self.do_duplicate_timestamp_candidates()
        self.do_emitted_group_dedup()
        self.do_runtime_mode_and_phase_protocol()
        self.do_all_fill_mode_early_stop()
        self.do_intermediate_operator_early_stop()
        self.do_expression_arg_matrix()
        self.do_wide_hole_watchdog()

    def test_interp_column_group_main_membership(self):
        """Verify interp column groups require MAIN membership

        1. Preserve PREV and NEXT rows as references for MAIN groups
        2. Drop PREV-only and NEXT-only column groups
        3. Exercise TableScan and TableMergeScan phased producers
        4. Stop each required side phase once all MAIN groups are resolved
        5. Keep group state when side data recurs across partition batches

        Catalog:
            - Query

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-28 OpenAI Add column-group MAIN membership
            - 2026-08-31 OpenAI Enable single-vgroup MULTI_GROUP runtime

        """
        self.prepare_column_groups()
        self.do_column_group_main_membership()

    def test_interp_large_column_group_result_spill(self):
        """Verify large column-group interpolation spills and reloads

        1. Block multiple column groups across MAIN, PREV, and NEXT phases
        2. Exceed the default 10 MiB interpolation result-store budget
        3. Verify disk flush and reload through paged-buffer statistics
        4. Validate every timestamp and interpolated value after reload

        Catalog:
            - Query

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 OpenAI Add default-budget spill coverage

        """
        self.prepare_large_result_spill()
        self.do_large_column_group_result_spill()
