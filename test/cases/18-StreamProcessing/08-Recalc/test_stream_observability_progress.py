import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamObservabilityProgress:
    STREAM_SQL = (
        "select history_progress_pct from information_schema.ins_streams "
        "where db_name='obs_progress' and stream_name='s_progress'"
    )
    RECALC_SQL = (
        "select recalc_id, progress, status "
        "from information_schema.ins_stream_recalculates "
        "where stream_name='s_progress'"
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _query_rows(sql):
        tdSql.query(sql, queryTimes=1)
        return [
            tuple(tdSql.getData(row, col) for col in range(tdSql.queryCols))
            for row in range(tdSql.getRows())
        ]

    @staticmethod
    def _assert_schema():
        tdSql.query("desc information_schema.ins_streams")
        stream_schema = {
            tdSql.getData(row, 0): str(tdSql.getData(row, 1)).upper()
            for row in range(tdSql.getRows())
        }
        if stream_schema.get("history_progress_pct") != "INT":
            raise AssertionError(
                "ins_streams.history_progress_pct has unexpected type: "
                f"{stream_schema.get('history_progress_pct')!r}"
            )

        tdSql.query("desc information_schema.ins_stream_recalculates")
        recalc_schema = {
            tdSql.getData(row, 0): str(tdSql.getData(row, 1)).upper()
            for row in range(tdSql.getRows())
        }
        if recalc_schema.get("status") != "VARCHAR":
            raise AssertionError(
                "ins_stream_recalculates.status has unexpected type: "
                f"{recalc_schema.get('status')!r}"
            )
        if recalc_schema.get("request_time") != "TIMESTAMP":
            raise AssertionError(
                "ins_stream_recalculates.request_time has unexpected type: "
                f"{recalc_schema.get('request_time')!r}"
            )
        if recalc_schema.get("message") != "VARCHAR":
            raise AssertionError(
                "ins_stream_recalculates.message has unexpected type: "
                f"{recalc_schema.get('message')!r}"
            )

    @staticmethod
    def _insert_history(table, tag, rows=60000, start_row=0, batch_size=5000):
        start_ms = 1735689600000
        for batch_start in range(start_row, start_row + rows, batch_size):
            values = "".join(
                f"({start_ms + row * 1000},{row})"
                for row in range(
                    batch_start, min(batch_start + batch_size, start_row + rows)
                )
            )
            tdSql.execute(
                f"insert into obs_progress.{table} using obs_progress.events "
                f"tags({tag}) values{values}"
            )

    @classmethod
    def _wait_for_history(cls, timeout=90):
        deadline = time.monotonic() + timeout
        observed = []
        while time.monotonic() < deadline:
            rows = cls._query_rows(cls.STREAM_SQL)
            if len(rows) == 1 and rows[0][0] is not None:
                progress = rows[0][0]
                if not 0 <= progress <= 100:
                    raise AssertionError(f"invalid history progress: {progress!r}")
                if not observed or observed[-1] != progress:
                    observed.append(progress)
                if progress == 100:
                    if not any(value < 100 for value in observed):
                        raise AssertionError(
                            f"history completed without an observable active value: {observed!r}"
                        )
                    return observed
            time.sleep(0.5)
        raise AssertionError(f"history progress did not finish: {observed!r}")

    @classmethod
    def _wait_for_recalc_terminal(cls, recalc_id, observed, timeout=30):
        deadline = time.monotonic() + timeout
        last_progress = observed[-1][0]
        last_status = observed[-1][1]
        while time.monotonic() < deadline:
            rows = cls._query_rows(cls.RECALC_SQL)
            matching = [row for row in rows if row[0] == recalc_id]
            if len(matching) == 1:
                _, progress_text, status = matching[0]
                if not progress_text.endswith("%"):
                    raise AssertionError(f"progress lost percent format: {matching[0]!r}")
                progress = int(progress_text[:-1])
                if progress < last_progress:
                    raise AssertionError(f"recalc progress regressed: {observed!r}")
                allowed = {
                    "Pending": {"Pending", "Running", "Finished"},
                    "Running": {"Running", "Finished"},
                }
                if status not in allowed.get(last_status, set()):
                    raise AssertionError(
                        f"illegal recalc transition {last_status!r} -> {status!r}"
                    )
                if status == "Pending" and progress != 0:
                    raise AssertionError(f"Pending is not 0%: {matching[0]!r}")
                if status == "Running" and progress >= 100:
                    raise AssertionError(f"Running reached 100%: {matching[0]!r}")
                last_progress = progress
                last_status = status
                if not observed or observed[-1] != (progress, status):
                    observed.append((progress, status))
                if status == "Finished":
                    if progress != 100:
                        raise AssertionError(f"Finished is not 100%: {matching[0]!r}")
                    return observed
            time.sleep(0.5)
        raise AssertionError(f"recalc did not finish: {observed!r}")

    def test_stream_observability_progress(self):
        """History and manual recalculation progress cross the system views.

        1. Observe fill-history progress through completion for two fixed groups.
        2. Observe Pending and eventual Finished/100 for a manual recalculation.
        3. Verify a later group does not change the captured progress denominator.
        4. Verify an empty-range recalculation finishes at 100 percent.

        Catalog: Streams:Observability

        Since: v3.4.0.0

        Labels: stream,recalc,observability,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7045278024

        History:
            - 2026-08-13 OpenAI GPT-5 Created
        """
        tdStream.createSnode(1)
        tdSql.executes(
            [
                "drop database if exists obs_progress",
                "create database obs_progress vgroups 1 buffer 8",
                "create stable obs_progress.events (ts timestamp, value int) tags(site int)",
            ]
        )
        self._insert_history("site_1", 1)
        self._insert_history("site_2", 2)
        self._assert_schema()

        tdSql.execute(
            "create stream obs_progress.s_progress interval(1m) sliding(1m) "
            "from obs_progress.events partition by tbname "
            "stream_options(fill_history('2025-01-01 00:00:00')) "
            "into obs_progress.result "
            "OUTPUT_SUBTABLE(CONCAT('result_', tbname)) "
            "(wstart, total) tags(source varchar(128) as tbname) "
            "as select _twstart, count(*) from %%trows"
        )

        history_observed = self._wait_for_history()
        tdLog.info(f"observed fill-history progress: {history_observed!r}")

        tdSql.execute(
            "recalculate stream obs_progress.s_progress "
            "from '2025-01-01 00:00:00' to '2025-01-01 17:00:00'"
        )
        rows = self._query_rows(self.RECALC_SQL)
        pending = [row for row in rows if row[1] == "0%" and row[2] == "Pending"]
        if len(pending) != 1:
            raise AssertionError(f"accepted recalculation is not Pending/0: {rows!r}")
        recalc_id = pending[0][0]

        recalc_observed = [(0, "Pending")]
        recalc_observed = self._wait_for_recalc_terminal(recalc_id, recalc_observed)
        tdLog.info(f"observed manual recalculation progress: {recalc_observed!r}")

        tdSql.execute(
            "recalculate stream obs_progress.s_progress "
            "from '2020-01-01 00:00:00' to '2020-01-01 00:01:00'"
        )
        deadline = time.monotonic() + 30
        empty_rows = []
        while time.monotonic() < deadline:
            empty_rows = self._query_rows(self.RECALC_SQL)
            empty_finished = [
                row
                for row in empty_rows
                if row[0] != recalc_id and row[1] == "100%" and row[2] == "Finished"
            ]
            if empty_finished:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(
                f"empty recalculation did not finish at 100%: {empty_rows!r}"
            )

        tdSql.execute("create table obs_progress.site_3 using obs_progress.events tags(3)")
        tdSql.execute(
            "insert into obs_progress.site_3 values"
            "('2025-01-01 00:00:00',1)('2025-01-01 00:01:00',2)"
        )
        history_rows = self._query_rows(self.STREAM_SQL)
        if history_rows != [(100,)]:
            raise AssertionError(
                f"new group changed completed history denominator: {history_rows!r}"
            )

    def test_recalc_range_uses_trigger_precision(self):
        """Cross-database recalculation uses the trigger table precision.

        1. Create a millisecond stream database over a microsecond trigger table.
        2. Seed historical input before stream creation and establish its group later.
        3. Recalculate the historical range and verify that it is actually scanned.

        Catalog: Streams:Observability

        Since: v3.4.0.0

        Labels: stream,recalc,precision,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7045278024

        History:
            - 2026-08-21 OpenAI GPT-5 Created
        """
        tdStream.ensureSnode()
        tdSql.executes(
            [
                "drop database if exists recalc_stream_ms",
                "drop database if exists recalc_trigger_us",
                "create database recalc_stream_ms vgroups 1 precision 'ms'",
                "create database recalc_trigger_us vgroups 1 precision 'us'",
                "create stable recalc_trigger_us.events "
                "(ts timestamp, value int) tags(site int)",
                "create table recalc_trigger_us.site_1 using "
                "recalc_trigger_us.events tags(1)",
                "insert into recalc_trigger_us.site_1 values"
                "('2025-01-01 00:00:00',1)"
                "('2025-01-01 00:00:30',2)",
            ]
        )
        tdSql.execute(
            "create stream recalc_stream_ms.s_cross_precision "
            "interval(1m) sliding(1m) from recalc_trigger_us.events "
            "partition by tbname into recalc_trigger_us.result "
            "OUTPUT_SUBTABLE(CONCAT('result_', tbname)) "
            "(wstart, total) tags(source varchar(128) as tbname) "
            "as select _twstart, count(*) from %%trows"
        )

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            rows = self._query_rows(
                "select status from information_schema.ins_streams "
                "where db_name='recalc_stream_ms' "
                "and stream_name='s_cross_precision'"
            )
            if rows == [("Running",)]:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(f"cross-precision stream did not run: {rows!r}")

        tdSql.execute(
            "insert into recalc_trigger_us.site_1 "
            "values(now,3)(now+2m,4)"
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            rows = self._query_rows(
                "select table_name from information_schema.ins_tables "
                "where db_name='recalc_trigger_us' "
                "and table_name='result_site_1'"
            )
            if rows == [("result_site_1",)]:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(f"stream group was not established: {rows!r}")

        tdSql.execute(
            "recalculate stream recalc_stream_ms.s_cross_precision "
            "from '2025-01-01 00:00:00' to '2025-01-01 00:01:00'"
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            rows = self._query_rows(
                "select count(*) "
                "from information_schema.ins_stream_recalculates "
                "where stream_name='s_cross_precision' "
                "and `start`='2025-01-01 00:00:00' "
                "and `end`='2025-01-01 00:01:00'"
            )
            if rows == [(1,)]:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(f"recalculation view has wrong range: {rows!r}")

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            rows = self._query_rows(
                "select total from recalc_trigger_us.result_site_1 "
                "where wstart='2025-01-01 00:00:00'"
            )
            if rows == [(2,)]:
                return
            time.sleep(0.5)
        raise AssertionError(f"historical microsecond range was not scanned: {rows!r}")
