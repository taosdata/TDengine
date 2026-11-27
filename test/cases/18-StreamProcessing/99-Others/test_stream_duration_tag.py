import time

from new_test_framework.utils import tdLog, tdSql, tdStream

# 流输出至少需要两列数据列
_OUT_SELECT = "_twstart, max(value) as max_v"


class TestStreamDurationTag:
    """CREATE STREAM duration tag tests for true_for(tag) and max_delay(tag)."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.dropAllStreamsAndDbs()
        tdStream.ensureSnode()

    def _reset_db(self, db, vgroups=1):
        tdSql.execute(f"drop database if exists {db}", queryTimes=3)
        tdSql.prepare(dbname=db, vgroups=vgroups)
        tdSql.execute(f"use {db}")

    def _assert_event_out_row(self, row, twstart, max_v, tbname, persist_dur):
        """Assert stream event output row; ns 库 _twstart 带纳秒后缀，整型 tag 可能为 str。"""
        ts_ok = tdSql.compareData(row, 0, twstart) or tdSql.compareData(
            row, 0, f"{twstart}.000000000"
        )
        tag_ok = tdSql.compareData(row, 3, persist_dur) or tdSql.compareData(
            row, 3, str(persist_dur)
        )
        return (
            ts_ok
            and tdSql.compareData(row, 1, max_v)
            and tdSql.compareData(row, 2, tbname)
            and tag_ok
        )

    def _setup_meters(self, db):
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) tags(persist_dur varchar(16))"
        )

    def _check_no_output_during(self, queries, seconds=5):
        # Unlike retry-until-zero, every poll must stay empty for the full interval.
        if isinstance(queries, str):
            queries = [queries]
        deadline = time.monotonic() + seconds
        while True:
            for sql in queries:
                tdSql.query(sql)
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, 0)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.5, remaining))

    def test_duration_tag_01_interval_max_delay(self):
        """Verify per-group MAX_DELAY tag values on interval windows.

        1. Verify the 3-second group fires while a 1-hour group stays open
        2. Advance event time to close the slow group's window normally

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add required test metadata

        """
        db = "dur_tag_max_delay"
        self._setup_meters(db)
        tdSql.execute("create table d1 using meters tags('3s')")
        tdSql.execute("create table d2 using meters tags('1h')")
        # The slow deadline leaves ample room for the fast group's bounded poll.
        tdSql.execute(
            f"create stream s_max_delay "
            f"interval(10s) sliding(10s) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(max_delay(persist_dur)) "
            f"into s_max_delay_out as select _twstart, count(*) as cnt "
            f"from %%tbname where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus("s_max_delay")

        tdSql.execute(
            "insert into d1 values ('2025-01-01 00:00:00', 1) "
            "d2 values ('2025-01-01 00:00:00', 1)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_max_delay_out where tag_tbname='d1'",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, "d1")
            and tdSql.compareData(0, 3, "3s"),
            retry=30,
        )
        self._check_no_output_during(
            "select count(*) from s_max_delay_out where tag_tbname='d2'"
        )
        tdSql.execute("insert into d2 values ('2025-01-01 00:00:10', 1)")
        tdSql.checkResultsByFunc(
            sql="select * from s_max_delay_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, "d1")
            and tdSql.compareData(0, 3, "3s")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(1, 2, "d2")
            and tdSql.compareData(1, 3, "1h"),
            retry=30,
        )

    def test_duration_tag_04_valid_event_true_for(self):
        """Verify TRUE_FOR and MAX_DELAY tags on event windows.

        1. Verify an event window applies its per-group TRUE_FOR threshold
        2. Verify TRUE_FOR and MAX_DELAY tags can be combined

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add required test metadata

        """
        db = "dur_tag_event"
        self._setup_meters(db)
        tdSql.execute("create table d1 using meters tags('3s')")
        tdSql.execute("create table d2 using meters tags('5s')")
        tdSql.execute(
            f"create stream s_ok_event "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(event_type(window_close)) "
            f"into s_ok_event_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_ok_event")
        tdSql.execute(
            f"create stream s_ok_both "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(max_delay(persist_dur)|event_type(window_close)) "
            f"into s_ok_both_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_ok_both")

        tdSql.execute(
            "insert into d1 values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:03', 20),"
            "('2025-01-01 00:00:04', 0)"
        )
        for output_table in ("s_ok_event_out", "s_ok_both_out"):
            tdSql.checkResultsByFunc(
                sql=f"select * from {output_table} order by tag_tbname, 1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                and tdSql.compareData(0, 1, 20)
                and tdSql.compareData(0, 2, "d1")
                and tdSql.compareData(0, 3, "3s"),
                retry=60,
            )

    def test_duration_tag_05_valid_state_true_for(self):
        """Verify TRUE_FOR duration tags on state windows.

        1. Create state-window groups with different duration tags
        2. Verify each group applies its own TRUE_FOR threshold

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add required test metadata

        """
        db = "dur_tag_state"
        self._setup_meters(db)
        tdSql.execute("create table d1 using meters tags('3s')")
        tdSql.execute(
            f"create stream s_ok_state "
            f"state_window(value) true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"into s_ok_state_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_ok_state")
        tdSql.execute(
            "insert into d1 values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:03', 20),"
            "('2025-01-01 00:00:04', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_ok_state_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, 20)
            and tdSql.compareData(0, 2, "d1")
            and tdSql.compareData(0, 3, "3s"),
            retry=60,
        )

    def test_duration_tag_06_valid_true_for_1s(self):
        """Verify TRUE_FOR accepts durations below the MAX_DELAY minimum.

        1. Use 3-second and 1-second duration tags in separate groups
        2. Verify the 1-second TRUE_FOR threshold remains valid

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add required test metadata

        """
        db = "dur_tag_1s"
        self._setup_meters(db)
        tdSql.execute("create table d1 using meters tags('3s')")
        tdSql.execute("create table d_sub using meters tags('1s')")
        tdSql.execute(
            f"create stream s_ok_1s "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(event_type(window_close)) "
            f"into s_ok_1s_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_ok_1s")

        tdSql.execute(
            "insert into d1 values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:03', 20),"
            "('2025-01-01 00:00:04', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_ok_1s_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, 20)
            and tdSql.compareData(0, 2, "d1")
            and tdSql.compareData(0, 3, "3s"),
            retry=60,
        )
        tdSql.execute(
            "insert into d_sub values "
            "('2025-01-01 00:01:00', 20),"
            "('2025-01-01 00:01:01', 20),"
            "('2025-01-01 00:01:02', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_ok_1s_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, 20)
            and tdSql.compareData(0, 2, "d1")
            and tdSql.compareData(0, 3, "3s")
            and tdSql.compareData(1, 0, "2025-01-01 00:01:00")
            and tdSql.compareData(1, 1, 20)
            and tdSql.compareData(1, 2, "d_sub")
            and tdSql.compareData(1, 3, "1s"),
            retry=60,
        )

    def test_duration_tag_10_integer_tag(self):
        """Verify integer duration tags use database precision.

        1. Cover millisecond, microsecond, and nanosecond database precision
        2. Verify integer tag values are interpreted in database precision

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add required test metadata
            - 2026-10-08 Peng Rongkun Cover microsecond integer duration tags

        """
        db_ms = "dur_tag_int_ms"
        self._reset_db(db_ms)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) tags(persist_dur int)"
        )
        tdSql.execute("create table d_ms using meters tags(3000)")
        tdSql.execute(
            f"create stream s_int_ms "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(event_type(window_close)) "
            f"into s_int_ms_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_int_ms")
        tdSql.execute(
            "insert into d_ms values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:03', 20),"
            "('2025-01-01 00:00:04', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_int_ms_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, 20)
            and tdSql.compareData(0, 2, "d_ms")
            and tdSql.compareData(0, 3, 3000),
            retry=60,
        )

        db_us = "dur_tag_int_us"
        tdSql.execute(f"drop database if exists {db_us}", queryTimes=3)
        tdSql.execute(f"create database {db_us} precision 'us' vgroups 1")
        tdSql.execute(f"use {db_us}")
        tdSql.execute(
            "create stable meters(ts timestamp, value int) tags(persist_dur bigint)"
        )
        tdSql.execute("create table d_us using meters tags(3000000)")
        tdSql.execute(
            f"create stream s_int_us "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(event_type(window_close)) "
            f"into s_int_us_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_int_us")
        tdSql.execute(
            "insert into d_us values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:03', 20),"
            "('2025-01-01 00:00:04', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_int_us_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 1
            and self._assert_event_out_row(
                0, "2025-01-01 00:00:00", 20, "d_us", 3000000
            ),
            retry=60,
        )

        db_ns = "dur_tag_int_ns"
        tdSql.execute(f"drop database if exists {db_ns}", queryTimes=3)
        tdSql.execute(f"create database {db_ns} precision 'ns' vgroups 1")
        tdSql.execute(f"use {db_ns}")
        tdSql.execute(
            "create stable meters(ts timestamp, value int) tags(persist_dur bigint)"
        )
        tdSql.execute("create table d_ns using meters tags(3000000000)")
        tdSql.execute(
            f"create stream s_int_ns "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(event_type(window_close)) "
            f"into s_int_ns_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_int_ns")
        tdSql.execute(
            "insert into d_ns values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:01', 20),"
            "('2025-01-01 00:00:02', 0),"
            "('2025-01-01 00:00:10', 20),"
            "('2025-01-01 00:00:13', 20),"
            "('2025-01-01 00:00:14', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_int_ns_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 1
            and self._assert_event_out_row(
                0, "2025-01-01 00:00:10", 20, "d_ns", 3000000000
            ),
            retry=60,
        )

    def test_count_tag_01_window_count(self):
        """Verify COUNT tags on event windows.

        1. Produce output for a group whose COUNT threshold is met
        2. Filter a group whose COUNT threshold is not met

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add required test metadata

        """
        db = "dur_tag_count"
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) "
            "tags(need_rows int, region varchar(8))"
        )
        tdSql.execute("create table d_cnt2 using meters tags(2, 'r1')")
        tdSql.execute("create table d_cnt3 using meters tags(3, 'r1')")
        tdSql.execute(
            f"create stream s_cnt_tag "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(count need_rows) "
            f"from meters partition by tbname, need_rows, region "
            f"stream_options(event_type(window_close)) "
            f"into s_cnt_tag_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_cnt_tag")
        tdSql.execute(
            "insert into d_cnt2 values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:01', 0)"
            "d_cnt3 values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:01', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_cnt_tag_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 2, "d_cnt2"),
            retry=60,
        )
        tdSql.execute(
            "insert into d_cnt3 values "
            "('2025-01-01 00:00:02', 20),"
            "('2025-01-01 00:00:03', 20),"
            "('2025-01-01 00:00:04', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_cnt_tag_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
            and tdSql.compareData(1, 2, "d_cnt3"),
            retry=60,
        )

    def test_count_tag_02_start_end_combo(self):
        """Verify duration/COUNT OR with start and end COUNT tags.

        1. Apply duration OR COUNT to the whole window
        2. Apply independent COUNT tags to the start and end conditions
        3. Verify the event window emits only after all required limits are met

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add required test metadata

        """
        db = "dur_tag_count_combo"
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) "
            "tags(persist_dur varchar(16), need_rows int, start_rows int, "
            "min_close int, region varchar(8))"
        )
        tdSql.execute("create table d_combo using meters tags('3s', 3, 1, 1, 'r1')")
        tdSql.execute(
            f"create stream s_combo_tag "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur or count need_rows, "
            f"start(count start_rows), end(count min_close)) "
            f"from meters partition by tbname, persist_dur, need_rows, start_rows, min_close, region "
            f"stream_options(event_type(window_close)) "
            f"into s_combo_tag_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_combo_tag")
        tdSql.execute(
            "insert into d_combo values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:01', 0),"
            "('2025-01-01 00:00:10', 20),"
            "('2025-01-01 00:00:11', 20),"
            "('2025-01-01 00:00:12', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_combo_tag_out order by tag_tbname, 1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
            and tdSql.compareData(0, 2, "d_combo"),
            retry=60,
        )


    def do_count_tag_invalid_zero_fallback(self):
        db = "dur_tag_count_zero"
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) "
            "tags(need_rows int, region varchar(8))"
        )
        tdSql.execute("create table d_zero using meters tags(0, 'r1')")
        tdSql.execute("create table d_null using meters tags(NULL, 'r1')")
        tdSql.execute(
            f"create stream s_zero_cnt "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(count need_rows) "
            f"from meters partition by tbname, need_rows, region "
            f"stream_options(event_type(window_close)) "
            f"into s_zero_cnt_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_zero_cnt")
        tdSql.execute(
            "insert into d_zero values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:01', 20),"
            "('2025-01-01 00:00:02', 0) "
            "d_null values "
            "('2025-01-01 00:01:00', 20),"
            "('2025-01-01 00:01:01', 20),"
            "('2025-01-01 00:01:02', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select tag_tbname from s_zero_cnt_out order by tag_tbname",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "d_null")
            and tdSql.compareData(1, 0, "d_zero"),
            retry=60,
        )
        tdLog.info("COUNT tag zero/NULL fallback ................ [ passed ]")

    def test_count_tag_04_invalid_zero(self):
        """COUNT tag zero and NULL fall back to zero without blocking a group.

        1. Verify COUNT tag value 0 does not filter the group's event window
        2. Verify a NULL COUNT tag logs an error path but still emits the window

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Codex Update invalid COUNT tag behavior to zero fallback

        """
        self.do_count_tag_invalid_zero_fallback()

    def do_invalid_tag_fallback_preserves_other_thresholds(self):
        db = "dur_tag_invalid_slot"
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) "
            "tags(persist_dur varchar(16), need_rows int, region varchar(8))"
        )
        tdSql.execute("create table d_null_dur using meters tags(NULL, 3, 'r1')")
        tdSql.execute("create table d_zero_dur using meters tags('0s', 3, 'r1')")
        tdSql.execute("create table d_null_count using meters tags('3s', NULL, 'r1')")
        tdSql.execute(
            f"create stream s_invalid_slot "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur and count need_rows) "
            f"from meters partition by tbname, persist_dur, need_rows, region "
            f"stream_options(event_type(window_close)) "
            f"into s_invalid_slot_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_invalid_slot")

        # A NULL duration disables only duration; the valid count threshold remains 3.
        tdSql.execute(
            "insert into d_null_dur values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:01', 0),"
            "('2025-01-01 00:00:10', 20),"
            "('2025-01-01 00:00:11', 20),"
            "('2025-01-01 00:00:12', 0)"
        )

        # A zero duration is invalid and disables only duration, like NULL.
        tdSql.execute(
            "insert into d_zero_dur values "
            "('2025-01-01 00:00:40', 20),"
            "('2025-01-01 00:00:41', 20),"
            "('2025-01-01 00:00:42', 0)"
        )

        # A NULL count disables only count; the valid duration threshold remains 3s.
        tdSql.execute(
            "insert into d_null_count values "
            "('2025-01-01 00:00:20', 20),"
            "('2025-01-01 00:00:22', 0),"
            "('2025-01-01 00:00:30', 20),"
            "('2025-01-01 00:00:33', 20),"
            "('2025-01-01 00:00:34', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select tag_tbname, `_twstart` from s_invalid_slot_out order by tag_tbname",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "d_null_count")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:30")
            and tdSql.compareData(1, 0, "d_null_dur")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:10")
            and tdSql.compareData(2, 0, "d_zero_dur")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:40"),
            retry=60,
        )
        tdLog.info("invalid tag slot fallback .................... [ passed ]")

    def test_count_tag_05_invalid_slot_fallback(self):
        """An invalid TRUE_FOR tag slot falls back without clearing valid slots.

        1. Verify NULL and zero duration fall back while COUNT remains effective
        2. Verify NULL count falls back to zero while duration remains effective

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Codex Add per-slot invalid tag fallback coverage
            - 2026-10-08 Peng Rongkun Treat zero duration tags as invalid

        """
        self.do_invalid_tag_fallback_preserves_other_thresholds()

    def do_truefor_tag_runtime_refresh(self):
        db = "dur_tag_set_tag"
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) tags(persist_dur varchar(16), region varchar(8))"
        )
        tdSql.execute("create table d1 using meters tags('1s', 'r1')")
        tdSql.execute(
            f"create stream s_set_tag "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname "
            f"stream_options(event_type(window_close)) "
            f"into s_set_tag_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_set_tag")

        tdSql.execute(
            "insert into d1 values "
            "('2025-01-01 00:00:00', 20),"
            "('2025-01-01 00:00:01', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select * from s_set_tag_out order by 1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, 20)
            and tdSql.compareData(0, 2, "d1"),
            retry=60,
        )

        tdSql.execute("alter table d1 set tag persist_dur='-1s'")
        time.sleep(2)
        tdSql.execute(
            "insert into d1 values "
            "('2025-01-01 00:01:00', 20),"
            "('2025-01-01 00:01:00.500', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select count(*) from s_set_tag_out",
            func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 2),
            retry=60,
        )

        tdSql.execute("alter table d1 set tag persist_dur='2s'")
        time.sleep(2)
        tdSql.execute(
            "insert into d1 values "
            "('2025-01-01 00:02:00', 20),"
            "('2025-01-01 00:02:01', 0),"
            "('2025-01-01 00:02:02', 20),"
            "('2025-01-01 00:02:03', 20),"
            "('2025-01-01 00:02:04', 0)"
        )
        tdSql.checkResultsByFunc(
            sql="select `_twstart` from s_set_tag_out order by 1",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(1, 0, "2025-01-01 00:01:00")
            and tdSql.compareData(2, 0, "2025-01-01 00:02:02"),
            retry=60,
        )

        # DROP TAG 仍拒绝
        tdSql.error(
            "alter table meters drop tag persist_dur",
            expectErrInfo="Col/Tag referenced by stream",
            fullMatched=False,
        )

        tdSql.execute("drop stream s_set_tag")
        tdSql.execute("drop table if exists s_set_tag_out")

        # PARTITION BY 含 duration Tag 时，SET TAG 应报 COL_TAG_REF_BY_STM
        tdSql.execute("create table d2 using meters tags('1s', 'r1')")
        tdSql.execute(
            f"create stream s_set_tag_part "
            f"event_window(start with value > 10 end with value <= 10) "
            f"true_for(persist_dur) "
            f"from meters partition by tbname, persist_dur "
            f"stream_options(event_type(window_close)) "
            f"into s_set_tag_part_out as select {_OUT_SELECT} from %%tbname"
        )
        tdStream.checkStreamStatus("s_set_tag_part")
        tdSql.error(
            "alter table d2 set tag persist_dur='3s'",
            expectErrInfo="Col/Tag referenced by stream",
            fullMatched=False,
        )
        tdLog.info("TRUE_FOR SET TAG fallback .................... [ passed ]")

    def test_truefor_tag_runtime_refresh(self):
        """SET TAG refreshes TRUE_FOR and invalid values fall back to zero.

        1. Verify a valid-to-invalid update continues producing with threshold zero
        2. Verify a later valid update applies its new duration threshold
        3. Verify an explicitly partitioned tag still rejects SET TAG

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Codex Update SET TAG invalid-value behavior

        """
        self.do_truefor_tag_runtime_refresh()

    def do_maxdelay_invalid_tag_fallback(self):
        db = "dur_tag_md_invalid"
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) "
            "tags(persist_dur varchar(16), region varchar(8))"
        )
        tdSql.execute("create table d_invalid using meters tags('-1s', 'r1')")
        tdSql.execute("create table d_null using meters tags(NULL, 'r1')")
        tdSql.execute(
            "create stream s_md_invalid "
            "interval(10s) sliding(10s) "
            "from meters partition by tbname, persist_dur, region "
            "stream_options(max_delay(persist_dur)) "
            "into s_md_invalid_out as select _twstart, count(*) as cnt "
            "from %%tbname where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus("s_md_invalid")

        # max_delay=0 disables the timer, but later data must still close prior windows.
        tdSql.execute(
            "insert into d_invalid values "
            "('2025-01-01 00:00:00', 1),"
            "('2025-01-01 00:00:10', 1) "
            "d_null values "
            "('2025-01-01 00:01:00', 1),"
            "('2025-01-01 00:01:10', 1)"
        )
        tdSql.checkResultsByFunc(
            sql="select tag_tbname from s_md_invalid_out order by tag_tbname",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "d_invalid")
            and tdSql.compareData(1, 0, "d_null"),
            retry=60,
        )
        tdLog.info("MAX_DELAY invalid tag fallback ............... [ passed ]")

    def test_maxdelay_tag_invalid_fallback(self):
        """Invalid MAX_DELAY tags fall back to zero without blocking data.

        1. Verify a negative duration tag does not block normal interval closure
        2. Verify a NULL duration tag does not block normal interval closure

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Codex Add MAX_DELAY invalid-tag fallback coverage

        """
        self.do_maxdelay_invalid_tag_fallback()

    def test_maxdelay_tag_runtime_refresh(self):
        """Verify MAX_DELAY tag updates refresh active groups.

        1. Shorten MAX_DELAY on an open window and emit without another insert
        2. Verify valid, invalid, and valid-again updates refresh the threshold
        3. Reject SET TAG when the duration tag is explicitly partitioned

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-08 Peng Rongkun Add active-window SET TAG regression coverage

        """
        db = "dur_tag_md_set"
        self._reset_db(db)
        tdSql.execute(
            "create stable meters(ts timestamp, value int) tags(persist_dur varchar(16), region varchar(8))"
        )
        tdSql.execute("create table d1 using meters tags('3s', 'r1')")
        tdSql.execute("create table d_refresh using meters tags('1h', 'r2')")
        tdSql.execute(
            "create stream s_md_set "
            "interval(10s) sliding(10s) "
            "from meters partition by tbname "
            "stream_options(max_delay(persist_dur)) "
            "into s_md_set_out as select _twstart, count(*) as cnt "
            "from %%tbname where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus("s_md_set")

        tdSql.execute("insert into d1 values ('2025-01-01 00:00:00', 1)")
        tdSql.checkResultsByFunc(
            sql="select * from s_md_set_out order by 1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, "d1"),
            retry=60,
        )

        # Shorten an open window's deadline; no post-ALTER insert may be needed to wake it.
        tdSql.execute("insert into d_refresh values ('2025-01-01 00:00:20', 1)")
        time.sleep(2)
        tdSql.execute("alter table d_refresh set tag persist_dur='3s'")
        tdSql.checkResultsByFunc(
            sql="select * from s_md_set_out where tag_tbname='d_refresh'",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:20")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, "d_refresh"),
            retry=60,
        )

        # 合法 → 非法（-1s）：等流消费 SET TAG 后再写；若仍冻在旧 3s，约 3s 后会误产出
        tdSql.execute("alter table d1 set tag persist_dur='-1s'")
        time.sleep(2)
        tdSql.execute("insert into d1 values ('2025-01-01 00:01:00', 1)")
        time.sleep(5)
        tdSql.query("select count(*) from s_md_set_out")
        tdSql.checkData(0, 0, 2)

        # 非法 → 合法（5s）：等流消费 SET TAG 后再写，验证热更新
        tdSql.execute("alter table d1 set tag persist_dur='5s'")
        time.sleep(2)
        tdSql.execute("insert into d1 values ('2025-01-01 00:02:00', 1)")
        tdSql.checkResultsByFunc(
            sql="select count(*) from s_md_set_out where cnt > 0",
            func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 4),
            retry=60,
        )

        # DROP TAG 仍拒绝
        tdSql.error(
            "alter table meters drop tag persist_dur",
            expectErrInfo="Col/Tag referenced by stream",
            fullMatched=False,
        )

        tdSql.execute("drop stream s_md_set")
        tdSql.execute("drop table if exists s_md_set_out")

        # PARTITION BY 含 duration Tag 时，SET TAG 应报 COL_TAG_REF_BY_STM
        tdSql.execute("create table d2 using meters tags('3s', 'r2')")
        tdSql.execute(
            "create stream s_md_set_part "
            "interval(10s) sliding(10s) "
            "from meters partition by tbname, persist_dur "
            "stream_options(max_delay(persist_dur)) "
            "into s_md_set_part_out as select _twstart, count(*) as cnt "
            "from %%tbname where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus("s_md_set_part")
        tdSql.error(
            "alter table d2 set tag persist_dur='5s'",
            expectErrInfo="Col/Tag referenced by stream",
            fullMatched=False,
        )

    def do_maxdelay_tag_precisions(self):
        for precision, scale in (("ms", 1), ("us", 1000), ("ns", 1000000)):
            db = f"dur_tag_md_{precision}"
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} precision '{precision}' vgroups 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable meters(ts timestamp, value int) tags(delay bigint)"
            )
            tdSql.execute(
                f"create table valid_delay using meters tags({3000 * scale})"
            )
            tdSql.execute(
                f"create table low_delay using meters tags({2000 * scale})"
            )
            tdSql.execute(
                "create stream s_md interval(10s) sliding(10s) "
                "from meters partition by tbname "
                "stream_options(max_delay(delay)) "
                "into out_md as select _twstart, count(*) as cnt "
                "from %%tbname where ts >= _twstart and ts < _twend"
            )
            tdStream.checkStreamStatus("s_md")
            tdSql.execute(
                "insert into valid_delay values ('2025-01-01 00:00:00', 1) "
                "low_delay values ('2025-01-01 00:00:00', 1)"
            )
            tdSql.checkResultsByFunc(
                sql="select cnt from out_md where tag_tbname='valid_delay'",
                func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 1),
                retry=60,
            )
            self._check_no_output_during(
                "select count(*) from out_md where tag_tbname='low_delay'"
            )
            # An invalid 2s timer falls back to zero without dropping the group.
            tdSql.execute("insert into low_delay values ('2025-01-01 00:00:10', 1)")
            tdSql.checkResultsByFunc(
                sql="select cnt from out_md where tag_tbname='low_delay'",
                func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 1),
                retry=60,
            )
            tdSql.execute("drop stream s_md")
        tdLog.info("MAX_DELAY tag precision boundaries ........... [ passed ]")

    def test_maxdelay_tag_precisions(self):
        """MAX_DELAY tag parsing preserves precision and its 3-second minimum.

        1. Verify integer 3-second tags at ms, us, and ns precision
        2. Verify 2-second tags disable the timer while data still closes windows

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-10 Peng Rongkun Cover MAX_DELAY integer precision
            - 2026-10-08 Peng Rongkun Cover microsecond integer duration tags

        """
        self.do_maxdelay_tag_precisions()

    def do_threshold_tag_parser_rejections(self):
        self._reset_db("dur_tag_parser_rejections")
        tdSql.execute(
            "create stable meters(ts timestamp, value int) "
            "tags(d_bool bool, d_float float, d_ts timestamp, d_str varchar(16), d_int int)"
        )
        for index, (limit, tag_kind) in enumerate(
            (("d_str", "duration"), ("count d_int", "count"))
        ):
            tdSql.error(
                f"create stream s_missing_part_{index} state_window(value) "
                f"true_for({limit}) from meters "
                f"into out_missing_part_{index} as select {_OUT_SELECT} from meters",
                expectErrInfo=f"{tag_kind} tag column must appear in PARTITION BY",
                fullMatched=False,
            )
        for index, (limit, expected_error) in enumerate((
            ("d_bool", "duration tag must be string"),
            ("d_float", "duration tag must be string"),
            ("d_ts", "duration tag must be string"),
            ("count d_str", "count tag must be integer type"),
        )):
            tdSql.error(
                f"create stream s_bad_type_{index} state_window(value) true_for({limit}) "
                "from meters partition by tbname "
                f"into out_bad_type_{index} as select {_OUT_SELECT} from %%tbname",
                expectErrInfo=expected_error,
                fullMatched=False,
            )
        for limit, expected_error in (
            ("d_str", "duration tag is only supported in CREATE STREAM"),
            ("count d_int", "count tag is only supported in CREATE STREAM"),
        ):
            tdSql.error(
                f"select count(*) from meters state_window(value) true_for({limit})",
                expectErrInfo=expected_error,
                fullMatched=False,
            )
            for subquery in (False, True):
                query = (
                    "select _wstart as ts, count(*) as cnt from meters "
                    f"partition by tbname state_window(value) true_for({limit})"
                )
                if subquery:
                    query = f"select ts, cnt from ({query})"
                tdSql.error(
                    "create stream s_bad_calc_tag interval(1s) sliding(1s) "
                    "from meters partition by tbname into out_bad_calc_tag as " + query,
                    expectErrInfo=expected_error,
                    fullMatched=False,
                )
        for function, expression in (("diff", "diff(value)"), ("derivative", "derivative(value, 1s, 0)")):
            tdSql.error(
                "select count(*) from meters "
                f"event_window start with {expression} > 0 end with value <= 0",
                expectErrInfo=f"Indefinite rows function {function} only allowed in select clause",
                fullMatched=False,
            )
            tdSql.error(
                f"create stream s_bad_window_{function} "
                f"event_window(start with ({expression} > 0, value > 0) end with value <= 0) "
                "from meters partition by tbname "
                f"into out_bad_window_{function} as select {_OUT_SELECT} from %%tbname"
            )
        tdLog.info("threshold tag parser rejections ............... [ passed ]")

    def test_threshold_tag_parser_rejections(self):
        """Reject invalid threshold tag types and non-stream tag thresholds.

        1. Require threshold tags in PARTITION BY
        2. Reject BOOL, FLOAT, and TIMESTAMP duration tags and string COUNT tags
        3. Reject duration and COUNT tags in ordinary and stream calculation SELECT windows
        4. Reject DIFF/DERIVATIVE in ordinary and stream window conditions

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-10 Peng Rongkun Cover threshold type and context rejection
            - 2026-10-08 Peng Rongkun Reject stateful row functions in stream conditions
            - 2026-10-22 Peng Rongkun Reject tags in calculation windows and subqueries

        """
        self.do_threshold_tag_parser_rejections()

    def do_event_start_end_tag_history(self):
        for history in (False, True):
            for index, limit in enumerate((
                "start(count start_rows), end(count end_rows)",
                "start(start_dur), end(end_dur)",
                "start(start_dur and count start_rows), end(end_dur or count end_rows)",
            )):
                self._reset_db(f"dur_tag_streak_{int(history)}_{index}")
                tdSql.execute(
                    "create stable meters(ts timestamp, value int) "
                    "tags(start_rows int, end_rows int, start_dur varchar(16), end_dur varchar(16))"
                )
                tdSql.execute("create table d1 using meters tags(2, 2, '1s', '1s')")
                rows = (
                    "insert into d1 values "
                    "('2025-01-01 00:00:00', 20),('2025-01-01 00:00:01', 0),"
                    "('2025-01-01 00:00:10', 20),('2025-01-01 00:00:11', 20),"
                    "('2025-01-01 00:00:12', 0),('2025-01-01 00:00:13', 20),"
                    "('2025-01-01 00:00:14', 0),('2025-01-01 00:00:15', 0)"
                )
                # The first start streak and the first end streak must be discarded.
                if history:
                    tdSql.execute(rows)
                history_option = "fill_history|" if history else ""
                tdSql.execute(
                    "create stream s_streak event_window(start with value > 10 end with value <= 10) "
                    f"true_for({limit}) from meters partition by tbname "
                    f"stream_options({history_option}event_type(window_close)) "
                    "into out_streak as select _twstart as ws, _twend as we, count(*) as cnt from %%tbname"
                )
                tdStream.checkStreamStatus("s_streak")
                if not history:
                    tdSql.execute(rows)
                tdSql.checkResultsByFunc(
                    sql="select ws, we from out_streak order by ws",
                    func=lambda: tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 00:00:10")
                    and tdSql.compareData(0, 1, "2025-01-01 00:00:14"),
                    retry=60,
                )
                tdSql.execute("drop stream s_streak")
        tdLog.info("event start/end tag history .................. [ passed ]")

    def test_event_start_end_tag_history(self):
        """Match single-condition event start/end Tag streaks in history and real time.

        1. Reject incomplete COUNT and duration start streaks
        2. Reset interrupted end streaks and retain their first timestamp on close
        3. Compare duration, COUNT, and mixed Tag thresholds in both processing modes

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-10-22 Peng Rongkun Cover start/end Tag thresholds with FILL_HISTORY

        """
        self.do_event_start_end_tag_history()

    def do_history_pending_open_tag_thresholds(self):
        cases = (
            ("count need_rows", ("both_ok", "count_ok")),
            ("persist_dur", ("both_ok", "duration_ok")),
            ("persist_dur and count need_rows", ("both_ok",)),
            ("persist_dur or count need_rows", ("both_ok", "count_ok", "duration_ok")),
            ("count 3, start(count start_rows)", ("both_ok", "count_ok")),
            ("3s, start(count start_rows)", ("both_ok", "duration_ok")),
            ("count 3, end(count end_rows)", ("both_ok", "count_ok")),
            ("3s, end(end_dur)", ("both_ok", "duration_ok")),
            ("persist_dur and count need_rows, start(count start_rows)", ("both_ok",)),
            ("persist_dur or count need_rows, end(end_dur)", ("both_ok", "count_ok", "duration_ok")),
        )
        for index, (limit, expected) in enumerate(cases):
            self._reset_db(f"dur_tag_hist_open_{index}")
            tdSql.execute(
                "create stable meters(ts timestamp, value int) "
                "tags(need_rows int, persist_dur varchar(16), start_rows int, end_rows int, end_dur varchar(16))"
            )
            for table, seconds in (
                ("short", (0, 1)),
                ("count_ok", (0, 0.5, 1)),
                ("duration_ok", (0, 4)),
                ("both_ok", (0, 1, 4)),
            ):
                tdSql.execute(f"create table {table} using meters tags(3, '3s', 1, 1, '1s')")
                # All rows precede CREATE STREAM; no row closes the event window.
                for second in seconds:
                    timestamp = 1735689600000 + int(second * 1000)
                    tdSql.execute(f"insert into {table} values({timestamp}, 20)")
            tdSql.execute(
                "create stream s_hist_open "
                "event_window(start with value > 10 end with value <= 10) "
                f"true_for({limit}) from meters partition by tbname "
                "stream_options(fill_history|event_type(window_open)) "
                "into out_hist_open as select _twstart as ws, count(*) as cnt from %%tbname"
            )
            tdStream.checkStreamStatus("s_hist_open")
            tdSql.checkResultsByFunc(
                sql="select count(*) from out_hist_open where tag_tbname='both_ok'",
                func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 1),
                retry=60,
            )
            names = ",".join(f"'{name}'" for name in expected)
            self._check_no_output_during(
                f"select count(*) from out_hist_open where tag_tbname not in ({names})"
            )
            tdSql.checkResultsByFunc(
                sql="select tag_tbname from out_hist_open order by tag_tbname",
                func=lambda: tdSql.getRows() == len(expected)
                and all(tdSql.compareData(row, 0, name) for row, name in enumerate(expected)),
                retry=60,
            )
            tdSql.execute("drop stream s_hist_open")
        tdLog.info("history pending open tag thresholds .......... [ passed ]")

    def test_history_pending_open_tag_thresholds(self):
        """Apply frozen Tag thresholds to pending history WINDOW_OPEN events.

        1. Keep incomplete historical COUNT and duration windows from emitting OPEN
        2. Emit qualified windows without a closing row
        3. Preserve duration/COUNT AND and OR combinations
        4. Cover single-condition history trees selected by start/end Tag thresholds

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-10-22 Peng Rongkun Cover pending history OPEN Tag thresholds
            - 2026-10-26 Peng Rongkun Cover global thresholds with start/end Tags

        """
        self.do_history_pending_open_tag_thresholds()

    def do_history_state_tail_tag_thresholds(self):
        cases = (
            ("persist_dur", ("both_ok", "duration_ok")),
            ("count need_rows", ("both_ok", "count_ok")),
            ("persist_dur and count need_rows", ("both_ok",)),
            ("persist_dur or count need_rows", ("both_ok", "count_ok", "duration_ok")),
            ("3s", ("both_ok", "duration_ok")),
            ("count 3", ("both_ok", "count_ok")),
        )
        rows = {
            "short": (0, 1000),
            "count_ok": (0, 500, 1000),
            "duration_ok": (0, 3000),
            "both_ok": (0, 1000, 3000),
        }
        for index, (limit, expected) in enumerate(cases):
            self._reset_db(f"dur_tag_hist_state_tail_{index}")
            tdSql.execute(
                "create stable meters(ts timestamp, value int) "
                "tags(persist_dur varchar(16), need_rows int)"
            )
            for table, offsets in rows.items():
                tdSql.execute(f"create table {table} using meters tags('3s', 3)")
                values = ",".join(f"({1735689600000 + offset}, 20)" for offset in offsets)
                tdSql.execute(f"insert into {table} values {values}")
            # No state change closes these windows; only history EOF can close them.
            tdSql.execute(
                "create stream s_hist_state_tail state_window(value) "
                f"true_for({limit}) from meters partition by tbname "
                "stream_options(fill_history|event_type(window_close)) "
                "into out_hist_state_tail as select _twstart as ws, count(*) as cnt from %%tbname"
            )
            tdStream.checkStreamStatus("s_hist_state_tail")
            tdSql.checkResultsByFunc(
                sql="select count(*) from out_hist_state_tail where tag_tbname='both_ok'",
                func=lambda: tdSql.getRows() == 1 and tdSql.compareData(0, 0, 1),
                retry=60,
            )
            names = ",".join(f"'{name}'" for name in expected)
            self._check_no_output_during(
                f"select count(*) from out_hist_state_tail where tag_tbname not in ({names})"
            )
            tdSql.checkResultsByFunc(
                sql="select tag_tbname, cnt from out_hist_state_tail order by tag_tbname",
                func=lambda: tdSql.getRows() == len(expected)
                and all(
                    tdSql.compareData(row, 0, name)
                    and tdSql.compareData(row, 1, len(rows[name]))
                    for row, name in enumerate(expected)
                ),
                retry=60,
            )
            tdSql.execute("drop stream s_hist_state_tail")
        tdLog.info("history state tail tag thresholds ............ [ passed ]")

    def test_history_state_tail_tag_thresholds(self):
        """Apply frozen TRUE_FOR thresholds when history EOF closes a state window.

        1. Filter historical tail windows below duration or COUNT Tag thresholds
        2. Preserve duration/COUNT AND and OR combinations at the threshold boundary
        3. Keep literal duration and COUNT threshold behavior unchanged

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-11-02 Peng Rongkun Cover frozen Tag thresholds at history state EOF

        """
        self.do_history_state_tail_tag_thresholds()

    def do_native_child_tag_thresholds(self):
        for history in (False, True):
            self._reset_db(f"dur_tag_child_{'hist' if history else 'live'}")
            tdSql.execute(
                "create stable meters(ts timestamp, value int) "
                "tags(persist_dur varchar(16), need_rows int, delay varchar(16))"
            )
            tdSql.execute("create table event_child using meters tags('3s', 3, '3s')")
            # Four windows: neither limit, duration only, count only, both limits.
            # The closing row counts toward the window's three-row threshold.
            event_rows = (
                "insert into event_child values "
                "('2025-01-01 00:00:00', 20),('2025-01-01 00:00:01', 0),"
                "('2025-01-01 00:00:10', 20),('2025-01-01 00:00:14', 0),"
                "('2025-01-01 00:00:20', 20),('2025-01-01 00:00:21', 20),"
                "('2025-01-01 00:00:22', 0),"
                "('2025-01-01 00:00:30', 20),('2025-01-01 00:00:31', 20),"
                "('2025-01-01 00:00:34', 0)"
            )
            if history:
                tdSql.execute(event_rows)
            history_option = "fill_history|" if history else ""
            tdSql.execute(
                "create stream s_child_event "
                "event_window(start with value > 10 end with value <= 10) "
                "true_for(persist_dur and count need_rows) "
                "from event_child partition by tbname "
                f"stream_options({history_option}event_type(window_close)) "
                f"into out_child_event as select {_OUT_SELECT} from %%tbname"
            )
            tdStream.checkStreamStatus("s_child_event")
            if not history:
                tdSql.execute(event_rows)
            tdSql.checkResultsByFunc(
                sql="select `_twstart`, max_v from out_child_event order by 1",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, "2025-01-01 00:00:30")
                and tdSql.compareData(0, 1, 20),
                retry=60,
            )

            tdSql.execute("drop stream s_child_event")

        # MAX_DELAY is independent of history mode, so cover the child-table lookup once.
        tdSql.execute("create table timer_child using meters tags('3s', 3, '3s')")
        tdSql.execute(
            "create stream s_child_timer interval(10s) sliding(10s) "
            "from timer_child partition by tbname "
            "stream_options(max_delay(delay)) "
            "into out_child_timer as select _twstart, count(*) as cnt "
            "from %%tbname where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus("s_child_timer")
        # No later input closes this window; the child table's 3s tag must fire it.
        tdSql.execute("insert into timer_child values ('2025-01-01 00:01:00', 1)")
        tdSql.checkResultsByFunc(
            sql="select `_twstart`, cnt from out_child_timer",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:01:00")
            and tdSql.compareData(0, 1, 1),
            retry=60,
        )
        tdSql.execute("drop stream s_child_timer")
        tdLog.info("native child tag thresholds .................. [ passed ]")

    def test_native_child_tag_thresholds(self):
        """Direct child-table triggers resolve duration, count, and MAX_DELAY tags.

        1. Filter windows that miss either duration or count in real-time and history
        2. Emit the window satisfying both tags when FROM names the child table
        3. Fire a 3-second MAX_DELAY tag without subsequent input

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7056479926

        Jira: None

        History:
            - 2026-09-10 Peng Rongkun Cover direct child-table threshold tags

        """
        self.do_native_child_tag_thresholds()
