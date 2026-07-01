import time

from new_test_framework.utils import clusterComCheck, tdLog, tdSql, tdStream


TSDB_CODE_STREAM_INVALID_TRIGGER = 0x410E


class TestStreamRecalcCountWindowSlide1:
    RECALC_RETRY = 60
    STABLE_RETRY = 10

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_count_window_slide1_recalc(self):
        """Recalc: count window slide-one automatic recalculation

        Verify count_window(n, 1) and count_window(1) use automatic
        recalculation for disorder data while count windows with other sliding
        steps still ignore disorder and reject delete_recalc.

        Catalog:
            - Streams:Recalculation:CountWindow

        Since: v3.3.7.0

        Labels: common,ci,integration,functional
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6670762934

        History:
            - 2026-06-29 kanekuang Created

        """

        self.create_snode()
        self.create_database()
        self.prepare_query_data()
        self.prepare_trigger_table()
        self.create_streams()
        self.check_stream_status()
        self.write_initial_trigger_data()
        self.check_initial_results()
        self.check_disorder_recalc()
        self.check_ignore_disorder_precedence()
        self.check_default_count_window_ignores_disorder()
        self.check_default_one_count_window_recalc()
        self.check_slide2_count_window_ignores_disorder()
        self.check_delete_recalc_rejections()

    def create_snode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def create_database(self):
        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1)
        tdSql.prepare(dbname="rdb", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb")
        clusterComCheck.checkDbReady("rdb")

    def prepare_query_data(self):
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

    def prepare_trigger_table(self):
        tdSql.execute(
            "create table tdb.cw_slide1 "
            "(ts timestamp, val int, category varchar(16)) tags(device_id int);"
        )
        tdSql.execute("create table tdb.cw1 using tdb.cw_slide1 tags(1)")
        tdSql.execute("create table tdb.cw_ignore using tdb.cw_slide1 tags(2)")
        tdSql.execute("create table tdb.cw_default using tdb.cw_slide1 tags(3)")
        tdSql.execute("create table tdb.cw_slide2 using tdb.cw_slide1 tags(4)")
        tdSql.execute("create table tdb.cw_default_one using tdb.cw_slide1 tags(5)")

    def create_streams(self):
        tdSql.execute(
            "create stream rdb.s_cw_slide1 count_window(3, 1) "
            "from tdb.cw_slide1 partition by tbname into rdb.r_cw_slide1 "
            "as select _twstart ts, count(*) cnt, avg(cint) avg_val "
            "from qdb.meters where cts >= _twstart and cts < _twend;"
        )
        tdSql.execute(
            "create stream rdb.s_cw_ignore count_window(3, 1) "
            "from tdb.cw_slide1 partition by tbname stream_options(ignore_disorder) "
            "into rdb.r_cw_ignore "
            "as select _twstart ts, count(*) cnt, avg(cint) avg_val "
            "from qdb.meters where cts >= _twstart and cts < _twend;"
        )
        tdSql.execute(
            "create stream rdb.s_cw_default count_window(3) "
            "from tdb.cw_slide1 partition by tbname into rdb.r_cw_default "
            "as select _twstart ts, count(*) cnt, avg(cint) avg_val "
            "from qdb.meters where cts >= _twstart and cts < _twend;"
        )
        tdSql.execute(
            "create stream rdb.s_cw_slide2 count_window(3, 2) "
            "from tdb.cw_slide1 partition by tbname into rdb.r_cw_slide2 "
            "as select _twstart ts, count(*) cnt, avg(cint) avg_val "
            "from qdb.meters where cts >= _twstart and cts < _twend;"
        )
        tdSql.execute(
            "create stream rdb.s_cw_default_one count_window(1) "
            "from tdb.cw_slide1 partition by tbname into rdb.r_cw_default_one "
            "as select _twstart ts, count(*) cnt, avg(cint) avg_val "
            "from qdb.meters where cts >= _twstart and cts < _twend;"
        )

    def check_stream_status(self):
        tdStream.checkStreamStatus()

    def write_initial_trigger_data(self):
        tdSql.executes(
            [
                "insert into tdb.cw1 values ('2025-01-01 02:00:00', 10, 'normal');",
                "insert into tdb.cw1 values ('2025-01-01 02:00:15', 20, 'normal');",
                "insert into tdb.cw1 values ('2025-01-01 02:00:30', 30, 'normal');",
                "insert into tdb.cw1 values ('2025-01-01 02:00:45', 40, 'normal');",
                "insert into tdb.cw1 values ('2025-01-01 02:01:00', 50, 'normal');",
                "insert into tdb.cw_ignore values ('2025-01-01 03:00:00', 10, 'normal');",
                "insert into tdb.cw_ignore values ('2025-01-01 03:00:15', 20, 'normal');",
                "insert into tdb.cw_ignore values ('2025-01-01 03:00:30', 30, 'normal');",
                "insert into tdb.cw_ignore values ('2025-01-01 03:00:45', 40, 'normal');",
                "insert into tdb.cw_ignore values ('2025-01-01 03:01:00', 50, 'normal');",
                "insert into tdb.cw_default values ('2025-01-01 04:00:00', 10, 'normal');",
                "insert into tdb.cw_default values ('2025-01-01 04:00:15', 20, 'normal');",
                "insert into tdb.cw_default values ('2025-01-01 04:00:30', 30, 'normal');",
                "insert into tdb.cw_default values ('2025-01-01 04:00:45', 40, 'normal');",
                "insert into tdb.cw_default values ('2025-01-01 04:01:00', 50, 'normal');",
                "insert into tdb.cw_default values ('2025-01-01 04:01:15', 60, 'normal');",
                "insert into tdb.cw_slide2 values ('2025-01-01 05:00:00', 10, 'normal');",
                "insert into tdb.cw_slide2 values ('2025-01-01 05:00:15', 20, 'normal');",
                "insert into tdb.cw_slide2 values ('2025-01-01 05:00:30', 30, 'normal');",
                "insert into tdb.cw_slide2 values ('2025-01-01 05:00:45', 40, 'normal');",
                "insert into tdb.cw_slide2 values ('2025-01-01 05:01:00', 50, 'normal');",
                "insert into tdb.cw_slide2 values ('2025-01-01 05:01:15', 60, 'normal');",
                "insert into tdb.cw_default_one values ('2025-01-01 06:00:00', 10, 'normal');",
                "insert into tdb.cw_default_one values ('2025-01-01 06:00:15', 20, 'normal');",
                "insert into tdb.cw_default_one values ('2025-01-01 06:00:30', 30, 'normal');",
            ]
        )

    def check_initial_results(self):
        self.check_exact_rows("rdb.r_cw_slide1", "cw1", self.expected_slide1_rows("02"))
        self.check_exact_rows(
            "rdb.r_cw_ignore", "cw_ignore", self.expected_slide1_rows("03")
        )
        self.check_exact_rows(
            "rdb.r_cw_default",
            "cw_default",
            [
                ("2025-01-01 04:00:00", 0, None),
                ("2025-01-01 04:00:45", 0, None),
            ],
        )
        self.check_exact_rows(
            "rdb.r_cw_slide2",
            "cw_slide2",
            [
                ("2025-01-01 05:00:00", 0, None),
                ("2025-01-01 05:00:30", 0, None),
            ],
        )
        self.check_exact_rows(
            "rdb.r_cw_default_one",
            "cw_default_one",
            [
                ("2025-01-01 06:00:00", 0, None),
                ("2025-01-01 06:00:15", 0, None),
                ("2025-01-01 06:00:30", 0, None),
            ],
        )

    def check_disorder_recalc(self):
        before_rows = self.result_rows("rdb.r_cw_slide1", "cw1")
        tdSql.execute("insert into tdb.cw1 values ('2025-01-01 01:59:45', 5, 'late');")
        tdSql.checkResultsByFunc(
            sql=self.result_sql("rdb.r_cw_slide1", "cw1"),
            func=lambda: self.has_recalculated_cw1_rows(before_rows),
            retry=self.RECALC_RETRY,
        )

    def check_ignore_disorder_precedence(self):
        before_rows = self.result_rows("rdb.r_cw_ignore", "cw_ignore")
        tdSql.execute(
            "insert into tdb.cw_ignore values ('2025-01-01 02:59:45', 5, 'late');"
        )
        self.assert_rows_stable(
            "rdb.r_cw_ignore",
            "cw_ignore",
            before_rows,
            "IGNORE_DISORDER should keep count-window results unchanged",
        )

    def check_default_count_window_ignores_disorder(self):
        before_rows = self.result_rows("rdb.r_cw_default", "cw_default")
        tdSql.execute(
            "insert into tdb.cw_default values ('2025-01-01 03:59:45', 5, 'late');"
        )
        self.assert_rows_stable(
            "rdb.r_cw_default",
            "cw_default",
            before_rows,
            "COUNT_WINDOW(3) should ignore disorder data by default",
        )

    def check_default_one_count_window_recalc(self):
        before_rows = self.result_rows("rdb.r_cw_default_one", "cw_default_one")
        tdSql.execute(
            "insert into tdb.cw_default_one values ('2025-01-01 05:59:45', 5, 'late');"
        )
        tdSql.checkResultsByFunc(
            sql=self.result_sql("rdb.r_cw_default_one", "cw_default_one"),
            func=lambda: self.has_recalculated_default_one_rows(before_rows),
            retry=self.RECALC_RETRY,
        )

    def check_slide2_count_window_ignores_disorder(self):
        before_rows = self.result_rows("rdb.r_cw_slide2", "cw_slide2")
        tdSql.execute(
            "insert into tdb.cw_slide2 values ('2025-01-01 04:59:45', 5, 'late');"
        )
        self.assert_rows_stable(
            "rdb.r_cw_slide2",
            "cw_slide2",
            before_rows,
            "COUNT_WINDOW(3,2) should ignore disorder data by default",
        )

    def check_delete_recalc_rejections(self):
        tdSql.error(
            "create stream rdb.s_cw_default_delete count_window(3) "
            "from tdb.cw_slide1 partition by tbname stream_options(delete_recalc) "
            "into rdb.r_cw_default_delete "
            "as select _twstart ts, count(*) cnt from qdb.meters "
            "where cts >= _twstart and cts < _twend;",
            expectedErrno=TSDB_CODE_STREAM_INVALID_TRIGGER,
            expectErrInfo="delete recalc is not supported when count window sliding is not 1",
            fullMatched=False,
        )
        tdSql.execute(
            "create stream rdb.s_cw_default_one_delete count_window(1) "
            "from tdb.cw_slide1 partition by tbname stream_options(delete_recalc) "
            "into rdb.r_cw_default_one_delete "
            "as select _twstart ts, count(*) cnt from qdb.meters "
            "where cts >= _twstart and cts < _twend;"
        )
        tdSql.error(
            "create stream rdb.s_cw_slide2_delete count_window(3, 2) "
            "from tdb.cw_slide1 partition by tbname stream_options(delete_recalc) "
            "into rdb.r_cw_slide2_delete "
            "as select _twstart ts, count(*) cnt from qdb.meters "
            "where cts >= _twstart and cts < _twend;",
            expectedErrno=TSDB_CODE_STREAM_INVALID_TRIGGER,
            expectErrInfo="delete recalc is not supported when count window sliding is not 1",
            fullMatched=False,
        )

    def has_recalculated_cw1_rows(self, before_rows):
        rows = self.current_result_rows()
        return rows != before_rows and rows == [
            ("2025-01-01 01:59:45", 100, 240.0),
            ("2025-01-01 02:00:00", 100, 240.0),
            ("2025-01-01 02:00:15", 100, 241.0),
            ("2025-01-01 02:00:30", 100, 241.0),
        ]

    def has_recalculated_default_one_rows(self, before_rows):
        rows = self.current_result_rows()
        return rows != before_rows and rows == [
            ("2025-01-01 05:59:45", 0, None),
            ("2025-01-01 06:00:00", 0, None),
            ("2025-01-01 06:00:15", 0, None),
            ("2025-01-01 06:00:30", 0, None),
        ]

    def expected_slide1_rows(self, hour):
        base = int(hour) * 120
        return [
            (f"2025-01-01 {hour}:00:00", 100, float(base)),
            (f"2025-01-01 {hour}:00:15", 100, float(base + 1)),
            (f"2025-01-01 {hour}:00:30", 100, float(base + 1)),
        ]

    def check_exact_rows(self, table, tbname, expected_rows):
        tdSql.checkResultsByFunc(
            sql=self.result_sql(table, tbname),
            func=lambda: self.current_result_rows() == expected_rows,
        )

    def assert_rows_stable(self, table, tbname, expected_rows, message):
        for attempt in range(self.STABLE_RETRY):
            after_rows = self.result_rows(table, tbname)
            assert after_rows == expected_rows, (
                f"{message}, attempt={attempt}, "
                f"before={expected_rows}, after={after_rows}"
            )
            time.sleep(1)

    def result_rows(self, table, tbname):
        tdSql.query(self.result_sql(table, tbname))
        return self.current_result_rows()

    def result_sql(self, table, tbname):
        return (
            f"select ts, cnt, avg_val from {table} "
            f"where tag_tbname = '{tbname}' order by ts"
        )

    def current_result_rows(self):
        rows = []
        for i in range(tdSql.getRows()):
            rows.append(
                (
                    self.normalize_timestamp(tdSql.getData(i, 0)),
                    tdSql.getData(i, 1),
                    self.normalize_float(tdSql.getData(i, 2)),
                )
            )
        return rows

    def normalize_timestamp(self, value):
        return str(value).removesuffix(".000")

    def normalize_float(self, value):
        return None if value is None else float(value)
