import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestWindowTrueFor:
    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.db = "test_true_for"
        self.precision = "ms"
        self.event_table_name = "event_true_for_tbl"
        self.state_table_name = "state_true_for_tbl"

    def test_true_for(self):
        """Verify the functionality of TRUE_FOR expr.

        This case validates the `TRUE_FOR` expr across various scenarios. It
        covers application in both queries and stream computing, specifically
        focusing on `EVENT_WINDOW` and `STATE_WINDOW`. The case includes all
        four modes of `TRUE_FOR`:
          - `TRUE_FOR(duration_time)`
          - `TRUE_FOR(COUNT n)`
          - `TRUE_FOR(duration_time AND COUNT n)`
          - `TRUE_FOR(duration_time OR COUNT n)`
        It ensures correct behavior under normal conditions, edge cases, and
        error scenarios.

        Since: v3.4.0.0

        Catalog:
            - Streams:TestTrueFor

        Labels: common,ci,stream,true_for,state_window,event_window

        JIRA: FEAT-589462593

        History:
            - 2026-01-14: Created for TRUE_FOR enhancement by Jinqing Kuang
        """

        tdStream.createSnode()

        sqls = [
            f"DROP DATABASE IF EXISTS {self.db};",
            f"CREATE DATABASE {self.db} VGROUPS 1 BUFFER 8 PRECISION '{self.precision}';",
            f"USE {self.db};",
            f"CREATE TABLE {self.event_table_name} (ts TIMESTAMP, c1 INT);",
            f"CREATE TABLE {self.state_table_name} (ts TIMESTAMP, c1 INT);",
        ]
        tdSql.executes(sqls)

        streams = []
        # streams.append(self.EventTrueForDuration(self.db, self.event_table_name))
        # streams.append(self.EventTrueForCount(self.db, self.event_table_name))
        # streams.append(self.EventTrueForAnd(self.db, self.event_table_name))
        # streams.append(self.EventTrueForOr(self.db, self.event_table_name))
        streams.append(self.StateTrueForDuration(self.db, self.state_table_name))
        streams.append(self.StateTrueForCount(self.db, self.state_table_name))
        streams.append(self.StateTrueForAnd(self.db, self.state_table_name))
        streams.append(self.StateTrueForOr(self.db, self.state_table_name))

        tdStream.checkAll(streams)

    class EventTrueForDuration(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM event_true_for_duration_1 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_duration_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_duration_2 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s) FROM {self.table}                                 INTO d_event_true_for_duration_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_duration_3 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(2999) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_duration_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_duration_4 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(2999) FROM {self.table}                                 INTO d_event_true_for_duration_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_duration_5 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3001a) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_duration_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_duration_6 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3001a) FROM {self.table}                                 INTO d_event_true_for_duration_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_duration_7 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_event_true_for_duration_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM event_true_for_duration_8 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_event_true_for_duration_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def insert1(self):
            sqls = [
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:00.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:01.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:02.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:03.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:04.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:05.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:06.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:08.999', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:10.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:11.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:12.999', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:14.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:15.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:15.999', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:16.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:17.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:20.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:21.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:22.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:23.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:24.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:25.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:26.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:27.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:27.999', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:28.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:29.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:30.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:31.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:32.001', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:33.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:34.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:35.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:36.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:36.001', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:37.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:41.000', -1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:42.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:43.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:44.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:47.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:48.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:50.000', -1);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with duration_time
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_1;",
                exp_result=[
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_duration_2;",
                exp_sql="SELECT * FROM d_event_true_for_duration_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_3;",
                exp_result=[
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_duration_4;",
                exp_sql="SELECT * FROM d_event_true_for_duration_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_5;",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_duration_6;",
                exp_sql="SELECT * FROM d_event_true_for_duration_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_7;",
                exp_result=[
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_duration_8;",
                exp_sql="SELECT * FROM d_event_true_for_duration_7;",
            )

            # verify results of queries using TRUE_FOR with duration time
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3s);",
                exp_result=[
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(2999);",
                exp_result=[
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3001a);",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def insert2(self):
            # update data
            sqls = [
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:00.000', 1);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:01.000', -1);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:02.000', 1);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:40.000', 1);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:47.000', -1);",
            ]
            tdSql.executes(sqls)

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_6;",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_duration_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3s);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(2999);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3001a);",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

    class EventTrueForCount(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM event_true_for_count_1 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 4) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_count_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_count_2 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 4) FROM {self.table}                                 INTO d_event_true_for_count_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_count_3 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 3) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_count_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_count_4 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 3) FROM {self.table}                                 INTO d_event_true_for_count_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_count_5 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 5) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_count_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_count_6 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 5) FROM {self.table}                                 INTO d_event_true_for_count_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_count_7 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_event_true_for_count_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM event_true_for_count_8 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_event_true_for_count_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with count
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_1;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_count_2;",
                exp_sql="SELECT * FROM d_event_true_for_count_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_3;",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_count_4;",
                exp_sql="SELECT * FROM d_event_true_for_count_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_5;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_count_6;",
                exp_sql="SELECT * FROM d_event_true_for_count_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_7;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_count_8;",
                exp_sql="SELECT * FROM d_event_true_for_count_7;",
            )

            # verify results of queries using TRUE_FOR with count
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_6;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_count_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                ],
            )

    class EventTrueForAnd(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM event_true_for_and_1 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s AND COUNT 4) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_and_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_and_2 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s AND COUNT 4) FROM {self.table}                                 INTO d_event_true_for_and_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_and_3 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(2999 AND COUNT 3) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_and_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_and_4 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(2999 AND COUNT 3) FROM {self.table}                                 INTO d_event_true_for_and_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_and_5 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3001a AND COUNT 5) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_and_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_and_6 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3001a AND COUNT 5) FROM {self.table}                                 INTO d_event_true_for_and_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_and_7 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s AND COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_event_true_for_and_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM event_true_for_and_8 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s AND COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_event_true_for_and_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with AND
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_1;",
                exp_result=[
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_and_2;",
                exp_sql="SELECT * FROM d_event_true_for_and_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_3;",
                exp_result=[
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_and_4;",
                exp_sql="SELECT * FROM d_event_true_for_and_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_5;",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_and_6;",
                exp_sql="SELECT * FROM d_event_true_for_and_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_7;",
                exp_result=[
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_and_8;",
                exp_sql="SELECT * FROM d_event_true_for_and_7;",
            )

            # verify results of queries using TRUE_FOR with AND
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3s AND COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(2999 AND COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3001a AND COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_6;",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_and_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3s AND COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(2999 AND COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3001a AND COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                ],
            )

    class EventTrueForOr(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM event_true_for_or_1 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s OR COUNT 4) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_or_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_or_2 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s OR COUNT 4) FROM {self.table}                                 INTO d_event_true_for_or_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_or_3 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(2999 OR COUNT 3) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_or_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_or_4 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(2999 OR COUNT 3) FROM {self.table}                                 INTO d_event_true_for_or_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_or_5 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3001a OR COUNT 5) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_true_for_or_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_or_6 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3001a OR COUNT 5) FROM {self.table}                                 INTO d_event_true_for_or_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM event_true_for_or_7 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s OR COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_event_true_for_or_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM event_true_for_or_8 EVENT_WINDOW(START WITH c1 > 0 END WITH C1 < 0) TRUE_FOR(3s OR COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_event_true_for_or_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with OR
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_1;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_or_2;",
                exp_sql="SELECT * FROM d_event_true_for_or_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_3;",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_or_4;",
                exp_sql="SELECT * FROM d_event_true_for_or_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_5;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_or_6;",
                exp_sql="SELECT * FROM d_event_true_for_or_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_7;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_event_true_for_or_8;",
                exp_sql="SELECT * FROM d_event_true_for_or_7;",
            )

            # verify results of queries using TRUE_FOR with OR
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3s OR COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(2999 OR COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3001a OR COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_6;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_event_true_for_or_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3s OR COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(2999 OR COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} EVENT_WINDOW START WITH c1 > 0 END WITH c1 < 0 TRUE_FOR(3001a OR COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

    class StateTrueForDuration(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM state_true_for_duration_1 STATE_WINDOW(c1) TRUE_FOR(3s) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_duration_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_duration_2 STATE_WINDOW(c1) TRUE_FOR(3s) FROM {self.table}                                 INTO d_state_true_for_duration_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_duration_3 STATE_WINDOW(c1) TRUE_FOR(2999) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_duration_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_duration_4 STATE_WINDOW(c1) TRUE_FOR(2999) FROM {self.table}                                 INTO d_state_true_for_duration_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_duration_5 STATE_WINDOW(c1) TRUE_FOR(3001a) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_duration_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_duration_6 STATE_WINDOW(c1) TRUE_FOR(3001a) FROM {self.table}                                 INTO d_state_true_for_duration_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_duration_7 STATE_WINDOW(c1) TRUE_FOR(3s) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_state_true_for_duration_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM state_true_for_duration_8 STATE_WINDOW(c1) TRUE_FOR(3s) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_state_true_for_duration_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def insert1(self):
            sqls = [
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:00.000', 1);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:01.000', 2);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:02.000', 2);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:03.000', 3);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:04.000', 3);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:05.000', 3);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:06.000', 4);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:08.999', 4);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:10.000', 5);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:11.000', 5);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:12.999', 5);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:14.000', 6);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:15.000', 6);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:15.999', 6);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:16.000', 6);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:17.000', 7);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:20.000', 7);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:21.000', 8);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:22.000', 8);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:23.000', 8);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:24.000', 8);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:25.000', 9);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:26.000', 9);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:27.000', 9);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:27.999', 9);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:28.000', 9);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:29.000', 10);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:30.000', 10);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:31.000', 10);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:32.001', 10);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:33.000', 11);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:34.000', 11);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:35.000', 11);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:36.000', 11);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:36.001', 11);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:37.000', 12);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:41.000', 12);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:42.000', 13);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:43.000', 13);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:44.000', 13);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:47.000', 13);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:48.000', 13);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:50.000', 13);",
                f"INSERT INTO {self.table} VALUES ('2025-01-01 00:00:51.000', -1);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with duration_time
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_1;",
                exp_result=[
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_duration_2;",
                exp_sql="SELECT * FROM d_state_true_for_duration_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_3;",
                exp_result=[
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_duration_4;",
                exp_sql="SELECT * FROM d_state_true_for_duration_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_5;",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_duration_6;",
                exp_sql="SELECT * FROM d_state_true_for_duration_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_7;",
                exp_result=[
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_duration_8;",
                exp_sql="SELECT * FROM d_state_true_for_duration_7;",
            )

            # verify results of queries using TRUE_FOR with duration time
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3s);",
                exp_result=[
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(2999);",
                exp_result=[
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3001a);",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def insert2(self):
            # update data
            sqls = [
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:01.000', 1);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:02.000', 3);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:40.000', 12);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:48.000', 14);",
                f"INSERT INTO {self.table} VALUES('2025-01-01 00:00:50.000', 14);",
            ]
            tdSql.executes(sqls)

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_6;",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_duration_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3s);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(2999);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3001a);",
                exp_result=[
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

    class StateTrueForCount(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM state_true_for_count_1 STATE_WINDOW(c1) TRUE_FOR(COUNT 4) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_count_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_count_2 STATE_WINDOW(c1) TRUE_FOR(COUNT 4) FROM {self.table}                                 INTO d_state_true_for_count_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_count_3 STATE_WINDOW(c1) TRUE_FOR(COUNT 3) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_count_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_count_4 STATE_WINDOW(c1) TRUE_FOR(COUNT 3) FROM {self.table}                                 INTO d_state_true_for_count_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_count_5 STATE_WINDOW(c1) TRUE_FOR(COUNT 5) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_count_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_count_6 STATE_WINDOW(c1) TRUE_FOR(COUNT 5) FROM {self.table}                                 INTO d_state_true_for_count_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_count_7 STATE_WINDOW(c1) TRUE_FOR(COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_state_true_for_count_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM state_true_for_count_8 STATE_WINDOW(c1) TRUE_FOR(COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_state_true_for_count_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with count
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_1;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_count_2;",
                exp_sql="SELECT * FROM d_state_true_for_count_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_3;",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_count_4;",
                exp_sql="SELECT * FROM d_state_true_for_count_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_5;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_count_6;",
                exp_sql="SELECT * FROM d_state_true_for_count_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_7;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_count_8;",
                exp_sql="SELECT * FROM d_state_true_for_count_7;",
            )

            # verify results of queries using TRUE_FOR with count
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_6;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_count_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                ],
            )

    class StateTrueForAnd(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM state_true_for_and_1 STATE_WINDOW(c1) TRUE_FOR(3s AND COUNT 4) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_and_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_and_2 STATE_WINDOW(c1) TRUE_FOR(3s AND COUNT 4) FROM {self.table}                                 INTO d_state_true_for_and_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_and_3 STATE_WINDOW(c1) TRUE_FOR(2999 AND COUNT 3) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_and_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_and_4 STATE_WINDOW(c1) TRUE_FOR(2999 AND COUNT 3) FROM {self.table}                                 INTO d_state_true_for_and_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_and_5 STATE_WINDOW(c1) TRUE_FOR(3001a AND COUNT 5) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_and_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_and_6 STATE_WINDOW(c1) TRUE_FOR(3001a AND COUNT 5) FROM {self.table}                                 INTO d_state_true_for_and_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_and_7 STATE_WINDOW(c1) TRUE_FOR(3s AND COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_state_true_for_and_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM state_true_for_and_8 STATE_WINDOW(c1) TRUE_FOR(3s AND COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_state_true_for_and_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with AND
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_1;",
                exp_result=[
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_and_2;",
                exp_sql="SELECT * FROM d_state_true_for_and_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_3;",
                exp_result=[
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_and_4;",
                exp_sql="SELECT * FROM d_state_true_for_and_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_5;",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_and_6;",
                exp_sql="SELECT * FROM d_state_true_for_and_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_7;",
                exp_result=[
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_and_8;",
                exp_sql="SELECT * FROM d_state_true_for_and_7;",
            )

            # verify results of queries using TRUE_FOR with AND
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3s AND COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(2999 AND COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3001a AND COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_6;",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_and_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3s AND COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(2999 AND COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3001a AND COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                ],
            )

    class StateTrueForOr(StreamCheckItem):
        def __init__(self, db, tbl):
            self.db = db
            self.table = tbl

        def create(self):
            sqls = [
                f"CREATE STREAM state_true_for_or_1 STATE_WINDOW(c1) TRUE_FOR(3s OR COUNT 4) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_or_1 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_or_2 STATE_WINDOW(c1) TRUE_FOR(3s OR COUNT 4) FROM {self.table}                                 INTO d_state_true_for_or_2 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_or_3 STATE_WINDOW(c1) TRUE_FOR(2999 OR COUNT 3) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_or_3 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_or_4 STATE_WINDOW(c1) TRUE_FOR(2999 OR COUNT 3) FROM {self.table}                                 INTO d_state_true_for_or_4 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_or_5 STATE_WINDOW(c1) TRUE_FOR(3001a OR COUNT 5) FROM {self.table} STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_true_for_or_5 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_or_6 STATE_WINDOW(c1) TRUE_FOR(3001a OR COUNT 5) FROM {self.table}                                 INTO d_state_true_for_or_6 AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
                f"CREATE STREAM state_true_for_or_7 STATE_WINDOW(c1) TRUE_FOR(3s OR COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN)|IGNORE_DISORDER) INTO d_state_true_for_or_7 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
                f"CREATE STREAM state_true_for_or_8 STATE_WINDOW(c1) TRUE_FOR(3s OR COUNT 4) FROM {self.table} STREAM_OPTIONS(EVENT_TYPE(WINDOW_OPEN))                 INTO d_state_true_for_or_8 AS SELECT _twstart, _twend, COUNT(*) FROM {self.table} WHERE _c0 = _twstart;",
            ]
            tdSql.executes(sqls)

        def check1(self):
            # verify results of streams using TRUE_FOR with OR
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_1;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_or_2;",
                exp_sql="SELECT * FROM d_state_true_for_or_1;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_3;",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_or_4;",
                exp_sql="SELECT * FROM d_state_true_for_or_3;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_5;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_or_6;",
                exp_sql="SELECT * FROM d_state_true_for_or_5;",
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_7;",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            tdSql.checkResultsBySql(
                sql="SELECT * FROM d_state_true_for_or_8;",
                exp_sql="SELECT * FROM d_state_true_for_or_7;",
            )

            # verify results of queries using TRUE_FOR with OR
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3s OR COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(2999 OR COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3001a OR COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 2],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:50.000", 6],
                ],
            )

        def check2(self):
            # verify stream results after data update
            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_2;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_4;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:03.000", "2025-01-01 00:00:05.000", 3],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_6;",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql="SELECT * FROM d_state_true_for_or_8;",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:02.000", 1],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:14.000", 1],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:17.000", 1],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:21.000", 1],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:25.000", 1],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:29.000", 1],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:33.000", 1],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:37.000", 1],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:42.000", 1],
                ],
            )

            # verify query results after data update
            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3s OR COUNT 4);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(2999 OR COUNT 3);",
                exp_result=[
                    ["2025-01-01 00:00:02.000", "2025-01-01 00:00:05.000", 4],
                    ["2025-01-01 00:00:06.000", "2025-01-01 00:00:08.999", 2],
                    ["2025-01-01 00:00:10.000", "2025-01-01 00:00:12.999", 3],
                    ["2025-01-01 00:00:14.000", "2025-01-01 00:00:16.000", 4],
                    ["2025-01-01 00:00:17.000", "2025-01-01 00:00:20.000", 2],
                    ["2025-01-01 00:00:21.000", "2025-01-01 00:00:24.000", 4],
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )

            tdSql.checkResultsByArray(
                sql=f"SELECT _wstart, _wend, COUNT(*) FROM {self.table} STATE_WINDOW(c1) TRUE_FOR(3001a OR COUNT 5);",
                exp_result=[
                    ["2025-01-01 00:00:25.000", "2025-01-01 00:00:28.000", 5],
                    ["2025-01-01 00:00:29.000", "2025-01-01 00:00:32.001", 4],
                    ["2025-01-01 00:00:33.000", "2025-01-01 00:00:36.001", 5],
                    ["2025-01-01 00:00:37.000", "2025-01-01 00:00:41.000", 3],
                    ["2025-01-01 00:00:42.000", "2025-01-01 00:00:47.000", 4],
                ],
            )
