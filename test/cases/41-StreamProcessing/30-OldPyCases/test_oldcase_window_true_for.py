from new_test_framework.utils import tdLog, tdSql, tdStream
import pytest
import time


class TestWindowTrueFor:

    def test_window_true_for(self):
        """OldPy: true for

        tests/system-test/2-query/test_window_true_for.py

        Catalog:
            - Streams:OldPyCases
            
        Since: v3.3.7.0

        Labels: common,ci

        Jira:
            - https://jira.taosdata.com:18080/browse/TD-36887

        History:
            - 2025-07-21: Created by zyyang
        """
        self.db = "test"

        # create snode if not exists
        snodes = tdSql.getResult("SHOW SNODES;")
        if snodes is None or len(snodes) == 0:
            tdStream.createSnode()

        # create database and tables
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS {self.db};",
                f"CREATE DATABASE {self.db} KEEP 36500 PRECISION 'ms';",
                f"USE {self.db};",
                "CREATE STABLE st(ts TIMESTAMP, c1 INT) TAGS (gid INT);",
                "CREATE TABLE ct_0 USING st(gid) TAGS(0);",
                "CREATE TABLE ct_1 USING st(gid) TAGS(1);",
            ],
            queryTimes=1,
        )

        # create stream
        tdSql.executes(
            [
                "CREATE STREAM s_event_1 EVENT_WINDOW(start with c1 > 0 end with c1 < 0) TRUE_FOR(3s)    FROM ct_0 STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_1 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_event_2 EVENT_WINDOW(start with c1 > 0 end with c1 < 0) TRUE_FOR(3s)    FROM ct_0                                 INTO d_event_2 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_event_3 EVENT_WINDOW(start with c1 > 0 end with c1 < 0) TRUE_FOR(2999)  FROM ct_0 STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_3 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_event_4 EVENT_WINDOW(start with c1 > 0 end with c1 < 0) TRUE_FOR(2999)  FROM ct_0                                 INTO d_event_4 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_event_5 EVENT_WINDOW(start with c1 > 0 end with c1 < 0) TRUE_FOR(3001a) FROM ct_0 STREAM_OPTIONS(IGNORE_DISORDER) INTO d_event_5 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_event_6 EVENT_WINDOW(start with c1 > 0 end with c1 < 0) TRUE_FOR(3001a) FROM ct_0                                 INTO d_event_6 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_state_1 STATE_WINDOW(c1) TRUE_FOR(3s)    FROM ct_1 STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_1 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_state_2 STATE_WINDOW(c1) TRUE_FOR(3s)    FROM ct_1                                 INTO d_state_2 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_state_3 STATE_WINDOW(c1) TRUE_FOR(2999)  FROM ct_1 STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_3 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_state_4 STATE_WINDOW(c1) TRUE_FOR(2999)  FROM ct_1                                 INTO d_state_4 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_state_5 STATE_WINDOW(c1) TRUE_FOR(3001a) FROM ct_1 STREAM_OPTIONS(IGNORE_DISORDER) INTO d_state_5 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
                "CREATE STREAM s_state_6 STATE_WINDOW(c1) TRUE_FOR(3001a) FROM ct_1                                 INTO d_state_6 AS SELECT _twstart, _twend, count(*) FROM %%trows;",
            ],
            queryTimes=1,
        )

        tdStream.checkStreamStatus()

        # insert data
        tdSql.executes(
            [
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:00.000', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:01.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:02.000', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:03.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:04.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:05.000', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:06.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:07.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:08.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:08.999', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:10.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:11.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:12.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:13.000', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:14.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:15.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:16.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:17.001', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:18.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:19.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:20.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:21.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:22.000', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:23.000', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:24.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:25.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:26.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:27.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:28.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:29.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:30.000', -1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:31.000', 0);",
            ],
            queryTimes=1,
        )
        tdSql.executes(
            [
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:00.000', 0);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:01.000', 1);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:02.000', 1);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:03.000', 2);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:04.000', 2);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:05.000', 2);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:06.000', 3);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:07.000', 3);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:08.000', 3);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:08.999', 3);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:10.000', 4);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:11.000', 4);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:12.000', 4);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:13.000', 4);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:14.000', 5);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:15.000', 5);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:16.000', 5);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:17.001', 5);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:18.000', 6);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:19.000', 6);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:20.000', 6);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:21.000', 6);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:22.000', 6);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:23.000', 0);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:24.000', 7);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:25.000', 7);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:26.000', 7);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:27.000', 7);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:28.000', 7);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:29.000', 7);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:30.000', 7);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:31.000', 0);",
            ],
            queryTimes=1,
        )

        time.sleep(10)

        # update data
        tdSql.executes(
            [
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:00.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:22.000', 1);",
                "INSERT INTO ct_0 VALUES('2025-01-01 00:00:28.000', -1);",
            ],
            queryTimes=1,
        )
        tdSql.executes(
            [
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:00.000', 1);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:23.000', 6);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:29.000', 8);",
                "INSERT INTO ct_1 VALUES('2025-01-01 00:00:30.000', 8);",
            ],
            queryTimes=1,
        )

        # check results
        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_event_1;",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:22.000")
            and tdSql.compareData(2, 2, 5)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:30.000")
            and tdSql.compareData(3, 2, 7),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_event_2;",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(2, 2, 6)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(3, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3s);",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(2, 2, 6)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(3, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_event_3;",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:08.999")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(2, 2, 4)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:22.000")
            and tdSql.compareData(3, 2, 5)
            and tdSql.compareData(4, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(4, 1, "2025-01-01 00:00:30.000")
            and tdSql.compareData(4, 2, 7),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_event_4;",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:08.999")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(2, 2, 4)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(3, 2, 6)
            and tdSql.compareData(4, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(4, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(4, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(2999);",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:08.999")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(2, 2, 4)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(3, 2, 6)
            and tdSql.compareData(4, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(4, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(4, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_event_5;",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:22.000")
            and tdSql.compareData(1, 2, 5)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:30.000")
            and tdSql.compareData(2, 2, 7),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_event_6;",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(1, 2, 6)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(2, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3001a);",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(1, 2, 6)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(2, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_state_1;",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:22.000")
            and tdSql.compareData(2, 2, 5)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:30.000")
            and tdSql.compareData(3, 2, 7),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_state_2;",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(2, 2, 6)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(3, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for(3s);",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(2, 2, 6)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(3, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_state_3;",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:08.999")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(2, 2, 4)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:22.000")
            and tdSql.compareData(3, 2, 5)
            and tdSql.compareData(4, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(4, 1, "2025-01-01 00:00:30.000")
            and tdSql.compareData(4, 2, 7),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_state_4;",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:08.999")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(2, 2, 4)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(3, 2, 6)
            and tdSql.compareData(4, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(4, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(4, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for(2999);",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2025-01-01 00:00:06.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:08.999")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:10.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:13.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(2, 2, 4)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(3, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(3, 2, 6)
            and tdSql.compareData(4, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(4, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(4, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_state_5;",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:22.000")
            and tdSql.compareData(1, 2, 5)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:30.000")
            and tdSql.compareData(2, 2, 7),
        )

        tdSql.checkResultsByFunc(
            sql="SELECT * FROM d_state_6;",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(1, 2, 6)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(2, 2, 5),
        )

        tdSql.checkResultsByFunc(
            sql="select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for(3001a);",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:14.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:17.001")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:18.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:23.000")
            and tdSql.compareData(1, 2, 6)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:24.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:28.000")
            and tdSql.compareData(2, 2, 5),
        )

        # test abnormal
        tdSql.error(
            "select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3n);"
        )
        tdSql.error(
            "select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3y);"
        )
        tdSql.error(
            "select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1);"
        )
        tdSql.error(
            "select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1a);"
        )
        tdSql.error(
            "select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('-1a');"
        )
        tdSql.error(
            "create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3n);"
        )
        tdSql.error(
            "create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3y);"
        )
        tdSql.error(
            "create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1);"
        )
        tdSql.error(
            "create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1a);"
        )
        tdSql.error(
            "create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('-1a');"
        )

        tdLog.info("TestWindowTrueFor done")
