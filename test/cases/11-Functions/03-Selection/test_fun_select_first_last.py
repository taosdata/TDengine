from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestFunSelectFirstLast:
    def test_first_last_window(self):
        """First Last with All Windows

        1. select list only contains first, last and
            _select_value functions with **INTERVAL** window
        2. select list only contains first, last and
            _select_value functions with **STATE** window
        3. select list only contains first, last and
            _select_value functions with **SESSION** window
        4. select list only contains first, last and
            _select_value functions with **EVENT** window
        5. select list only contains first, last and
            _select_value functions with **COUNT** window

        Catalog:
            - Function:Selection

        Since: v3.3.6.0

        Labels: first, last, interval window, ci

        Jira: TS-7474

        History:
            - 2025-10-22 Tony Zhang created

        """
        tdSql.execute("create database if not exists test_first_last_interval_window cachemodel 'both' keep 36500", show=True)
        tdSql.execute("use test_first_last_interval_window")
        tdSql.execute("create table tt (ts timestamp, v int)", show=True)
        tdSql.execute('''insert into tt values
                        ("2025-10-10 12:00:00", 1),
                        ("2025-10-10 12:00:10", 1),
                        ("2025-10-10 12:00:44", 1),
                        ("2025-10-10 12:01:10", 2), 
                        ("2025-10-10 12:01:33", 2), 
                        ("2025-10-10 12:02:10", 3), 
                        ("2025-10-10 12:02:55", 3)''', show=True)

        tdSql.query(f"select cols(first(ts), ts as first_ts, v as first_v), cols(last(ts), ts as last_ts, v as last_v) from tt INTERVAL(1m)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-10 12:00:00.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2025-10-10 12:00:44.000")
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, "2025-10-10 12:01:10.000")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, "2025-10-10 12:01:33.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(2, 0, "2025-10-10 12:02:10.000")
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, "2025-10-10 12:02:55.000")
        tdSql.checkData(2, 3, 3)

        tdSql.query(f"select cols(first(ts), ts as first_ts, v as first_v), cols(last(ts), ts as last_ts, v as last_v) from tt STATE_WINDOW(v);", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2025-10-10 12:00:00.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2025-10-10 12:00:44.000")
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, "2025-10-10 12:01:10.000")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, "2025-10-10 12:01:33.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(2, 0, "2025-10-10 12:02:10.000")
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, "2025-10-10 12:02:55.000")
        tdSql.checkData(2, 3, 3)

        tdSql.query(f"select cols(first(ts), ts as first_ts, v as first_v), cols(last(ts), ts as last_ts, v as last_v) from tt SESSION(ts, 30s);", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-10-10 12:00:00.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2025-10-10 12:00:10.000")
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, "2025-10-10 12:00:44.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "2025-10-10 12:01:33.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(2, 0, "2025-10-10 12:02:10.000")
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, "2025-10-10 12:02:10.000")
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(3, 0, "2025-10-10 12:02:55.000")
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, "2025-10-10 12:02:55.000")
        tdSql.checkData(3, 3, 3)
        
        tdSql.query(f"select cols(first(ts), ts as first_ts, v as first_v), cols(last(ts), ts as last_ts, v as last_v) from tt EVENT_WINDOW start with v <= 1 end with v > 1;", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2025-10-10 12:00:00.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2025-10-10 12:01:10.000")
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select cols(first(ts), ts as first_ts, v as first_v), cols(last(ts), ts as last_ts, v as last_v) from tt COUNT_WINDOW(2);", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-10-10 12:00:00.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2025-10-10 12:00:10.000")
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, "2025-10-10 12:00:44.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "2025-10-10 12:01:10.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(2, 0, "2025-10-10 12:01:33.000")
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, "2025-10-10 12:02:10.000")
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(3, 0, "2025-10-10 12:02:55.000")
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, "2025-10-10 12:02:55.000")
        tdSql.checkData(3, 3, 3)
