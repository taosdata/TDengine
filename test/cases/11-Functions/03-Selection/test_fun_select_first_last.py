from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from datetime import datetime, timedelta

class TestFunSelectFirstLast:
    def test_last_interval_partition_many_generated_windows(self):
        """Last with many generated partitioned interval windows

        1. Generate many partitioned 10-second interval windows without external data files.
        2. Query last(datavalue) with _wstart and partition key in an ordered subquery.
        3. Verify the subquery can be consumed by an outer count query.

        Catalog:
            - Function:Selection

        Since: v3.4.1.13

        Labels: common,ci,last,interval window,partition by

        Jira: TS-7474

        History:
            - 2026-07-07 Ren Xin created

        """
        self.do_last_interval_partition_many_generated_windows()

    def do_last_interval_partition_many_generated_windows(self):
        db = "test_last_interval_partition_many_generated_windows"
        table_count = 8
        rows_per_table = 5875
        batch_rows = 500
        start_ts = datetime(2026, 6, 2, 0, 0, 0)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 cachemodel 'none' keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table st (ts timestamp, dataquality int, datavalue float) tags (dataname nchar(64))")

        for table_idx in range(table_count):
            tdSql.execute(f"create table t{table_idx} using st tags ('g{table_idx}')")
            for batch_start in range(0, rows_per_table, batch_rows):
                values = []
                batch_end = min(batch_start + batch_rows, rows_per_table)
                for row_idx in range(batch_start, batch_end):
                    ts = start_ts + timedelta(seconds=row_idx * 10)
                    value = row_idx + table_idx / 100
                    values.append(f"('{ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', 1, {value})")
                tdSql.execute(f"insert into t{table_idx} values {' '.join(values)}")

        tdSql.execute(f"flush database {db}")

        end_ts = start_ts + timedelta(seconds=rows_per_table * 10)
        names = ", ".join([f"'g{idx}'" for idx in range(table_count)])
        inner_sql = f"""select _wstart datatime, dataname, last(datavalue) datavalue
                        from st
                        where ts >= '{start_ts.strftime('%Y-%m-%d %H:%M:%S')}'
                          and ts < '{end_ts.strftime('%Y-%m-%d %H:%M:%S')}'
                          and dataquality != '0'
                          and dataname in ({names})
                        partition by dataname
                        interval(10s)
                        order by ts"""
        tdSql.query(f"select count(*) from ({inner_sql})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, table_count * rows_per_table)

        print("last interval partition many generated windows ........ [ passed ]")

    def test_last_interval_partition_empty_result_window(self):
        """Last with partitioned interval windows and sparse valid values

        1. Create partitioned interval windows with mixed NULL and filtered rows.
        2. Query last(datavalue) with _wstart and partition key in a subquery.
        3. Pivot the subquery result by the partition key.
        4. Verify sparse windows return stable rows and values.

        Catalog:
            - Function:Selection

        Since: v3.4.1.13

        Labels: common,ci,last,interval window,partition by

        Jira: TS-7474

        History:
            - 2026-07-07 Ren Xin created

        """
        self.do_last_interval_partition_empty_result_window()

    def do_last_interval_partition_empty_result_window(self):
        tdSql.execute("drop database if exists test_last_interval_partition_empty_result")
        tdSql.execute("create database test_last_interval_partition_empty_result vgroups 4 cachemodel 'both' keep 36500")
        tdSql.execute("use test_last_interval_partition_empty_result")
        tdSql.execute("create table st (ts timestamp, dataname varchar(32), datavalue double, dataquality varchar(8)) tags(tid int)")
        for tid in range(64):
            tdSql.execute(f"create table t{tid} using st tags({tid})")

        table_rows = {}
        for tid in range(64):
            table = f"t{tid}"
            shard = tid % 16
            values = []
            for group_idx in range(4):
                group = f"g{group_idx:02d}"
                value_base = group_idx * 100
                ms = group_idx * 10 + shard
                if group_idx in (0, 1):
                    values.append(f"('2026-06-02 00:00:00.{ms:03d}', '{group}', NULL, '1')")
                if tid % 4 == group_idx:
                    values.append(f"('2026-06-02 00:00:10.{ms:03d}', '{group}', NULL, '1')")
                    values.append(f"('2026-06-02 00:00:11.{ms:03d}', '{group}', {value_base + 1 + shard / 100}, '1')")
                else:
                    values.append(f"('2026-06-02 00:00:10.{ms:03d}', '{group}', NULL, '1')")
                values.append(f"('2026-06-02 00:00:20.{ms:03d}', '{group}', {value_base + 2 + shard / 100}, '0')")
                if group_idx in (0, 2):
                    if tid % 4 == group_idx:
                        values.append(f"('2026-06-02 00:00:30.{ms:03d}', '{group}', {value_base + 3 + shard / 100}, '1')")
                    else:
                        values.append(f"('2026-06-02 00:00:30.{ms:03d}', '{group}', NULL, '1')")
                if group_idx in (1, 3):
                    values.append(f"('2026-06-02 00:00:40.{ms:03d}', '{group}', NULL, '1')")
                    values.append(f"('2026-06-02 00:00:50.{ms:03d}', '{group}', NULL, '1')")
                    if tid % 4 == group_idx:
                        values.append(f"('2026-06-02 00:00:51.{ms:03d}', '{group}', {value_base + 5 + shard / 100}, '1')")
                values.append(f"('2026-06-02 00:01:00.{ms:03d}', '{group}', {value_base + 6 + shard / 100}, '0')")
                if tid % 4 == group_idx:
                    values.append(f"('2026-06-02 00:01:10.{ms:03d}', '{group}', {value_base + 7 + shard / 100}, '1')")
            table_rows[table] = values

        tdSql.execute("insert into " + " ".join([f"{table} values {(' '.join(values))}" for table, values in table_rows.items()]))
        tdSql.execute("flush database test_last_interval_partition_empty_result")

        inner_sql = """select _wstart datatime, dataname, last(datavalue) datavalue
                      from st
                      where ts between '2026-06-02 00:00:00' and '2026-06-02 00:01:20'
                        and dataquality != '0'
                        and dataname in ('g00', 'g01', 'g02', 'g03')
                      partition by dataname
                      interval(10s)
                      order by 1"""
        pivot_sql = f"""select datatime,
                               max(case when dataname = 'g00' then datavalue else NULL end) as g00,
                               max(case when dataname = 'g01' then datavalue else NULL end) as g01,
                               max(case when dataname = 'g02' then datavalue else NULL end) as g02,
                               max(case when dataname = 'g03' then datavalue else NULL end) as g03
                        from ({inner_sql})
                        group by datatime
                        order by datatime"""
        expected = [
            ("2026-06-02 00:00:00.000", None, None, None, None),
            ("2026-06-02 00:00:10.000", 1.12, 101.13, 201.14, 301.15),
            ("2026-06-02 00:00:30.000", 3.12, None, 203.14, None),
            ("2026-06-02 00:00:40.000", None, None, None, None),
            ("2026-06-02 00:00:50.000", None, 105.13, None, 305.15),
            ("2026-06-02 00:01:10.000", 7.12, 107.13, 207.14, 307.15),
        ]

        for _ in range(20):
            tdSql.query(pivot_sql)
            tdSql.checkRows(6)
            for row, values in enumerate(expected):
                for col, value in enumerate(values):
                    tdSql.checkData(row, col, value)

        print("last interval partition empty result window ............ [ passed ]")

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

        Labels: ci,first,interval window,last,integration,functional
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
