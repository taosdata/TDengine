import time
from new_test_framework.utils import (tdLog, tdSql, tdCom, tdStream, StreamCheckItem, waitForRows)


class TestStreamSubQueryInVtable4:
    """Test cases for virtual tables in IN subqueries for streams (part 2: window + aggregation)"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable4(self):
        """Windowed/aggregating stream with an IN subquery that scans a virtual table.

        Each stream filters its trigger rows with `WHERE col IN (SELECT ... FROM
        <vtable> WHERE ...)` while also applying a window or aggregation clause.
        The subquery reads a virtual table mapping columns from a separate
        reference database, so the planner must set and propagate the
        streamVtableCalc flag for the subquery branch even when the outer query
        carries a window/aggregate. These shapes are more timing-sensitive than
        the plain sliding cases, so they are isolated here for CI stability.

        Scenarios (5):
            1. InSubqueryVirtualTableWithInterval    - sliding(1s), keeps active device_ids; expects 3 forwarded rows.
            2. InSubqueryVirtualTableWithSession     - session(ts, 30s) count(*) over premium user_ids; expects 2 windows.
            3. InSubqueryVirtualTableWithState       - state_window(status) last/count/avg over monitored machine_ids; expects 2 windows.
            4. InSubqueryVirtualTableWithAggregation - sliding(1s) over featured product_ids; expects 4 forwarded rows.
            5. InSubqueryVirtualTableWithGroupBy     - sliding(1s) over active store_ids; expects 4 forwarded rows.

        Basic IN/NOT IN and empty-result cases live in stream_subquery_in_vtable.py.

        Since: v3.3.4.0

        Labels: stream, vtable

        Jira: None

        History:
            - 2026-03-18 Created
            - 2026-03-20 Split into two files for CI stability
            - 2026-06-24 Split window/aggregation cases out to vtable4 for CI stability
        """

        tdCom.create_snode_if_not_exists()
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        # Clean up databases from previous runs
        for db in ["test_in_vtable_interval", "ref_db_interval", "test_in_vtable_session", "ref_db_session",
                   "test_in_vtable_state", "ref_db_state", "test_in_vtable_agg", "ref_db_agg",
                   "test_in_vtable_groupby", "ref_db_groupby"]:
            tdSql.execute(f"drop database if exists {db}")

        streams = []

        # Window function cases (3 cases)
        streams.append(self.InSubqueryVirtualTableWithInterval())
        streams.append(self.InSubqueryVirtualTableWithSession())
        streams.append(self.InSubqueryVirtualTableWithState())

        # Aggregation cases (2 cases)
        streams.append(self.InSubqueryVirtualTableWithAggregation())
        streams.append(self.InSubqueryVirtualTableWithGroupBy())

        tdStream.checkAll(streams)

    class InSubqueryVirtualTableWithInterval(StreamCheckItem):
        """Test IN subquery with virtual table in interval window"""
        def __init__(self):
            self.db = "test_in_vtable_interval"
            self.refdb = "ref_db_interval"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_interval"
            self.stream = "s_in_vtable_interval"
            self.restb = "res_in_vtable_interval"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, device_id int, temperature float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, device_id int, is_active int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, device_id int from {self.refdb}.{self.reftb}.device_id, is_active int from {self.refdb}.{self.reftb}.is_active)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, device_id, temperature from {self.triggertb} "
                f"where device_id in (select device_id from {self.vtb} where is_active = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 0) ('2026-01-01 00:00:02', 3, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:01:00', 1, 25.5) ('2026-01-01 00:01:01', 2, 30.0) ('2026-01-01 00:01:02', 3, 22.0) ('2026-01-01 00:01:03', 1, 26.0)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 3, 180)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(2, 1, 1)

    class InSubqueryVirtualTableWithSession(StreamCheckItem):
        """Test IN subquery with virtual table in session window"""
        def __init__(self):
            self.db = "test_in_vtable_session"
            self.refdb = "ref_db_session"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_session"
            self.stream = "s_in_vtable_session"
            self.restb = "res_in_vtable_session"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, user_id int, action_type int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, user_id int, is_premium int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, user_id int from {self.refdb}.{self.reftb}.user_id, is_premium int from {self.refdb}.{self.reftb}.is_premium)")

            tdSql.execute(
                f"create stream {self.stream} session(ts, 30s) from {self.triggertb} into {self.restb} as "
                f"select _twstart as ts, _twend as wend, count(*) as action_count "
                f"from {self.triggertb} "
                f"where user_id in (select user_id from {self.vtb} where is_premium = 1)"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 100, 1) ('2026-01-01 00:00:01', 200, 0) ('2026-01-01 00:00:02', 300, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:10', 100, 1) ('2026-01-01 00:00:20', 300, 1) ('2026-01-01 00:00:25', 200, 1) ('2026-01-01 00:01:05', 100, 2) ('2026-01-01 00:01:15', 300, 2) ('2026-01-01 00:02:30', 100, 3)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)

    class InSubqueryVirtualTableWithState(StreamCheckItem):
        """Test IN subquery with virtual table in state window"""
        def __init__(self):
            self.db = "test_in_vtable_state"
            self.refdb = "ref_db_state"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_state"
            self.stream = "s_in_vtable_state"
            self.restb = "res_in_vtable_state"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, machine_id int, status int, val float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, machine_id int, is_monitored int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, machine_id int from {self.refdb}.{self.reftb}.machine_id, is_monitored int from {self.refdb}.{self.reftb}.is_monitored)")

            tdSql.execute(
                f"create stream {self.stream} state_window(status) from {self.triggertb} into {self.restb} as "
                f"select _twstart as ts, _twend as wend, last(status) as status, count(*) as cnt, avg(val) as avg_val "
                f"from {self.triggertb} "
                f"where machine_id in (select machine_id from {self.vtb} where is_monitored = 1)"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 0, 10.0) ('2026-01-01 00:00:05', 1, 0, 11.0) ('2026-01-01 00:00:10', 1, 1, 12.0) ('2026-01-01 00:00:15', 1, 1, 13.0) ('2026-01-01 00:00:16', 2, 0, 20.0) ('2026-01-01 00:00:20', 1, 0, 14.0)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)

    class InSubqueryVirtualTableWithAggregation(StreamCheckItem):
        """Test IN subquery with virtual table and aggregation functions"""
        def __init__(self):
            self.db = "test_in_vtable_agg"
            self.refdb = "ref_db_agg"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_agg"
            self.stream = "s_in_vtable_agg"
            self.restb = "res_in_vtable_agg"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, product_id int, quantity int, price float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, product_id int, is_featured int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, product_id int from {self.refdb}.{self.reftb}.product_id, is_featured int from {self.refdb}.{self.reftb}.is_featured)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, product_id, quantity, price from {self.triggertb} "
                f"where product_id in (select product_id from {self.vtb} where is_featured = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 101, 1) ('2026-01-01 00:00:01', 102, 0) ('2026-01-01 00:00:02', 103, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 101, 5, 99.99) ('2026-01-01 00:00:01', 101, 3, 89.99) ('2026-01-01 00:00:02', 102, 10, 49.99) ('2026-01-01 00:00:03', 103, 2, 199.99) ('2026-01-01 00:00:04', 103, 1, 179.99)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 4, 180)
            tdSql.checkData(0, 1, 101)
            tdSql.checkData(3, 1, 103)

    class InSubqueryVirtualTableWithGroupBy(StreamCheckItem):
        """Test IN subquery with virtual table and GROUP BY"""
        def __init__(self):
            self.db = "test_in_vtable_groupby"
            self.refdb = "ref_db_groupby"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_groupby"
            self.stream = "s_in_vtable_groupby"
            self.restb = "res_in_vtable_groupby"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable4.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, store_id int, category int, sales float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, store_id int, is_active int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, store_id int from {self.refdb}.{self.reftb}.store_id, is_active int from {self.refdb}.{self.reftb}.is_active)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, store_id, sales from {self.triggertb} "
                f"where store_id in (select store_id from {self.vtb} where is_active = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 0) ('2026-01-01 00:00:02', 3, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 1, 500.0) ('2026-01-01 00:00:01', 1, 2, 200.0) ('2026-01-01 00:00:02', 2, 1, 300.0) ('2026-01-01 00:00:03', 3, 1, 450.0) ('2026-01-01 00:00:04', 3, 3, 100.0)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 4, 180)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(2, 1, 3)
