import time
from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem)


WAIT_TIMEOUT = 60

def wait_for_rows(sql, expected_rows, timeout=WAIT_TIMEOUT):
    """Poll until the query returns the expected number of rows, or timeout."""
    for i in range(timeout):
        tdSql.query(sql)
        if tdSql.queryRows == expected_rows:
            return
        time.sleep(1)
    # Final call — let checkRows raise with a proper error message
    tdSql.checkRows(expected_rows)


class TestStreamSubQueryInVtable:
    """Test cases for virtual tables in IN subqueries for streams (part 1: basic + window + aggregation)"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable(self):
        """Test virtual tables in IN subqueries - basic, window and aggregation cases

        Test that streams can use virtual tables in IN subqueries correctly.
        This ensures the streamVtableCalc flag is properly set and propagated.

        Since: v3.3.4.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-03-18 Created
            - 2026-03-20 Split into two files for CI stability
        """

        tdStream.createSnode()
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        streams = []

        # Basic IN/NOT IN cases (5 cases)
        streams.append(self.InSubqueryVirtualNormalTable())
        streams.append(self.InSubqueryVirtualChildTable())
        streams.append(self.InSubqueryVirtualSuperTable())
        streams.append(self.NotInSubqueryVirtualTable())
        streams.append(self.InSubqueryMultipleVirtualTables())

        # Window function cases (3 cases)
        streams.append(self.InSubqueryVirtualTableWithInterval())
        streams.append(self.InSubqueryVirtualTableWithSession())
        streams.append(self.InSubqueryVirtualTableWithState())

        # Aggregation cases (2 cases)
        streams.append(self.InSubqueryVirtualTableWithAggregation())
        streams.append(self.InSubqueryVirtualTableWithGroupBy())

        # Edge case (1 case)
        streams.append(self.InSubqueryVirtualTableEmptyResult())

        tdStream.checkAll(streams)

    class InSubqueryVirtualNormalTable(StreamCheckItem):
        """Test IN subquery with virtual normal table"""
        def __init__(self):
            self.db = "test_in_vtable_normal"
            self.refdb = "ref_db_normal"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_normal"
            self.stream = "s_in_vtable_normal"
            self.restb = "res_in_vtable_normal"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, status int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, status int from {self.refdb}.{self.reftb}.status)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 0) ('2026-01-01 00:00:02', 3, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 100)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(1, 2, 300)

    class InSubqueryVirtualChildTable(StreamCheckItem):
        """Test IN subquery with virtual child table"""
        def __init__(self):
            self.db = "test_in_vtable_child"
            self.refdb = "ref_db_child"
            self.triggertb = "trigger_tb"
            self.refstb = "ref_stb"
            self.refctb = "ref_ctb"
            self.vtb = "vtb_child"
            self.stream = "s_in_vtable_child"
            self.restb = "res_in_vtable_child"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create stable {self.refdb}.{self.refstb} (ts timestamp, id int, status int) tags (location nchar(20))")
            tdSql.execute(f"create table {self.refdb}.{self.refctb} using {self.refdb}.{self.refstb} tags ('beijing')")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.refctb}.id, status int from {self.refdb}.{self.refctb}.status)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status > 0) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.refctb} values ('2026-01-01 00:00:00', 10, 5) ('2026-01-01 00:00:01', 20, 0) ('2026-01-01 00:00:02', 30, 3)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 10, 1000) ('2026-01-01 00:00:01', 20, 2000) ('2026-01-01 00:00:02', 30, 3000)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, 1000)
            tdSql.checkData(1, 1, 30)
            tdSql.checkData(1, 2, 3000)

    class InSubqueryVirtualSuperTable(StreamCheckItem):
        """Test IN subquery with virtual super table"""
        def __init__(self):
            self.db = "test_in_vtable_super"
            self.refdb = "ref_db_super"
            self.triggertb = "trigger_tb"
            self.refstb = "ref_stb"
            self.refctb1 = "ref_ctb1"
            self.refctb2 = "ref_ctb2"
            self.vstb = "vstb_super"
            self.stream = "s_in_vtable_super"
            self.restb = "res_in_vtable_super"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create stable {self.refdb}.{self.refstb} (ts timestamp, id int, active int) tags (region nchar(20))")
            tdSql.execute(f"create table {self.refdb}.{self.refctb1} using {self.refdb}.{self.refstb} tags ('north')")
            tdSql.execute(f"create table {self.refdb}.{self.refctb2} using {self.refdb}.{self.refstb} tags ('south')")

            tdSql.execute(f"create stable {self.db}.{self.vstb} (ts timestamp, id int, active int) tags (region nchar(20)) virtual 1")
            tdSql.execute(f"create vtable {self.db}.vtb_super_north ({self.refdb}.{self.refctb1}.id, {self.refdb}.{self.refctb1}.active) using {self.db}.{self.vstb} tags ('north')")
            tdSql.execute(f"create vtable {self.db}.vtb_super_south ({self.refdb}.{self.refctb2}.id, {self.refdb}.{self.refctb2}.active) using {self.db}.{self.vstb} tags ('south')")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vstb} where active = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.refctb1} values ('2026-01-01 00:00:00', 100, 1) ('2026-01-01 00:00:01', 200, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.refctb2} values ('2026-01-01 00:00:02', 300, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 100, 10000) ('2026-01-01 00:00:01', 200, 20000) ('2026-01-01 00:00:02', 300, 30000)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(0, 2, 10000)
            tdSql.checkData(1, 1, 300)
            tdSql.checkData(1, 2, 30000)

    class NotInSubqueryVirtualTable(StreamCheckItem):
        """Test NOT IN subquery with virtual table"""
        def __init__(self):
            self.db = "test_not_in_vtable"
            self.refdb = "ref_db_not_in"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_not_in"
            self.stream = "s_not_in_vtable"
            self.restb = "res_not_in_vtable"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, blocked int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, blocked int from {self.refdb}.{self.reftb}.blocked)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id not in (select id from {self.vtb} where blocked = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 2, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 100)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(1, 2, 300)

    class InSubqueryMultipleVirtualTables(StreamCheckItem):
        """Test IN subquery with multiple virtual tables"""
        def __init__(self):
            self.db = "test_in_multi_vtable"
            self.refdb1 = "ref_db_multi1"
            self.refdb2 = "ref_db_multi2"
            self.triggertb = "trigger_tb"
            self.reftb1 = "ref_tb1"
            self.reftb2 = "ref_tb2"
            self.vtb1 = "vtb_multi1"
            self.vtb2 = "vtb_multi2"
            self.stream = "s_in_multi_vtable"
            self.restb = "res_in_multi_vtable"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb1}.{self.reftb1} (ts timestamp, id int, flag1 int)")
            tdSql.execute(f"create table {self.refdb2}.{self.reftb2} (ts timestamp, id int, flag2 int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb1} (ts timestamp, id int from {self.refdb1}.{self.reftb1}.id, flag1 int from {self.refdb1}.{self.reftb1}.flag1)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb2} (ts timestamp, id int from {self.refdb2}.{self.reftb2}.id, flag2 int from {self.refdb2}.{self.reftb2}.flag2)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb1} where flag1 = 1) "
                f"and id in (select id from {self.vtb2} where flag2 = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 1) ('2026-01-01 00:00:02', 3, 0)")
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:00', 1, 0) ('2026-01-01 00:00:01', 2, 1) ('2026-01-01 00:00:02', 3, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 1)
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(0, 2, 200)

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
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
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
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 3)
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
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
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
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)

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
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
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
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)

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
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
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
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 4)
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
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
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
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 4)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(2, 1, 3)

    class InSubqueryVirtualTableEmptyResult(StreamCheckItem):
        """Test IN subquery with virtual table returning empty result"""
        def __init__(self):
            self.db = "test_in_vtable_empty"
            self.refdb = "ref_db_empty"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_empty"
            self.stream = "s_in_vtable_empty"
            self.restb = "res_in_vtable_empty"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, flag int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, flag int from {self.refdb}.{self.reftb}.flag)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where flag = 999) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 2) ('2026-01-01 00:00:02', 3, 3)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            time.sleep(10)
            tdSql.query(f"select * from {self.db}.{self.restb}")
            tdSql.checkRows(0)
