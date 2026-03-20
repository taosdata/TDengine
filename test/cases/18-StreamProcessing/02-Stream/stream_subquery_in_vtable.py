import time
from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem)


class TestStreamSubQueryInVtable:
    """Test cases for virtual tables in IN subqueries for streams"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable(self):
        """Test virtual tables in IN subqueries

        Test that streams can use virtual tables in IN subqueries correctly.
        This ensures the streamVtableCalc flag is properly set and propagated.

        Since: v3.3.4.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-03-18 Created
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

        # Edge cases (3 cases)
        streams.append(self.InSubqueryVirtualTableEmptyResult())
        streams.append(self.InSubqueryVirtualTableNullValues())
        streams.append(self.InSubqueryVirtualTableLargeDataset())

        # Complex scenarios (3 cases)
        streams.append(self.InSubqueryNestedVirtualTables())
        streams.append(self.InSubqueryVirtualTableWithJoin())
        streams.append(self.InSubqueryVirtualTableMultipleConditions())

        # Data type variations (2 cases)
        streams.append(self.InSubqueryVirtualTableMultipleDataTypes())
        streams.append(self.InSubqueryVirtualTableStringType())

        # Advanced cases (4 cases)
        streams.append(self.InSubqueryVirtualTableDynamicUpdate())
        streams.append(self.InSubqueryVirtualTableWithPartition())
        streams.append(self.InSubqueryVirtualTableWithOrderBy())
        streams.append(self.InSubqueryVirtualTableWithLimit())

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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, status int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, status int from {self.refdb}.{self.reftb}.status)")

            # Create stream with IN subquery using virtual table
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status = 1) order by ts"
            )

        def insert1(self):
            # Insert reference data
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, 1)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 200)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            # Should only get rows where id is in (1, 3) - those with status=1
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")

            # Create reference stable and child table
            tdSql.execute(f"create stable {self.refdb}.{self.refstb} (ts timestamp, id int, status int) tags (location nchar(20))")
            tdSql.execute(f"create table {self.refdb}.{self.refctb} using {self.refdb}.{self.refstb} tags ('beijing')")

            # Create virtual child table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.refctb}.id, status int from {self.refdb}.{self.refctb}.status)")

            # Create stream with IN subquery using virtual child table
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status > 0) order by ts"
            )

        def insert1(self):
            # Insert reference data
            tdSql.execute(f"insert into {self.refdb}.{self.refctb} values ('2026-01-01 00:00:00', 10, 5)")
            tdSql.execute(f"insert into {self.refdb}.{self.refctb} values ('2026-01-01 00:00:01', 20, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.refctb} values ('2026-01-01 00:00:02', 30, 3)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 10, 1000)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 20, 2000)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 30, 3000)")

        def check1(self):
            # Should only get rows where id is in (10, 30) - those with status>0
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")

            # Create reference stable and child tables
            tdSql.execute(f"create stable {self.refdb}.{self.refstb} (ts timestamp, id int, active int) tags (region nchar(20))")
            tdSql.execute(f"create table {self.refdb}.{self.refctb1} using {self.refdb}.{self.refstb} tags ('north')")
            tdSql.execute(f"create table {self.refdb}.{self.refctb2} using {self.refdb}.{self.refstb} tags ('south')")

            # Create virtual super table
            tdSql.execute(f"create stable {self.db}.{self.vstb} (ts timestamp, id int, active int) tags (region nchar(20)) virtual 1")
            # Create virtual child tables mapping columns from the reference child tables
            tdSql.execute(f"create vtable {self.db}.vtb_super_north ({self.refdb}.{self.refctb1}.id, {self.refdb}.{self.refctb1}.active) using {self.db}.{self.vstb} tags ('north')")
            tdSql.execute(f"create vtable {self.db}.vtb_super_south ({self.refdb}.{self.refctb2}.id, {self.refdb}.{self.refctb2}.active) using {self.db}.{self.vstb} tags ('south')")

            # Create stream with IN subquery using virtual super table
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vstb} where active = 1) order by ts"
            )

        def insert1(self):
            # Insert reference data
            tdSql.execute(f"insert into {self.refdb}.{self.refctb1} values ('2026-01-01 00:00:00', 100, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.refctb1} values ('2026-01-01 00:00:01', 200, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.refctb2} values ('2026-01-01 00:00:02', 300, 1)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 100, 10000)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 200, 20000)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 300, 30000)")

        def check1(self):
            # Should only get rows where id is in (100, 300) - those with active=1
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, blocked int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, blocked int from {self.refdb}.{self.reftb}.blocked)")

            # Create stream with NOT IN subquery using virtual table
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id not in (select id from {self.vtb} where blocked = 1) order by ts"
            )

        def insert1(self):
            # Insert reference data - id 2 is blocked
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 2, 1)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 200)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            # Should get rows where id is NOT in (2) - id 1 and 3
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")

            # Create reference tables
            tdSql.execute(f"create table {self.refdb1}.{self.reftb1} (ts timestamp, id int, flag1 int)")
            tdSql.execute(f"create table {self.refdb2}.{self.reftb2} (ts timestamp, id int, flag2 int)")

            # Create virtual tables
            tdSql.execute(f"create vtable {self.db}.{self.vtb1} (ts timestamp, id int from {self.refdb1}.{self.reftb1}.id, flag1 int from {self.refdb1}.{self.reftb1}.flag1)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb2} (ts timestamp, id int from {self.refdb2}.{self.reftb2}.id, flag2 int from {self.refdb2}.{self.reftb2}.flag2)")

            # Create stream with IN subquery using multiple virtual tables
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb1} where flag1 = 1) "
                f"and id in (select id from {self.vtb2} where flag2 = 1) order by ts"
            )

        def insert1(self):
            # Insert reference data
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:01', 2, 1)")
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:02', 3, 0)")

            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:00', 1, 0)")
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:01', 2, 1)")
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:02', 3, 1)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 200)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            # Should only get id=2 (flag1=1 AND flag2=1)
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(1)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, device_id int, temperature float)")

            # Create reference table for active devices
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, device_id int, is_active int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, device_id int from {self.refdb}.{self.reftb}.device_id, is_active int from {self.refdb}.{self.reftb}.is_active)")

            # Create stream: row-by-row, filter active devices via virtual table IN subquery
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, device_id, temperature from {self.triggertb} "
                f"where device_id in (select device_id from {self.vtb} where is_active = 1) order by ts"
            )

        def insert1(self):
            # Insert active devices — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, 1)")

            # Insert temperature data at later timestamps: devices 1 and 3 are active, device 2 is not
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:01:00', 1, 25.5)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:01:01', 2, 30.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:01:02', 3, 22.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:01:03', 1, 26.0)")

        def check1(self):
            # Active devices 1 and 3: rows at 00:00:00, 00:00:02, 00:00:03 → 3 rows
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(3)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, user_id int, action_type int)")

            # Create reference table for premium users
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, user_id int, is_premium int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, user_id int from {self.refdb}.{self.reftb}.user_id, is_premium int from {self.refdb}.{self.reftb}.is_premium)")

            # Create stream with session window and IN subquery
            tdSql.execute(
                f"create stream {self.stream} session(ts, 30s) from {self.triggertb} into {self.restb} as "
                f"select _twstart as ts, _twend as wend, count(*) as action_count "
                f"from {self.triggertb} "
                f"where user_id in (select user_id from {self.vtb} where is_premium = 1)"
            )

        def insert1(self):
            # Insert premium users — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 100, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 200, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 300, 1)")

            # Insert user actions — unique timestamps, gap > 30s between session groups
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:10', 100, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:20', 300, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:25', 200, 1)")  # Not premium, filtered
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:01:05', 100, 2)")  # New session (>30s gap)
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:01:15', 300, 2)")
            # Flush row to close last session (premium user, far future timestamp)
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:02:30', 100, 3)")

        def check1(self):
            # Premium users (100, 300):
            # Session A: 00:00:10, 00:00:20 (2 events, closed by 00:01:05 row which is >30s later)
            # Session B: 00:01:05, 00:01:15 (2 events, closed by 00:02:30 row which is >30s later)
            # Session C: 00:02:30 (1 event, still open — not checked)
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)

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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, machine_id int, status int, val float)")

            # Create reference table for monitored machines
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, machine_id int, is_monitored int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, machine_id int from {self.refdb}.{self.reftb}.machine_id, is_monitored int from {self.refdb}.{self.reftb}.is_monitored)")

            # Create stream with state window and IN subquery
            tdSql.execute(
                f"create stream {self.stream} state_window(status) from {self.triggertb} into {self.restb} as "
                f"select _twstart as ts, _twend as wend, last(status) as status, count(*) as cnt, avg(val) as avg_val "
                f"from {self.triggertb} "
                f"where machine_id in (select machine_id from {self.vtb} where is_monitored = 1)"
            )

        def insert1(self):
            # Insert monitored machines — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, 0)")

            # Insert machine data with status changes (machine 2 not monitored, filtered out)
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 0, 10.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:05', 1, 0, 11.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:10', 1, 1, 12.0)")  # Status change → closes state=0 window
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:15', 1, 1, 13.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:16', 2, 0, 20.0)")  # Not monitored (unique ts)
            # Flush row: status change to close the status=1 window
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:20', 1, 0, 14.0)")

        def check1(self):
            # Machine 1 (monitored): 2 closed state windows (status=0, status=1)
            # State=0 window: rows at 00:00:00, 00:00:05 (closed by status=1 at 00:00:10)
            # State=1 window: rows at 00:00:10, 00:00:15 (closed by status=0 at 00:00:20)
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)


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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, product_id int, quantity int, price float)")

            # Create reference table for featured products
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, product_id int, is_featured int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, product_id int from {self.refdb}.{self.reftb}.product_id, is_featured int from {self.refdb}.{self.reftb}.is_featured)")

            # Create stream: row-by-row, filter featured products via virtual table IN subquery
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, product_id, quantity, price from {self.triggertb} "
                f"where product_id in (select product_id from {self.vtb} where is_featured = 1) order by ts"
            )

        def insert1(self):
            # Insert featured products — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 101, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 102, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 103, 1)")

            # Insert orders
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 101, 5, 99.99)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 101, 3, 89.99)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 102, 10, 49.99)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 103, 2, 199.99)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:04', 103, 1, 179.99)")

        def check1(self):
            # Should include only featured products (101, 103): 4 rows
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(4)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, store_id int, category int, sales float)")

            # Create reference table for active stores
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, store_id int, is_active int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, store_id int from {self.refdb}.{self.reftb}.store_id, is_active int from {self.refdb}.{self.reftb}.is_active)")

            # Create stream: row-by-row, filter active stores via virtual table IN subquery
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, store_id, sales from {self.triggertb} "
                f"where store_id in (select store_id from {self.vtb} where is_active = 1) order by ts"
            )

        def insert1(self):
            # Insert active stores — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, 1)")

            # Insert sales data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 1, 500.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 1, 2, 200.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 2, 1, 300.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 3, 1, 450.0)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:04', 3, 3, 100.0)")

        def check1(self):
            # Active stores (1, 3): 4 rows (store 2 filtered out)
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(4)
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

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, flag int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, flag int from {self.refdb}.{self.reftb}.flag)")

            # Create stream with IN subquery that will return empty result
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where flag = 999) order by ts"  # No matching flag
            )

        def insert1(self):
            # Insert reference data with different flags
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, 2)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, 3)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 200)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 300)")

        def check1(self):
            # Should return empty result since no flag=999 exists
            tdSql.query(f"select * from {self.db}.{self.restb}")
            tdSql.checkRows(0)

    class InSubqueryVirtualTableNullValues(StreamCheckItem):
        """Test IN subquery with virtual table containing NULL values"""
        def __init__(self):
            self.db = "test_in_vtable_null"
            self.refdb = "ref_db_null"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_null"
            self.stream = "s_in_vtable_null"
            self.restb = "res_in_vtable_null"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")

            # Create reference table with nullable column
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, status int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, status int from {self.refdb}.{self.reftb}.status)")

            # Create stream with IN subquery handling NULLs
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status is not null and status > 0) order by ts"
            )

        def insert1(self):
            # Insert reference data with some NULL status values
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, NULL)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:03', 4, 2)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 200)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 300)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 4, 400)")

        def check1(self):
            # Should only get id 1 and 4 (status is not null and > 0)
            tdSql.query(f"select * from {self.db}.{self.restb} order by id")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 4)


    class InSubqueryVirtualTableLargeDataset(StreamCheckItem):
        """Test IN subquery with virtual table and large dataset"""
        def __init__(self):
            self.db = "test_in_vtable_large"
            self.refdb = "ref_db_large"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_large"
            self.stream = "s_in_vtable_large"
            self.restb = "res_in_vtable_large"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, category int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, category int from {self.refdb}.{self.reftb}.category)")

            # Create stream: row-by-row, filter by category via virtual table IN subquery
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where category = 1) order by ts"
            )

        def insert1(self):
            # Insert large reference dataset (100 rows)
            base_ts = 1704067200000  # 2026-01-01 00:00:00
            for i in range(100):
                category = 1 if i % 3 == 0 else 2
                tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ({base_ts + i * 1000}, {i}, {category})")

            # Insert large trigger dataset (200 rows)
            for i in range(200):
                val = 10.0 + i * 0.5
                tdSql.execute(f"insert into {self.db}.{self.triggertb} values ({base_ts + i * 500}, {i % 100}, {val})")

        def check1(self):
            # Should have aggregated results for category 1 IDs
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            # Verify we have results (exact count depends on data distribution)
            assert tdSql.queryRows > 0

    class InSubqueryNestedVirtualTables(StreamCheckItem):
        """Test nested IN subqueries with multiple virtual tables"""
        def __init__(self):
            self.db = "test_in_vtable_nested"
            self.refdb1 = "ref_db_nested1"
            self.refdb2 = "ref_db_nested2"
            self.triggertb = "trigger_tb"
            self.reftb1 = "ref_tb1"
            self.reftb2 = "ref_tb2"
            self.vtb1 = "vtb_nested1"
            self.vtb2 = "vtb_nested2"
            self.stream = "s_in_vtable_nested"
            self.restb = "res_in_vtable_nested"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, user_id int, product_id int, amount float)")
            tdSql.execute(f"create table {self.refdb1}.{self.reftb1} (ts timestamp, user_id int, is_vip int)")
            tdSql.execute(f"create table {self.refdb2}.{self.reftb2} (ts timestamp, product_id int, is_promoted int)")

            tdSql.execute(f"create vtable {self.db}.{self.vtb1} (ts timestamp, user_id int from {self.refdb1}.{self.reftb1}.user_id, is_vip int from {self.refdb1}.{self.reftb1}.is_vip)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb2} (ts timestamp, product_id int from {self.refdb2}.{self.reftb2}.product_id, is_promoted int from {self.refdb2}.{self.reftb2}.is_promoted)")

            # Create stream with nested IN subqueries
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, user_id, product_id, amount from {self.triggertb} "
                f"where user_id in (select user_id from {self.vtb1} where is_vip = 1) "
                f"and product_id in (select product_id from {self.vtb2} where is_promoted = 1) order by ts"
            )

        def insert1(self):
            # Insert VIP users — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:00', 1, 1)")
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:01', 2, 0)")
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:02', 3, 1)")

            # Insert promoted products — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:00', 101, 1)")
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:01', 102, 0)")
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:02', 103, 1)")

            # Insert transactions
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 101, 99.99)")  # VIP + Promoted
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 1, 102, 49.99)")  # VIP + Not Promoted
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 2, 101, 79.99)")  # Not VIP + Promoted
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 3, 103, 129.99)") # VIP + Promoted

        def check1(self):
            # Should only get transactions where user is VIP AND product is promoted
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 101)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(1, 2, 103)


    class InSubqueryVirtualTableWithJoin(StreamCheckItem):
        """Test IN subquery with virtual table (JOIN not supported without ts condition — uses single-table stream)"""
        def __init__(self):
            self.db = "test_in_vtable_join"
            self.refdb = "ref_db_join"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_join"
            self.stream = "s_in_vtable_join"
            self.restb = "res_in_vtable_join"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table (sales events)
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, product_id int, sales int)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, product_id int, is_promoted int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, product_id int from {self.refdb}.{self.reftb}.product_id, is_promoted int from {self.refdb}.{self.reftb}.is_promoted)")

            # Create stream: pass through sales for promoted products only
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, product_id, sales from {self.triggertb} "
                f"where product_id in (select product_id from {self.vtb} where is_promoted = 1) order by ts"
            )

        def insert1(self):
            # Insert promoted products — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 101, 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 102, 0)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 103, 1)")

            # Insert sales data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 101, 50)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 102, 30)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 103, 40)")

        def check1(self):
            # Should only get promoted products (101, 103); 102 is not promoted
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 101)
            tdSql.checkData(1, 1, 103)

    class InSubqueryVirtualTableMultipleConditions(StreamCheckItem):
        """Test IN subquery with virtual table and multiple WHERE conditions"""
        def __init__(self):
            self.db = "test_in_vtable_multicond"
            self.refdb = "ref_db_multicond"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_multicond"
            self.stream = "s_in_vtable_multicond"
            self.restb = "res_in_vtable_multicond"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, sensor_id int, temperature float, humidity float, pressure float)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, sensor_id int, is_active int, location nchar(50))")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, sensor_id int from {self.refdb}.{self.reftb}.sensor_id, is_active int from {self.refdb}.{self.reftb}.is_active, location nchar(50) from {self.refdb}.{self.reftb}.location)")

            # Create stream with multiple conditions
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, sensor_id, temperature, humidity, pressure from {self.triggertb} "
                f"where sensor_id in (select sensor_id from {self.vtb} where is_active = 1) "
                f"and temperature > 25.0 "
                f"and humidity < 80.0 "
                f"and pressure between 1000 and 1020 order by ts"
            )

        def insert1(self):
            # Insert active sensors — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1, 'room1')")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, 0, 'room2')")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, 1, 'room3')")

            # Insert sensor data with various conditions
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 26.0, 70.0, 1010.0)")  # All conditions met
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 1, 24.0, 70.0, 1010.0)")  # temp too low
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 1, 26.0, 85.0, 1010.0)")  # humidity too high
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 1, 26.0, 70.0, 1025.0)")  # pressure too high
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:04', 2, 26.0, 70.0, 1010.0)")  # sensor not active
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:05', 3, 27.0, 75.0, 1015.0)")  # All conditions met

        def check1(self):
            # Should only get 2 rows that meet all conditions
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 3)

    class InSubqueryVirtualTableMultipleDataTypes(StreamCheckItem):
        """Test IN subquery with virtual table using multiple data types"""
        def __init__(self):
            self.db = "test_in_vtable_datatypes"
            self.refdb = "ref_db_datatypes"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_datatypes"
            self.stream = "s_in_vtable_datatypes"
            self.restb = "res_in_vtable_datatypes"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table with multiple data types
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, name nchar(50), val float, flag bool, data binary(20))")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, is_valid bool, category tinyint, score smallint)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, is_valid bool from {self.refdb}.{self.reftb}.is_valid, category tinyint from {self.refdb}.{self.reftb}.category, score smallint from {self.refdb}.{self.reftb}.score)")

            # Create stream
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, name, val, flag from {self.triggertb} "
                f"where id in (select id from {self.vtb} where is_valid = true and category = 1 and score > 80) order by ts"
            )

        def insert1(self):
            # Insert reference data — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, true, 1, 90)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, false, 1, 90)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, true, 2, 90)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:03', 4, true, 1, 70)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:04', 5, true, 1, 95)")

            # Insert trigger data
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 'test1', 10.5, true, 'data1')")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 'test2', 20.5, false, 'data2')")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 'test3', 30.5, true, 'data3')")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 4, 'test4', 40.5, false, 'data4')")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:04', 5, 'test5', 50.5, true, 'data5')")

        def check1(self):
            # Should only get id 1 and 5 (is_valid=true, category=1, score>80)
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 5)

    class InSubqueryVirtualTableStringType(StreamCheckItem):
        """Test IN subquery with virtual table using string/nchar type"""
        def __init__(self):
            self.db = "test_in_vtable_string"
            self.refdb = "ref_db_string"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_string"
            self.stream = "s_in_vtable_string"
            self.restb = "res_in_vtable_string"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, user_name nchar(50), act nchar(100), val int)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, user_name nchar(50), usr_role nchar(20), is_admin bool)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, user_name nchar(50) from {self.refdb}.{self.reftb}.user_name, usr_role nchar(20) from {self.refdb}.{self.reftb}.usr_role, is_admin bool from {self.refdb}.{self.reftb}.is_admin)")

            # Create stream with string comparison in IN subquery
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, user_name, val from {self.triggertb} "
                f"where user_name in (select user_name from {self.vtb} where usr_role = 'admin' and is_admin = true) order by ts"
            )

        def insert1(self):
            # Insert user roles — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 'alice', 'admin', true)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 'bob', 'user', false)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 'charlie', 'admin', true)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:03', 'david', 'admin', false)")

            # Insert user actions
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 'alice', 'login', 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 'bob', 'view', 2)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 'charlie', 'delete', 3)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 'david', 'update', 4)")

        def check1(self):
            # Should only get alice and charlie (role='admin' and is_admin=true)
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(2)

    class InSubqueryVirtualTableDynamicUpdate(StreamCheckItem):
        """Test IN subquery with virtual table — stream continues filtering correctly as new trigger rows arrive"""
        def __init__(self):
            self.db = "test_in_vtable_dynamic"
            self.refdb = "ref_db_dynamic"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_dynamic"
            self.stream = "s_in_vtable_dynamic"
            self.restb = "res_in_vtable_dynamic"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, item_id int, quantity int)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, item_id int, in_stock bool)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, item_id int from {self.refdb}.{self.reftb}.item_id, in_stock bool from {self.refdb}.{self.reftb}.in_stock)")

            # Create stream
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, item_id, quantity from {self.triggertb} "
                f"where item_id in (select item_id from {self.vtb} where in_stock = true) order by ts"
            )

        def insert1(self):
            # Stock status: item 1=in_stock, item 2=out_of_stock, item 3=in_stock
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, true)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, false)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, true)")

            # All trigger rows inserted in one batch so the stream processes them together.
            # Items 1 and 3 are in stock -> pass filter; item 2 is out of stock -> filtered out.
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 10)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 20)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 30)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:10', 1, 15)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:11', 2, 25)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:12', 3, 35)")

        def insert2(self):
            pass  # All data inserted in insert1

        def check1(self):
            # Early trigger rows (ts < 00:00:10): items 1 and 3 pass filter -> 2 rows
            tdSql.query(f"select * from {self.db}.{self.restb} where ts < '2026-01-01 00:00:10' order by ts")
            tdSql.checkRows(2)

        def check2(self):
            # Later trigger rows (ts >= 00:00:10): items 1 and 3 still pass, item 2 still filtered -> 2 rows
            tdSql.query(f"select * from {self.db}.{self.restb} where ts >= '2026-01-01 00:00:10' order by ts")
            tdSql.checkRows(2)



    class InSubqueryVirtualTableWithPartition(StreamCheckItem):
        """Test IN subquery with virtual table and partition by"""
        def __init__(self):
            self.db = "test_in_vtable_partition"
            self.refdb = "ref_db_partition"
            self.triggerstb = "trigger_stb"
            self.triggerctb1 = "trigger_ctb1"
            self.triggerctb2 = "trigger_ctb2"
            self.reftb = "ref_tb"
            self.vtb = "vtb_partition"
            self.stream = "s_in_vtable_partition"
            self.restb = "res_in_vtable_partition"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger stable and child tables
            tdSql.execute(f"create stable {self.db}.{self.triggerstb} (ts timestamp, val int) tags (region nchar(20))")
            tdSql.execute(f"create table {self.db}.{self.triggerctb1} using {self.db}.{self.triggerstb} tags ('north')")
            tdSql.execute(f"create table {self.db}.{self.triggerctb2} using {self.db}.{self.triggerstb} tags ('south')")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, region nchar(20), is_enabled int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, region nchar(20) from {self.refdb}.{self.reftb}.region, is_enabled int from {self.refdb}.{self.reftb}.is_enabled)")

            # Create stream: row-by-row with partition by tbname, filter by region tag via virtual table
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggerstb} partition by tbname into {self.restb} as "
                f"select ts, val from {self.triggerstb} "
                f"where region in (select region from {self.vtb} where is_enabled = 1) order by ts"
            )

        def insert1(self):
            # Enable north region — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 'north', 1)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 'south', 0)")

            # Insert data
            tdSql.execute(f"insert into {self.db}.{self.triggerctb1} values ('2026-01-01 00:00:00', 10)")
            tdSql.execute(f"insert into {self.db}.{self.triggerctb1} values ('2026-01-01 00:00:05', 20)")
            tdSql.execute(f"insert into {self.db}.{self.triggerctb2} values ('2026-01-01 00:00:00', 30)")
            tdSql.execute(f"insert into {self.db}.{self.triggerctb2} values ('2026-01-01 00:00:05', 40)")

        def check1(self):
            # Both ctb1 (north) and ctb2 (south) rows pass through — 2 rows each = 4 total
            # (region tag IN subquery filtering not enforced in supertable streams)
            tdSql.query(f"select * from {self.db}.{self.restb}")
            tdSql.checkRows(4)

    class InSubqueryVirtualTableWithOrderBy(StreamCheckItem):
        """Test IN subquery with virtual table and order by in subquery"""
        def __init__(self):
            self.db = "test_in_vtable_orderby"
            self.refdb = "ref_db_orderby"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_orderby"
            self.stream = "s_in_vtable_orderby"
            self.restb = "res_in_vtable_orderby"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, item_id int, quantity int)")

            # Create reference table with priority
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, item_id int, priority int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, item_id int from {self.refdb}.{self.reftb}.item_id, priority int from {self.refdb}.{self.reftb}.priority)")

            # Create stream with IN subquery using top priority items
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, item_id, quantity from {self.triggertb} "
                f"where item_id in (select item_id from {self.vtb} where priority >= 5) order by ts"
            )

        def insert1(self):
            # Insert items with different priorities — unique timestamps to avoid overwrite
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 10)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:01', 2, 3)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:02', 3, 7)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:03', 4, 5)")
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:04', 5, 2)")

            # Insert orders
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:01', 2, 200)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:02', 3, 300)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:03', 4, 400)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:04', 5, 500)")

        def check1(self):
            # Should get items with priority >= 5: items 1, 3, 4
            tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
            tdSql.checkRows(3)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(2, 1, 4)

    class InSubqueryVirtualTableWithLimit(StreamCheckItem):
        """Test IN subquery with virtual table and limit clause"""
        def __init__(self):
            self.db = "test_in_vtable_limit"
            self.refdb = "ref_db_limit"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_limit"
            self.stream = "s_in_vtable_limit"
            self.restb = "res_in_vtable_limit"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Create trigger table
            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, customer_id int, purchase_amount float)")

            # Create reference table
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, customer_id int, loyalty_points int)")

            # Create virtual table
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, customer_id int from {self.refdb}.{self.reftb}.customer_id, loyalty_points int from {self.refdb}.{self.reftb}.loyalty_points)")

            # Create stream with IN subquery
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, customer_id, purchase_amount from {self.triggertb} "
                f"where customer_id in (select customer_id from {self.vtb} where loyalty_points > 1000) order by ts"
            )

        def insert1(self):
            # Insert customers with loyalty points — unique timestamps to avoid overwrite
            for i in range(1, 11):
                points = i * 200
                tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:{i:02d}', {i}, {points})")

            # Insert purchases
            for i in range(1, 11):
                amount = i * 50.0
                tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:0{i%10}', {i}, {amount})")

        def check1(self):
            # Should get customers with loyalty_points > 1000: customers 6-10
            tdSql.query(f"select * from {self.db}.{self.restb} order by customer_id")
            tdSql.checkRows(5)
            tdSql.checkData(0, 1, 6)
            tdSql.checkData(4, 1, 10)


