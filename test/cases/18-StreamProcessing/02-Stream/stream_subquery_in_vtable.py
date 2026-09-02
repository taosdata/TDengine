import time
from new_test_framework.utils import (tdLog, tdSql, tdCom, tdStream, StreamCheckItem, waitForRows)


class TestStreamSubQueryInVtable:
    """Test cases for virtual tables in IN subqueries for streams (part 1: basic IN/NOT IN + edge)"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable(self):
        """Stream with an IN/NOT IN subquery whose subquery scans a virtual table.

        A sliding(1s) stream filters its trigger rows with `WHERE col IN/NOT IN
        (SELECT ... FROM <vtable> WHERE ...)`. The subquery reads a virtual table
        that maps columns from a separate reference database, so the planner must
        set and propagate the streamVtableCalc flag for the subquery branch. Each
        case asserts the result table contains exactly the trigger rows whose id
        matches the subquery output.

        Scenarios (6):
            1. InSubqueryVirtualNormalTable    - IN, vtable over a basic table; keeps ids with status = 1.
            2. InSubqueryVirtualChildTable     - IN, vtable over a super-table child; keeps ids with status > 0.
            3. InSubqueryVirtualSuperTable     - IN, virtual super table over 2 children; keeps ids with active = 1.
            4. NotInSubqueryVirtualTable       - NOT IN, vtable; drops blocked ids, keeps the rest.
            5. InSubqueryMultipleVirtualTables - two IN subqueries (AND) over vtables in different ref dbs; keeps the common id.
            6. InSubqueryVirtualTableEmptyResult - subquery matches nothing; stream produces 0 rows.

        Window and aggregation scenarios live in stream_subquery_in_vtable4.py.

        Since: v3.3.4.0

        Labels: stream, vtable

        Jira: None

        History:
            - 2026-03-18 Created
            - 2026-03-20 Split into two files for CI stability
            - 2026-06-16 Increase interval stream wait for CI load stability
            - 2026-06-24 Split window/aggregation cases out to vtable4 for CI stability
        """

        tdCom.create_snode_if_not_exists()
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        # Clean up databases from previous runs
        for db in ["test_in_vtable_normal", "ref_db_normal", "test_in_vtable_child", "ref_db_child",
                   "test_in_vtable_super", "ref_db_super", "test_not_in_vtable", "ref_db_not_in",
                   "test_in_multi_vtable", "ref_db_multi1", "ref_db_multi2",
                   "test_in_vtable_empty", "ref_db_empty"]:
            tdSql.execute(f"drop database if exists {db}")

        streams = []

        # Basic IN/NOT IN cases (5 cases)
        streams.append(self.InSubqueryVirtualNormalTable())
        streams.append(self.InSubqueryVirtualChildTable())
        streams.append(self.InSubqueryVirtualSuperTable())
        streams.append(self.NotInSubqueryVirtualTable())
        streams.append(self.InSubqueryMultipleVirtualTables())

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
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
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
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
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
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
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
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 180)
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
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 1, 180)
            tdSql.checkData(0, 1, 2)
            tdSql.checkData(0, 2, 200)

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
