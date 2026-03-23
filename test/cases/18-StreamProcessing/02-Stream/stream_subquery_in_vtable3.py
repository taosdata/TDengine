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
    tdSql.checkRows(expected_rows)


class TestStreamSubQueryInVtable3:
    """Test cases for virtual tables in IN subqueries for streams (part 3: data types + advanced)"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable3(self):
        """Test virtual tables in IN subqueries - data type and advanced cases

        Since: v3.3.4.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-03-18 Created
            - 2026-03-23 Split from original file for CI stability
        """

        tdStream.createSnode()
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        streams = []

        # Data type variations (2 cases)
        streams.append(self.InSubqueryVirtualTableMultipleDataTypes())
        streams.append(self.InSubqueryVirtualTableStringType())

        # Advanced cases (4 cases)
        streams.append(self.InSubqueryVirtualTableDynamicUpdate())
        streams.append(self.InSubqueryVirtualTableWithPartition())
        streams.append(self.InSubqueryVirtualTableWithOrderBy())
        streams.append(self.InSubqueryVirtualTableWithLimit())

        tdStream.checkAll(streams)

    class InSubqueryVirtualTableMultipleDataTypes(StreamCheckItem):
        """Test IN subquery with virtual table using multiple data types"""
        def __init__(self):
            self.db = "test_in_vtable_datatypes3"
            self.refdb = "ref_db_datatypes3"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_datatypes"
            self.stream = "s_in_vtable_datatypes3"
            self.restb = "res_in_vtable_datatypes3"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, name nchar(50), val float, flag bool, data binary(20))")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, is_valid bool, category tinyint, score smallint)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, is_valid bool from {self.refdb}.{self.reftb}.is_valid, category tinyint from {self.refdb}.{self.reftb}.category, score smallint from {self.refdb}.{self.reftb}.score)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, name, val, flag from {self.triggertb} "
                f"where id in (select id from {self.vtb} where is_valid = true and category = 1 and score > 80) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, true, 1, 90) ('2026-01-01 00:00:01', 2, false, 1, 90) ('2026-01-01 00:00:02', 3, true, 2, 90) ('2026-01-01 00:00:03', 4, true, 1, 70) ('2026-01-01 00:00:04', 5, true, 1, 95)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 'test1', 10.5, true, 'data1') ('2026-01-01 00:00:01', 2, 'test2', 20.5, false, 'data2') ('2026-01-01 00:00:02', 3, 'test3', 30.5, true, 'data3') ('2026-01-01 00:00:03', 4, 'test4', 40.5, false, 'data4') ('2026-01-01 00:00:04', 5, 'test5', 50.5, true, 'data5')")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 5)

    class InSubqueryVirtualTableStringType(StreamCheckItem):
        """Test IN subquery with virtual table using string/nchar type"""
        def __init__(self):
            self.db = "test_in_vtable_string3"
            self.refdb = "ref_db_string3"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_string"
            self.stream = "s_in_vtable_string3"
            self.restb = "res_in_vtable_string3"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, user_name nchar(50), act nchar(100), val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, user_name nchar(50), usr_role nchar(20), is_admin bool)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, user_name nchar(50) from {self.refdb}.{self.reftb}.user_name, usr_role nchar(20) from {self.refdb}.{self.reftb}.usr_role, is_admin bool from {self.refdb}.{self.reftb}.is_admin)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, user_name, val from {self.triggertb} "
                f"where user_name in (select user_name from {self.vtb} where usr_role = 'admin' and is_admin = true) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 'alice', 'admin', true) ('2026-01-01 00:00:01', 'bob', 'user', false) ('2026-01-01 00:00:02', 'charlie', 'admin', true) ('2026-01-01 00:00:03', 'david', 'admin', false)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 'alice', 'login', 1) ('2026-01-01 00:00:01', 'bob', 'view', 2) ('2026-01-01 00:00:02', 'charlie', 'delete', 3) ('2026-01-01 00:00:03', 'david', 'update', 4)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)

    class InSubqueryVirtualTableDynamicUpdate(StreamCheckItem):
        """Test IN subquery with virtual table — stream continues filtering correctly as new trigger rows arrive"""
        def __init__(self):
            self.db = "test_in_vtable_dynamic3"
            self.refdb = "ref_db_dynamic3"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_dynamic"
            self.stream = "s_in_vtable_dynamic3"
            self.restb = "res_in_vtable_dynamic3"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, item_id int, quantity int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, item_id int, in_stock bool)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, item_id int from {self.refdb}.{self.reftb}.item_id, in_stock bool from {self.refdb}.{self.reftb}.in_stock)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, item_id, quantity from {self.triggertb} "
                f"where item_id in (select item_id from {self.vtb} where in_stock = true) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, true) ('2026-01-01 00:00:01', 2, false) ('2026-01-01 00:00:02', 3, true)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 10) ('2026-01-01 00:00:01', 2, 20) ('2026-01-01 00:00:02', 3, 30) ('2026-01-01 00:00:10', 1, 15) ('2026-01-01 00:00:11', 2, 25) ('2026-01-01 00:00:12', 3, 35)")

        def insert2(self):
            pass

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} where ts < '2026-01-01 00:00:10' order by ts", 2)

        def check2(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} where ts >= '2026-01-01 00:00:10' order by ts", 2)

    class InSubqueryVirtualTableWithPartition(StreamCheckItem):
        """Test IN subquery with virtual table and partition by"""
        def __init__(self):
            self.db = "test_in_vtable_partition3"
            self.refdb = "ref_db_partition3"
            self.triggerstb = "trigger_stb"
            self.triggerctb1 = "trigger_ctb1"
            self.triggerctb2 = "trigger_ctb2"
            self.reftb = "ref_tb"
            self.vtb = "vtb_partition"
            self.stream = "s_in_vtable_partition3"
            self.restb = "res_in_vtable_partition3"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create stable {self.db}.{self.triggerstb} (ts timestamp, val int) tags (region nchar(20))")
            tdSql.execute(f"create table {self.db}.{self.triggerctb1} using {self.db}.{self.triggerstb} tags ('north')")
            tdSql.execute(f"create table {self.db}.{self.triggerctb2} using {self.db}.{self.triggerstb} tags ('south')")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, region nchar(20), is_enabled int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, region nchar(20) from {self.refdb}.{self.reftb}.region, is_enabled int from {self.refdb}.{self.reftb}.is_enabled)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggerstb} partition by tbname into {self.restb} as "
                f"select ts, val from {self.triggerstb} "
                f"where region in (select region from {self.vtb} where is_enabled = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 'north', 1) ('2026-01-01 00:00:01', 'south', 0)")
            tdSql.execute(f"insert into {self.db}.{self.triggerctb1} values ('2026-01-01 00:00:00', 10) ('2026-01-01 00:00:05', 20)")
            tdSql.execute(f"insert into {self.db}.{self.triggerctb2} values ('2026-01-01 00:00:00', 30) ('2026-01-01 00:00:05', 40)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb}", 4)

    class InSubqueryVirtualTableWithOrderBy(StreamCheckItem):
        """Test IN subquery with virtual table and order by in subquery"""
        def __init__(self):
            self.db = "test_in_vtable_orderby3"
            self.refdb = "ref_db_orderby3"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_orderby"
            self.stream = "s_in_vtable_orderby3"
            self.restb = "res_in_vtable_orderby3"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, item_id int, quantity int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, item_id int, priority int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, item_id int from {self.refdb}.{self.reftb}.item_id, priority int from {self.refdb}.{self.reftb}.priority)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, item_id, quantity from {self.triggertb} "
                f"where item_id in (select item_id from {self.vtb} where priority >= 5) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 10) ('2026-01-01 00:00:01', 2, 3) ('2026-01-01 00:00:02', 3, 7) ('2026-01-01 00:00:03', 4, 5) ('2026-01-01 00:00:04', 5, 2)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300) ('2026-01-01 00:00:03', 4, 400) ('2026-01-01 00:00:04', 5, 500)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 3)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(2, 1, 4)

    class InSubqueryVirtualTableWithLimit(StreamCheckItem):
        """Test IN subquery with virtual table and limit clause"""
        def __init__(self):
            self.db = "test_in_vtable_limit3"
            self.refdb = "ref_db_limit3"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_limit"
            self.stream = "s_in_vtable_limit3"
            self.restb = "res_in_vtable_limit3"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable3.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, customer_id int, purchase_amount float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, customer_id int, loyalty_points int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, customer_id int from {self.refdb}.{self.reftb}.customer_id, loyalty_points int from {self.refdb}.{self.reftb}.loyalty_points)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, customer_id, purchase_amount from {self.triggertb} "
                f"where customer_id in (select customer_id from {self.vtb} where loyalty_points > 1000) order by ts"
            )

        def insert1(self):
            ref_values = " ".join(f"('2026-01-01 00:00:{i:02d}', {i}, {i * 200})" for i in range(1, 11))
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values {ref_values}")
            trigger_values = " ".join(f"('2026-01-01 00:00:0{i%10}', {i}, {i * 50.0})" for i in range(1, 11))
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values {trigger_values}")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by customer_id", 5)
            tdSql.checkData(0, 1, 6)
            tdSql.checkData(4, 1, 10)
