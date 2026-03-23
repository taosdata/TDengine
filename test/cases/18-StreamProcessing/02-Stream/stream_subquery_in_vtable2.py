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


class TestStreamSubQueryInVtable2:
    """Test cases for virtual tables in IN subqueries for streams (part 2: edge cases + complex + data types + advanced)"""
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_in_vtable2(self):
        """Test virtual tables in IN subqueries - edge, complex, data type and advanced cases

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

        # Edge cases (2 cases)
        streams.append(self.InSubqueryVirtualTableNullValues())
        streams.append(self.InSubqueryVirtualTableLargeDataset())

        # Complex scenarios (3 cases)
        streams.append(self.InSubqueryNestedVirtualTables())
        streams.append(self.InSubqueryVirtualTableWithJoin())
        streams.append(self.InSubqueryVirtualTableMultipleConditions())

        tdStream.checkAll(streams)

    class InSubqueryVirtualTableNullValues(StreamCheckItem):
        """Test IN subquery with virtual table containing NULL values"""
        def __init__(self):
            self.db = "test_in_vtable_null2"
            self.refdb = "ref_db_null2"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_null"
            self.stream = "s_in_vtable_null2"
            self.restb = "res_in_vtable_null2"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, status int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, status int from {self.refdb}.{self.reftb}.status)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where status is not null and status > 0) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, NULL) ('2026-01-01 00:00:02', 3, 0) ('2026-01-01 00:00:03', 4, 2)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 100) ('2026-01-01 00:00:01', 2, 200) ('2026-01-01 00:00:02', 3, 300) ('2026-01-01 00:00:03', 4, 400)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by id", 2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 4)

    class InSubqueryVirtualTableLargeDataset(StreamCheckItem):
        """Test IN subquery with virtual table and large dataset"""
        def __init__(self):
            self.db = "test_in_vtable_large2"
            self.refdb = "ref_db_large2"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_large"
            self.stream = "s_in_vtable_large2"
            self.restb = "res_in_vtable_large2"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, id int, val float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, id int, category int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, id int from {self.refdb}.{self.reftb}.id, category int from {self.refdb}.{self.reftb}.category)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, id, val from {self.triggertb} "
                f"where id in (select id from {self.vtb} where category = 1) order by ts"
            )

        def insert1(self):
            base_ts = 1704067200000
            # Batch insert reference data
            for batch_start in range(0, 100, 50):
                values = " ".join(f"({base_ts + i * 1000}, {i}, {1 if i % 3 == 0 else 2})" for i in range(batch_start, min(batch_start + 50, 100)))
                tdSql.execute(f"insert into {self.refdb}.{self.reftb} values {values}")
            # Batch insert trigger data
            for batch_start in range(0, 200, 50):
                values = " ".join(f"({base_ts + i * 500}, {i % 100}, {10.0 + i * 0.5})" for i in range(batch_start, min(batch_start + 50, 200)))
                tdSql.execute(f"insert into {self.db}.{self.triggertb} values {values}")

        def check1(self):
            for i in range(WAIT_TIMEOUT):
                tdSql.query(f"select * from {self.db}.{self.restb} order by ts")
                if tdSql.queryRows > 0:
                    break
                time.sleep(1)
            assert tdSql.queryRows > 0

    class InSubqueryNestedVirtualTables(StreamCheckItem):
        """Test nested IN subqueries with multiple virtual tables"""
        def __init__(self):
            self.db = "test_in_vtable_nested2"
            self.refdb1 = "ref_db_nested12"
            self.refdb2 = "ref_db_nested22"
            self.triggertb = "trigger_tb"
            self.reftb1 = "ref_tb1"
            self.reftb2 = "ref_tb2"
            self.vtb1 = "vtb_nested1"
            self.vtb2 = "vtb_nested2"
            self.stream = "s_in_vtable_nested2"
            self.restb = "res_in_vtable_nested2"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, user_id int, product_id int, amount float)")
            tdSql.execute(f"create table {self.refdb1}.{self.reftb1} (ts timestamp, user_id int, is_vip int)")
            tdSql.execute(f"create table {self.refdb2}.{self.reftb2} (ts timestamp, product_id int, is_promoted int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb1} (ts timestamp, user_id int from {self.refdb1}.{self.reftb1}.user_id, is_vip int from {self.refdb1}.{self.reftb1}.is_vip)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb2} (ts timestamp, product_id int from {self.refdb2}.{self.reftb2}.product_id, is_promoted int from {self.refdb2}.{self.reftb2}.is_promoted)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, user_id, product_id, amount from {self.triggertb} "
                f"where user_id in (select user_id from {self.vtb1} where is_vip = 1) "
                f"and product_id in (select product_id from {self.vtb2} where is_promoted = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb1}.{self.reftb1} values ('2026-01-01 00:00:00', 1, 1) ('2026-01-01 00:00:01', 2, 0) ('2026-01-01 00:00:02', 3, 1)")
            tdSql.execute(f"insert into {self.refdb2}.{self.reftb2} values ('2026-01-01 00:00:00', 101, 1) ('2026-01-01 00:00:01', 102, 0) ('2026-01-01 00:00:02', 103, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 101, 99.99) ('2026-01-01 00:00:01', 1, 102, 49.99) ('2026-01-01 00:00:02', 2, 101, 79.99) ('2026-01-01 00:00:03', 3, 103, 129.99)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 101)
            tdSql.checkData(1, 1, 3)
            tdSql.checkData(1, 2, 103)

    class InSubqueryVirtualTableWithJoin(StreamCheckItem):
        """Test IN subquery with virtual table — single-table stream with IN filter"""
        def __init__(self):
            self.db = "test_in_vtable_join2"
            self.refdb = "ref_db_join2"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_join"
            self.stream = "s_in_vtable_join2"
            self.restb = "res_in_vtable_join2"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, product_id int, sales int)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, product_id int, is_promoted int)")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, product_id int from {self.refdb}.{self.reftb}.product_id, is_promoted int from {self.refdb}.{self.reftb}.is_promoted)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, product_id, sales from {self.triggertb} "
                f"where product_id in (select product_id from {self.vtb} where is_promoted = 1) order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 101, 1) ('2026-01-01 00:00:01', 102, 0) ('2026-01-01 00:00:02', 103, 1)")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 101, 50) ('2026-01-01 00:00:01', 102, 30) ('2026-01-01 00:00:02', 103, 40)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 101)
            tdSql.checkData(1, 1, 103)

    class InSubqueryVirtualTableMultipleConditions(StreamCheckItem):
        """Test IN subquery with virtual table and multiple WHERE conditions"""
        def __init__(self):
            self.db = "test_in_vtable_multicond2"
            self.refdb = "ref_db_multicond2"
            self.triggertb = "trigger_tb"
            self.reftb = "ref_tb"
            self.vtb = "vtb_multicond"
            self.stream = "s_in_vtable_multicond2"
            self.restb = "res_in_vtable_multicond2"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb} vgroups 1 buffer 8 precision '{TestStreamSubQueryInVtable2.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.db}.{self.triggertb} (ts timestamp, sensor_id int, temperature float, humidity float, pressure float)")
            tdSql.execute(f"create table {self.refdb}.{self.reftb} (ts timestamp, sensor_id int, is_active int, location nchar(50))")
            tdSql.execute(f"create vtable {self.db}.{self.vtb} (ts timestamp, sensor_id int from {self.refdb}.{self.reftb}.sensor_id, is_active int from {self.refdb}.{self.reftb}.is_active, location nchar(50) from {self.refdb}.{self.reftb}.location)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.triggertb} into {self.restb} as "
                f"select ts, sensor_id, temperature, humidity, pressure from {self.triggertb} "
                f"where sensor_id in (select sensor_id from {self.vtb} where is_active = 1) "
                f"and temperature > 25.0 and humidity < 80.0 and pressure between 1000 and 1020 order by ts"
            )

        def insert1(self):
            tdSql.execute(f"insert into {self.refdb}.{self.reftb} values ('2026-01-01 00:00:00', 1, 1, 'room1') ('2026-01-01 00:00:01', 2, 0, 'room2') ('2026-01-01 00:00:02', 3, 1, 'room3')")
            tdSql.execute(f"insert into {self.db}.{self.triggertb} values ('2026-01-01 00:00:00', 1, 26.0, 70.0, 1010.0) ('2026-01-01 00:00:01', 1, 24.0, 70.0, 1010.0) ('2026-01-01 00:00:02', 1, 26.0, 85.0, 1010.0) ('2026-01-01 00:00:03', 1, 26.0, 70.0, 1025.0) ('2026-01-01 00:00:04', 2, 26.0, 70.0, 1010.0) ('2026-01-01 00:00:05', 3, 27.0, 75.0, 1015.0)")

        def check1(self):
            wait_for_rows(f"select * from {self.db}.{self.restb} order by ts", 2)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 3)
