from new_test_framework.utils import tdLog, tdSql, etool

import uuid


class TestCorrelatedScalarSemantics:
    # P00 captures existing behavior; P02 must replace only the legal rejections.
    INVALID_COLUMN = 0x2602
    TABLE_NOT_EXIST = 0x2603
    AMBIGUOUS_COLUMN = 0x2604
    SCALAR_COLUMNS = 0x26A4
    SCALAR_ROWS = 0x26A5
    CORRELATED_UNSUPPORTED = 0x26A6
    SCALAR_UNSUPPORTED_SQL = 0x26A7
    SCALAR_UNSUPPORTED_CLAUSE = 0x26A8
    SCALAR_POLICY = 0x26A9

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def check_result(self, sql, expected):
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(len(expected))
        if expected:
            tdSql.checkCols(len(expected[0]))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def prepare_data(self):
        tdSql.execute(
            "create table current_readings "
            "(ts timestamp, device_id varchar(64), voltage int)"
        )
        tdSql.execute(
            "create table meter_history "
            "(ts timestamp, device_id varchar(64), voltage int, status tinyint)"
        )
        tdSql.execute(
            "insert into current_readings values "
            "('2026-09-11 00:00:10', 'd1', 10) "
            "('2026-09-11 00:00:20', 'd1', 20) "
            "('2026-09-11 00:01:10', 'd2', 50)"
        )
        tdSql.execute(
            "insert into meter_history values "
            "('2026-09-11 00:00:10', 'd1', 20, 1) "
            "('2026-09-11 00:00:20', 'd1', 30, 1) "
            "('2026-09-11 00:01:10', 'd2', 60, 1)"
        )
        tdSql.execute(
            "create table fs_history "
            "(ts timestamp, device_id varchar(64), voltage int)"
        )
        tdSql.execute(
            "insert into fs_history values "
            "('2026-09-11 00:00:10', 'd1', 210) "
            "('2026-09-11 00:00:20', 'd1', 230) "
            "('2026-09-11 00:01:10', 'd2', 240) "
            "('2026-09-11 00:01:20', 'd2', 260)"
        )

    # --- impl ---
    def do_constant_controls(self):
        self.check_result(
            "select device_id, voltage from current_readings order by ts",
            [('d1', 10), ('d1', 20), ('d2', 50)],
        )
        # Literal substitution provides an oracle without requiring correlation.
        for device, voltage, ts, avg, ranged, deviation, latest in [
            ('d1', 10, '2026-09-11 00:00:10', 25, 20, 15, 20),
            ('d1', 20, '2026-09-11 00:00:20', 25, 25, 5, 30),
            ('d2', 50, '2026-09-11 00:01:10', 60, 60, 10, 60),
        ]:
            source = f"from meter_history where device_id = '{device}'"
            time_range = (
                f"and ts > cast('{ts}' as timestamp) - 10m "
                f"and ts <= cast('{ts}' as timestamp)"
            )
            self.check_result(f"select (select avg(voltage) {source})", [(avg,)])
            self.check_result(
                f"select (select avg(voltage) {source} {time_range})",
                [(ranged,)],
            )
            self.check_result(
                f"select (select avg(abs(voltage - {voltage})) {source})",
                [(deviation,)],
            )
            self.check_result(
                f"select (select voltage {source} {time_range} "
                "order by ts desc limit 1)",
                [(latest,)],
            )
        self.check_result(
            "select device_id, avg(voltage) from fs_history "
            "group by device_id order by device_id",
            [('d1', 220), ('d2', 250)],
        )
        print("constant controls ......................... [ passed ]")

    def do_existing_scalar_behavior(self):
        self.check_result("select 1", [(1,)])
        self.check_result(
            "select (select 1) from current_readings order by ts",
            [(1,), (1,), (1,)],
        )
        for sql in ["select (select 1)", "select (select (select 2))"]:
            tdSql.error(sql, expectedErrno=self.SCALAR_UNSUPPORTED_SQL)
        for predicate, expected in [
            ("device_id = 'missing'", None),
            ("device_id = 'd2'", 60),
        ]:
            self.check_result(
                "select r.voltage, (select voltage from meter_history "
                f"where {predicate}) from current_readings r order by r.ts",
                [(10, expected), (20, expected), (50, expected)],
            )
        tdSql.error(
            "select (select 7 from meter_history where device_id = 'd1')",
            expectedErrno=self.SCALAR_ROWS,
        )
        self.check_result(
            "select (select voltage from meter_history "
            "where device_id = 'd1' order by ts limit 1 offset 1)",
            [(30,)],
        )
        self.check_result(
            "select (select count(*) from meter_history "
            "where device_id = 'missing')",
            [(0,)],
        )
        self.check_result(
            "select (select avg(r.voltage) from meter_history r) "
            "from current_readings r order by r.ts",
            [(110 / 3,), (110 / 3,), (110 / 3,)],
        )
        print("existing scalar behavior .................. [ passed ]")

    def do_static_errors(self):
        for sql in [
            "select (select voltage, status from meter_history limit 1) "
            "from current_readings where voltage < 0",
            "select case when voltage > 0 then 0 else "
            "(select voltage, status from meter_history limit 1) end "
            "from current_readings",
        ]:
            tdSql.error(sql, expectedErrno=self.SCALAR_COLUMNS)
        tdSql.error("select voltage", expectedErrno=self.INVALID_COLUMN)
        tdSql.error(
            "select current_readings.voltage from current_readings r",
            expectedErrno=self.TABLE_NOT_EXIST,
        )
        tdSql.error(
            "select r.voltage from (select device_id from current_readings) r",
            expectedErrno=self.INVALID_COLUMN,
        )
        print("static errors ............................. [ passed ]")

    def do_unsupported_correlated_rejections(self):
        # Non-scalar correlation remains outside this feature's scope.
        for predicate in [
            "r.voltage in (select h.voltage from meter_history h "
            "where h.device_id = r.device_id)",
            "exists (select h.voltage from meter_history h "
            "where h.device_id = r.device_id)",
            "r.voltage > any (select h.voltage from meter_history h "
            "where h.device_id = r.device_id)",
            "r.voltage > all (select h.voltage from meter_history h "
            "where h.device_id = r.device_id)",
        ]:
            tdSql.error(
                f"select r.voltage from current_readings r where {predicate}",
                expectedErrno=self.CORRELATED_UNSUPPORTED,
            )
        tdSql.error(
            "select r.voltage from current_readings r, "
            "(select h.voltage from meter_history h "
            "where h.device_id = r.device_id) d",
            expectedErrno=self.TABLE_NOT_EXIST,
        )
        scalar = (
            "(select h.voltage from meter_history h "
            "where h.device_id = r.device_id limit 1)"
        )
        tdSql.error(
            f"create topic p14_corr_topic as select r.ts, {scalar} "
            "from current_readings r",
            expectedErrno=self.SCALAR_UNSUPPORTED_CLAUSE,
        )
        tdSql.error(
            "create stream p14_corr_stream interval(1d) sliding(1d) "
            "from current_readings into p14_corr_output "
            f"as select r.ts, {scalar} from current_readings r",
            expectedErrno=self.SCALAR_UNSUPPORTED_CLAUSE,
        )
        print("unsupported correlated rejections ......... [ passed ]")

    def do_scope_rules(self):
        self.check_result(
            "select r.voltage, (select max(h.voltage + "
            "(select r.voltage from meter_history r where r.ts = h.ts limit 1)) "
            "from meter_history h where h.device_id = r.device_id) "
            "from current_readings r order by r.ts",
            [(10, 60), (20, 60), (50, 120)],
        )
        tdSql.error(
            "select r.voltage, (select max(voltage) "
            "from meter_history h, current_readings x "
            "where h.device_id = r.device_id and x.device_id = r.device_id) "
            "from current_readings r",
            expectedErrno=self.AMBIGUOUS_COLUMN,
        )
        print("nearest scope and local ambiguity ....... [ passed ]")

    def do_policy_controls(self):
        tdSql.query("show qnodes")
        create_qnode = tdSql.queryRows == 0
        if create_qnode:
            tdSql.execute("create qnode on dnode 1")
        try:
            for policy in [1, 2, 3]:
                tdSql.execute(f'alter local "queryPolicy" "{policy}"')
                self.check_result(
                    "select (select avg(voltage) from meter_history "
                    "where device_id = 'd1') from current_readings order by ts",
                    [(25,), (25,), (25,)],
                )
            tdSql.execute('alter local "queryPolicy" "4"')
            tdSql.error(
                "select (select avg(voltage) from meter_history) "
                "from current_readings",
                expectedErrno=self.SCALAR_POLICY,
            )
        finally:
            if create_qnode:
                tdSql.execute("drop qnode on dnode 1")
        print("policy controls ........................... [ passed ]")

    def do_baseline(self):
        tdSql.query("show local variables")
        settings = {row[0]: row[1] for row in tdSql.queryResult}
        # SHOW appends display text, e.g. "Asia/Shanghai (CST, +0800)".
        settings['timezone'] = settings['timezone'].split(' (', 1)[0]
        database = f"correlated_scalar_p00_{uuid.uuid4().hex[:8]}"
        tdSql.execute(f"create database {database} precision 'ms'")
        try:
            tdSql.execute(f"use {database}")
            tdSql.execute('alter local "timezone" "UTC"')
            tdSql.execute('alter local "queryPolicy" "1"')
            self.prepare_data()
            self.do_constant_controls()
            self.do_existing_scalar_behavior()
            self.do_static_errors()
            self.do_unsupported_correlated_rejections()
            self.do_policy_controls()
        finally:
            try:
                for name in ['queryPolicy', 'timezone']:
                    tdSql.execute(f'alter local "{name}" "{settings[name]}"')
            finally:
                tdSql.execute(f"drop database {database}")

    def do_correlated_results(self):
        self.check_result(
            "select r.voltage, (select r.voltage + 1) "
            "from current_readings r order by r.ts",
            [(10, 11), (20, 21), (50, 51)],
        )
        self.check_result(
            "select r.device_id, (select avg(h.voltage) from fs_history h "
            "where h.device_id = r.device_id) from current_readings r order by r.ts",
            [('d1', 220), ('d1', 220), ('d2', 250)],
        )
        self.check_result(
            "select r.device_id, (select avg(h.voltage) from meter_history h "
            "where h.device_id = r.device_id and h.status = 1) "
            "from current_readings r order by r.ts",
            [('d1', 25), ('d1', 25), ('d2', 60)],
        )
        self.check_result(
            "select r.voltage, (select avg(h.voltage) from meter_history h "
            "where h.device_id = r.device_id and h.ts > r.ts - 10m "
            "and h.ts <= r.ts) from current_readings r order by r.ts",
            [(10, 20), (20, 25), (50, 60)],
        )
        self.check_result(
            "select r.voltage, (select avg(abs(h.voltage - r.voltage)) "
            "from meter_history h where h.device_id = r.device_id) "
            "from current_readings r order by r.ts",
            [(10, 15), (20, 5), (50, 10)],
        )
        self.check_result(
            "select r.voltage, (select h.voltage from meter_history h "
            "where h.device_id = r.device_id and h.ts = r.ts) "
            "from current_readings r order by r.ts",
            [(10, 20), (20, 30), (50, 60)],
        )
        self.check_result(
            "select r.voltage, (select h.voltage from meter_history h "
            "where h.device_id = r.device_id and h.ts <= r.ts "
            "order by h.ts desc limit 1) "
            "from current_readings r order by r.ts",
            [(10, 20), (20, 30), (50, 60)],
        )
        print("correlated results ....................... [ passed ]")

    def do_cardinality_and_null(self):
        self.check_result(
            "select r.voltage, (select h.voltage from meter_history h "
            "where h.device_id = r.device_id and h.ts < r.ts) "
            "from current_readings r where r.voltage in (10, 20) order by r.ts",
            [(10, None), (20, 20)],
        )
        tdSql.error(
            "select (select 1 from meter_history h "
            "where h.device_id = r.device_id) "
            "from current_readings r",
            expectedErrno=self.SCALAR_ROWS,
        )
        tdSql.error(
            "select r.device_id, (select h.voltage from meter_history h "
            "where h.device_id = r.device_id) from current_readings r",
            expectedErrno=self.SCALAR_ROWS,
        )
        self.check_result(
            "select r.voltage, (select count(*) from meter_history h "
            "where h.device_id = r.device_id and h.ts < r.ts), "
            "(select avg(h.voltage) from meter_history h "
            "where h.device_id = r.device_id and h.ts < r.ts) "
            "from current_readings r where r.voltage in (10, 20) order by r.ts",
            [(10, 0, None), (20, 1, 20)],
        )
        self.check_result(
            "select r.voltage, (select avg(h.voltage) from meter_history h "
            "where h.device_id = r.device_id having avg(h.voltage) > 40) "
            "from current_readings r order by r.ts",
            [(10, None), (20, None), (50, 60)],
        )
        self.check_result(
            "select r.voltage, (select h.voltage from meter_history h "
            "where h.device_id = r.device_id order by h.ts desc limit 1 offset 1) "
            "from current_readings r order by r.ts",
            [(10, 20), (20, 20), (50, None)],
        )
        print("correlated cardinality and NULL ......... [ passed ]")

    def do_ancestor_reference(self):
        self.check_result(
            "select r.voltage, (select avg(h.voltage) from meter_history h "
            "where h.device_id = r.device_id and h.voltage > "
            "(select avg(h2.voltage) from meter_history h2 "
            "where h2.device_id = h.device_id and h2.ts <= r.ts)) "
            "from current_readings r order by r.ts",
            [(10, 30), (20, 30), (50, None)],
        )
        print("ancestor reference ....................... [ passed ]")

    def do_three_level_ancestor_reference(self):
        tdSql.execute("create table nested_readings (ts timestamp, device_id int)")
        tdSql.execute("create table nested_history (ts timestamp, device_id int, voltage int)")
        tdSql.execute("create table calibration (ts timestamp, device_id int, delta int)")
        tdSql.execute("insert into nested_readings values ('2024-01-01 00:00:10', 202)")
        tdSql.execute(
            "insert into nested_history values "
            "('2024-01-01 00:00:00', 202, 40) "
            "('2024-01-01 00:00:05', 202, 50)"
        )
        tdSql.execute(
            "insert into calibration values "
            "('2024-01-01 00:00:00', 202, 1) "
            "('2024-01-01 00:00:05', 202, 3) "
            "('2024-01-01 00:00:20', 202, 100)"
        )
        sql = (
            "select r.device_id, "
            "(select sum(h.voltage + "
            "(select avg(c.delta) from calibration c "
            "where c.device_id = h.device_id and c.ts <= r.ts)) "
            "from nested_history h where h.device_id = r.device_id) "
            "from nested_readings r"
        )
        self.check_result(sql, [(202, 94)])
        tdSql.error(
            sql.replace("select avg(c.delta)", "select c.delta"),
            expectedErrno=self.SCALAR_ROWS,
        )
        tdSql.error(
            "select (select h.voltage + (select avg(c.delta) "
            "from calibration c where c.device_id = h.device_id and c.ts <= r.ts) "
            "from nested_history h where h.device_id = r.device_id) "
            "from nested_readings r",
            expectedErrno=self.SCALAR_ROWS,
        )
        tdSql.execute("create table ancestor_readings (ts timestamp, marker int)")
        tdSql.execute(
            "insert into ancestor_readings values "
            "('2024-01-01 00:00:00', 1) ('2024-01-01 00:00:10', 2)"
        )
        self.check_result(
            "select (select sum((select avg(c.delta) from calibration c "
            "where c.device_id = h.device_id and c.ts <= r.ts)) "
            "from nested_history h) from ancestor_readings r order by r.ts",
            [(2,), (4,)],
        )
        independent_sql = (
            "select (select sum(h.voltage + (select avg(c.delta) "
            "from calibration c where c.device_id = h.device_id and c.ts <= h.ts)) "
            "from nested_history h) from ancestor_readings r order by r.ts"
        )
        self.check_result(independent_sql, [(93,), (93,)])
        print("three-level ancestor reference ......... [ passed ]")

    def do_expression_positions(self):
        scalar = (
            "(select avg(h.voltage) from meter_history h "
            "where h.device_id = r.device_id)"
        )
        self.check_result(
            f"select r.voltage from current_readings r where r.voltage > {scalar}",
            [],
        )
        self.check_result(
            f"select sum({scalar}) from current_readings r",
            [(110,)],
        )
        self.check_result(
            f"select count(*), sum(r.voltage) from current_readings r group by {scalar} "
            "order by 2",
            [(2, 30), (1, 50)],
        )
        tdSql.query(
            f"select r.voltage from current_readings r partition by {scalar}",
            queryTimes=1,
        )
        assert sorted(row[0] for row in tdSql.queryResult) == [10, 20, 50]
        self.check_result(
            f"select r.device_id, sum(r.voltage) from current_readings r "
            f"group by r.device_id having sum(r.voltage) > {scalar}",
            [('d1', 30)],
        )
        window_sql = (
            "select sum(r.voltage), (select count(*) from meter_history h "
            "where h.voltage < sum(r.voltage)) from current_readings r "
            "interval(1m) order by _wstart"
        )
        self.check_result(
            window_sql,
            [(30, 1), (50, 2)],
        )
        self.check_result(
            f"select r.voltage from current_readings r "
            f"order by {scalar} desc, r.ts",
            [(50,), (10,), (20,)],
        )
        print("operator expression positions ........... [ passed ]")

    def do_aggregate_ownership(self):
        outer_sum = (
            "select (select sum(r.voltage) from meter_history h limit 1) "
            "from current_readings r where r.device_id = 'd1'"
        )
        self.check_result(outer_sum, [(30,)])
        tdSql.error(
            outer_sum.replace(" limit 1", ""),
            expectedErrno=self.SCALAR_ROWS,
        )
        tdSql.error(
            "select r.voltage from current_readings r "
            "where (select count(r.voltage) from meter_history h) > 2",
            expectedErrno=0x2608,
        )
        tdSql.error(
            "select sum((select count(r.voltage) from meter_history h limit 1)) "
            "from current_readings r",
            expectedErrno=0x2627,
        )
        self.check_result(
            "select r.device_id, (select count(*) from meter_history h "
            "where h.device_id = r.device_id), "
            "(select sum(1) from meter_history h where h.device_id = r.device_id) "
            "from current_readings r order by r.ts",
            [('d1', 2, 2), ('d1', 2, 2), ('d2', 1, 1)],
        )
        self.check_result(
            "select r.device_id, (select sum(r.voltage) from meter_history h "
            "where h.device_id = r.device_id limit 1) "
            "from current_readings r group by r.device_id order by r.device_id",
            [('d1', 30), ('d2', 50)],
        )
        self.check_result(
            "select r.device_id, (select (select sum(r.voltage) "
            "from fs_history f limit 1) from meter_history h "
            "where h.device_id = r.device_id limit 1) "
            "from current_readings r group by r.device_id order by r.device_id",
            [('d1', 30), ('d2', 50)],
        )
        self.check_result(
            "select r.voltage, (select (select sum(h.voltage + r.voltage) "
            "from fs_history f limit 1) from meter_history h "
            "where h.device_id = r.device_id) "
            "from current_readings r order by r.ts",
            [(10, 70), (20, 90), (50, 110)],
        )
        tdSql.error(
            "select r.device_id, (select h.voltage from meter_history h "
            "where h.voltage = r.voltage limit 1) "
            "from current_readings r group by r.device_id",
            expectedErrno=0x260A,
        )
        print("aggregate ownership ...................... [ passed ]")

    def do_repeated_and_null_parameters(self):
        long_key = "x" * 50
        tdSql.execute("create table param_readings (ts timestamp, seq int, device_id varchar(64))")
        tdSql.execute("create table param_history (ts timestamp, device_id varchar(64), v int)")
        tdSql.execute(
            "insert into param_readings values "
            "('2026-09-11 00:02:01', 1, 'd1') "
            "('2026-09-11 00:02:02', 2, null) "
            f"('2026-09-11 00:02:03', 3, '{long_key}') "
            "('2026-09-11 00:02:04', 4, 'x') "
            "('2026-09-11 00:02:05', 5, 'd1')"
        )
        tdSql.execute(
            "insert into param_history values "
            "('2026-09-11 00:02:01', 'd1', 11) "
            f"('2026-09-11 00:02:03', '{long_key}', 99) "
            "('2026-09-11 00:02:04', 'x', 7)"
        )
        self.check_result(
            "select r.seq, (select h.v from param_history h "
            "where h.device_id = r.device_id) "
            "from param_readings r order by r.ts",
            [(1, 11), (2, None), (3, 99), (4, 7), (5, 11)],
        )
        print("repeated, NULL and varying parameters .. [ passed ]")

    def do_correlated_semantics(self):
        database = f"correlated_scalar_e1_{uuid.uuid4().hex[:8]}"
        tdSql.execute(f"create database {database} precision 'ms'")
        try:
            tdSql.execute(f"use {database}")
            self.prepare_data()
            self.do_correlated_results()
            self.do_scope_rules()
            self.do_cardinality_and_null()
            self.do_ancestor_reference()
            self.do_three_level_ancestor_reference()
            self.do_expression_positions()
            self.do_aggregate_ownership()
            self.do_repeated_and_null_parameters()
        finally:
            tdSql.execute(f"drop database {database}")

    # --- main ---
    def test_p00_baseline(self):
        """Scalar correlation P00 behavior baseline

        1. Fixed dataset A and FS 4.1 values with literal substitution controls.
        2. Existing scalar NULL/value/cardinality, aliases and no-FROM behavior.
        3. Unsupported correlation rejection and query policies 1/2/3/4.
        4. Existing scalar behavior remains the baseline; correlated behavior
           and invocation counts are covered separately.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-14 Codex Add deterministic P00 baseline.

        """
        self.do_baseline()

    def test_correlated_semantics(self):
        """Correlated scalar results and ancestor binding

        1. Row-specific aggregation, range, expression and cardinality.
        2. COUNT and AVG over an empty filtered set.
        3. Nested scalar referencing the root and immediate parent.
        4. Duplicate, NULL, long and short parameter values do not retain stale data.
        5. WHERE, aggregate input, GROUP, PARTITION, HAVING, window and ORDER
           BY evaluate the current row or group at their owning operator.
        6. Pure outer SUM owns the outer aggregate row; invalid WHERE and
           nested aggregate uses fail during translation.
        7. Grouping and nested aggregates keep their owners.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-17 Codex Verify FS 4 and 9.6 correlated semantics.

        """
        self.do_correlated_semantics()

    def test_empty_outer_aggregate(self):
        """An empty outer input keeps the existing zero-row SUM behavior.

        Compare ordinary and correlated scalar SUM queries when the outer
        filter matches no rows; both must return an empty result.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        """
        database = f"correlated_scalar_t24_{uuid.uuid4().hex[:8]}"
        tdSql.execute(f"create database {database} precision 'ms'")
        try:
            tdSql.execute(f"use {database}")
            self.prepare_data()
            self.check_result(
                "select sum(r.voltage) from current_readings r "
                "where r.device_id = 'missing'",
                [],
            )
            self.check_result(
                "select (select sum(r.voltage) from meter_history h limit 1) "
                "from current_readings r where r.device_id = 'missing'",
                [],
            )
        finally:
            tdSql.execute(f"drop database {database}")
