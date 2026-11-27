# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql, etool

from datetime import datetime, timedelta
import json
import os
import sys
import time

import pytest

_VTABLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _VTABLE_DIR not in sys.path:
    sys.path.insert(0, _VTABLE_DIR)

from vtable_util import ExtSrcEnv, VtableQueryUtil
from ext_source_helpers import (
    create_ext_source, create_mysql_table, create_mysql_view,
    create_oracle_table, create_oracle_view, create_pg_table, create_pg_view,
    create_remote_db, create_sqlserver_table, extract_show_create_sql,
)

_BACKENDS = ("mysql", "postgres", "sqlserver", "oracle")
_LOCAL_DB = "vreference"
_REMOTE_DB = "vreference_ext"
_ORACLE_PREFIX = "vreference_"
_BASE = datetime(2024, 1, 1)
_BASE_US = 1704067200000000


@pytest.mark.parametrize("backend", _BACKENDS)
class TestVtableReferenceExtSource:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.prepared_backends = set()
        cls.oracle_tables = set()
        cls.oracle_views = set()

    @classmethod
    def teardown_class(cls):
        for backend in cls.prepared_backends:
            if backend == "oracle":
                cfg = next(ExtSrcEnv.oracle_version_configs())
                for kind, objects in (("VIEW", cls.oracle_views), ("TABLE", cls.oracle_tables)):
                    for name in objects:
                        try:
                            ExtSrcEnv._oracle_sql(
                                cfg, f'DROP {kind} "{name}"', user=cfg.user,
                                password=cfg.password)
                        except RuntimeError as error:
                            if "ORA-00942" not in str(error):
                                raise
            else:
                drop = {"mysql": ExtSrcEnv.mysql_drop_db,
                        "postgres": ExtSrcEnv.pg_drop_db,
                        "sqlserver": ExtSrcEnv.sqlserver_drop_db}[backend]
                drop(f"{_REMOTE_DB}_{backend}")

    @pytest.fixture(autouse=True)
    def prepare_backend(self, backend):
        self.configure_backend(backend)
        VtableQueryUtil._EXT_SOURCE_ADAPTERS[backend]().ensure_running()
        ExtSrcEnv.ensure_qnode()
        tdSql.query("SHOW LOCAL VARIABLES LIKE 'timezone'")
        timezone = str(tdSql.getData(0, 1)).split(" ")[0]
        try:
            tdSql.execute('ALTER LOCAL "timezone" "UTC"')
            if backend not in self.prepared_backends:
                create_remote_db(self.source_type(), self.remote_db)
                self.prepared_backends.add(backend)
                self.prepare_remote_data()
            database = ExtSrcEnv.ORACLE_SERVICE if backend == "oracle" else self.remote_db
            create_ext_source(self.source, self.source_type(), database)
            tdSql.execute(f"DROP DATABASE IF EXISTS {self.local_db}")
            tdSql.execute(f"CREATE DATABASE {self.local_db} PRECISION 'us'")
            tdSql.execute(f"USE {self.local_db}")
            yield
        finally:
            try:
                tdSql.execute(f"DROP DATABASE IF EXISTS {self.local_db}")
            finally:
                try:
                    tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {self.source}")
                finally:
                    tdSql.execute(f'ALTER LOCAL "timezone" "{timezone}"')

    # --- util ---

    def configure_backend(self, backend):
        self.backend = backend
        self.local_db = f"{_LOCAL_DB}_{backend}"
        self.remote_db = f"{_REMOTE_DB}_{backend}"
        self.source = f"vreference_{backend}_src"
        self.time_type = {"mysql": "DATETIME(3)", "postgres": "TIMESTAMP(3)",
                          "sqlserver": "DATETIME2(3)", "oracle": "TIMESTAMP(3)"}[backend]
        self.integer_type = "NUMBER(18,0)" if backend == "oracle" else "BIGINT"
        self.float_type = {"mysql": "DOUBLE", "postgres": "DOUBLE PRECISION",
                           "sqlserver": "FLOAT", "oracle": "BINARY_DOUBLE"}[backend]
        self.text_type = "VARCHAR2(16)" if backend == "oracle" else "VARCHAR(16)"

    def source_type(self):
        return "postgresql" if self.backend == "postgres" else self.backend

    def table_name(self, name):
        return f"{_ORACLE_PREFIX}{name}" if self.backend == "oracle" else name

    def quote(self, name):
        if self.backend in ("oracle", "postgres"):
            return '"' + name.replace('"', '""') + '"'
        if self.backend == "sqlserver":
            return "[" + name.replace("]", "]]") + "]"
        return "`" + name.replace("`", "``") + "`"

    def remote_object(self, name, schema=None):
        table = self.quote(self.table_name(name))
        if schema is None:
            schema = {"postgres": "public", "sqlserver": "dbo"}.get(self.backend)
        return f"{self.quote(schema)}.{table}" if schema else table

    def reference_path(self, name, schema=None, database=None):
        table = self.table_name(name)
        if self.backend == "oracle":
            return f"{self.source}.`{ExtSrcEnv.ORACLE_USER.upper()}`.{table}"
        database = database or self.remote_db
        if self.backend == "sqlserver":
            return f"{self.source}.{database}.{schema or 'dbo'}.{table}"
        if schema is not None:
            return f"{self.source}.{database}.{schema}.{table}"
        return f"{self.source}.{database}.{table}"

    def time_literal(self, milliseconds, microseconds=0):
        value = _BASE + timedelta(milliseconds=milliseconds, microseconds=microseconds)
        literal = "'" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
        return "TIMESTAMP " + literal if self.backend == "oracle" else literal

    def remote_execute(self, statements, database=None):
        if self.backend == "oracle":
            cfg = next(ExtSrcEnv.oracle_version_configs())
            ExtSrcEnv._oracle_sql(cfg, statements, user=cfg.user, password=cfg.password)
        else:
            execute = {"mysql": ExtSrcEnv.mysql_exec, "postgres": ExtSrcEnv.pg_exec,
                       "sqlserver": ExtSrcEnv.sqlserver_exec}[self.backend]
            execute(database or self.remote_db, statements)

    def create_table(self, name, columns, rows=None, database=None):
        if self.backend == "oracle":
            table = self.table_name(name)
            self.oracle_tables.add(table)
            create_oracle_table(table, columns, rows)
        else:
            create = {"mysql": create_mysql_table, "postgres": create_pg_table,
                      "sqlserver": create_sqlserver_table}[self.backend]
            create(database or self.remote_db, name, columns, rows)

    def create_view(self, name, select_sql, schema=None):
        if self.backend == "oracle":
            table = self.table_name(name)
            self.oracle_views.add(table)
            create_oracle_view(table, select_sql)
        elif self.backend == "mysql":
            create_mysql_view(self.remote_db, name, select_sql)
        elif self.backend == "postgres":
            create_pg_view(self.remote_db, name, select_sql, schema or "public")
        else:
            target = self.remote_object(name, schema)
            self.remote_execute([
                f"IF OBJECT_ID('{schema or 'dbo'}.{name}') IS NOT NULL DROP VIEW {target}",
                f"CREATE VIEW {target} AS {select_sql}",
            ])

    def drop_view(self, name):
        target = self.remote_object(name)
        if self.backend == "oracle":
            try:
                self.remote_execute([f"DROP VIEW {target}"])
            except RuntimeError as error:
                if "ORA-00942" not in str(error):
                    raise
        elif self.backend == "sqlserver":
            self.remote_execute([f"IF OBJECT_ID('dbo.{name}') IS NOT NULL DROP VIEW {target}"])
        else:
            self.remote_execute([f"DROP VIEW IF EXISTS {target}"])

    def prepare_remote_data(self):
        self.drop_view("readings_view")
        columns = (f"row_id {self.integer_type} PRIMARY KEY, device {self.text_type}, "
                   f"event_time {self.time_type}, collected_time {self.time_type}, "
                   f"reading {self.float_type}, voltage {self.integer_type}")
        rows = [
            f"(1, 'd1', {self.time_literal(0)}, {self.time_literal(500)}, 1, 220)",
            f"(2, 'd1', {self.time_literal(1000)}, {self.time_literal(1500)}, NULL, 221)",
            f"(3, 'd1', {self.time_literal(3000)}, {self.time_literal(3500)}, 3, 223)",
            f"(4, 'd2', {self.time_literal(0)}, {self.time_literal(500)}, 10, 230)",
            f"(5, 'd2', {self.time_literal(2000)}, {self.time_literal(2500)}, 20, 232)",
        ]
        self.create_table("readings", columns, rows)
        self.create_view("readings_view", f"SELECT * FROM {self.remote_object('readings')}")
        self.create_table(
            "time_key", f"key_time {self.time_type} NOT NULL PRIMARY KEY, "
            f"event_time {self.time_type}, reading {self.float_type}",
            [f"({self.time_literal(0)}, {self.time_literal(2000)}, 10)",
             f"({self.time_literal(1000)}, {self.time_literal(0)}, 20)",
             f"({self.time_literal(2000)}, {self.time_literal(1000)}, 30)"])
        self.create_table(
            "invalid_times", f"row_id {self.integer_type} PRIMARY KEY, "
            f"event_time {self.time_type}, reading {self.integer_type}",
            ["(1, NULL, 1)", f"(2, {self.time_literal(0)}, 11)",
             f"(3, {self.time_literal(0)}, 12)", f"(4, {self.time_literal(1000)}, 13)"])

    def check_rows(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row, values in enumerate(expected):
            for column, value in enumerate(values):
                tdSql.checkData(row, column, value)

    def aligned_rows(self, columns):
        timestamps = set()
        for name, expression, sequence in columns:
            timestamps.update(sequence)
        return [(timestamp, *[sequence.get(timestamp) for name, expression, sequence in columns])
                for timestamp in sorted(timestamps)]

    def create_readings_vtable(self, name="v", time_column="event_time", view=True):
        path = self.reference_path("readings_view" if view else "readings")
        tdSql.execute(
            f"CREATE VTABLE {name} (ts TIMESTAMP, "
            "reading DOUBLE FROM r.reading WHERE device = 'd1', "
            "voltage BIGINT FROM r.voltage WHERE device = 'd1') "
            f"REFERENCE r AS {path} TIMESTAMP {time_column}")

    def validation_rows(self, table):
        tdSql.query(f"SHOW VTABLE VALIDATE FOR {table}")
        names = [column[0].lower() for column in tdSql.cursor.description]
        return [dict(zip(names, row)) for row in tdSql.queryResult]

    # --- impl ---

    def do_create_table_and_view(self):
        for name, view in (("v_table", False), ("v_view", True)):
            self.create_readings_vtable(name, view=view)
            self.check_rows(f"SELECT reading, voltage FROM {name} ORDER BY ts",
                            [(1, 220), (None, 221), (3, 223)])
            self.check_rows(f"SELECT COUNT(*), COUNT(reading), SUM(reading), AVG(reading) FROM {name}",
                            [(3, 2, 4, 2)])
        self.create_table("no_key", f"event_time {self.time_type}, reading {self.float_type}",
                          [f"({self.time_literal(0)}, 7)"])
        tdSql.execute("CREATE VTABLE v_no_key (ts TIMESTAMP, reading DOUBLE FROM r.reading) "
                      f"REFERENCE r AS {self.reference_path('no_key')} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_no_key", [(7,)])
        print("reference table, view and primary-key admission ............ [ passed ]")

    def do_selected_axis_and_value_columns(self):
        tdSql.execute(
            "CREATE VTABLE v_axis (ts TIMESTAMP, reading DOUBLE FROM r.reading, "
            "original_time TIMESTAMP FROM r.key_time, selected_time TIMESTAMP FROM r.event_time) "
            f"REFERENCE r AS {self.reference_path('time_key')} TIMESTAMP event_time")
        self.check_rows(
            "SELECT CAST(ts AS BIGINT), reading, CAST(original_time AS BIGINT), "
            "CAST(selected_time AS BIGINT) FROM v_axis ORDER BY ts",
            [(_BASE_US, 20, _BASE_US + 1000000, _BASE_US),
             (_BASE_US + 1000000, 30, _BASE_US + 2000000, _BASE_US + 1000000),
             (_BASE_US + 2000000, 10, _BASE_US, _BASE_US + 2000000)])
        self.check_rows("SELECT FIRST(reading), LAST(reading) FROM v_axis", [(20, 10)])
        self.check_rows("SELECT reading FROM v_axis ORDER BY ts DESC LIMIT 1", [(10,)])
        self.check_rows("SELECT reading FROM v_axis ORDER BY ts LIMIT 1 OFFSET 1", [(30,)])
        print("reference selected axis and ordinary timestamp values ............ [ passed ]")

    def do_mixed_direct_reference_axes(self):
        self.create_table(
            "mixed_axes", f"key_time {self.time_type} NOT NULL PRIMARY KEY, "
            f"event_time {self.time_type}, reading {self.float_type}",
            [f"({self.time_literal(0)}, {self.time_literal(10000)}, 10)",
             f"({self.time_literal(1000)}, {self.time_literal(11000)}, 20)"])
        path = self.reference_path("mixed_axes")
        expected = [(_BASE_US, 10, None), (_BASE_US + 1000000, 20, None),
                    (_BASE_US + 10000000, None, 10), (_BASE_US + 11000000, None, 20)]
        for order, columns in enumerate((
                [("direct_value", f"{path}.reading"), ("ref_value", "r.reading")],
                [("ref_value", "r.reading"), ("direct_value", f"{path}.reading")])):
            normal, stable, child = (f"v_mixed_axis_{order}", f"vst_mixed_axis_{order}",
                                     f"c_mixed_axis_{order}")
            definitions = ", ".join(f"{name} DOUBLE FROM {source}" for name, source in columns)
            tdSql.execute(f"CREATE VTABLE {normal} (ts TIMESTAMP, {definitions}) "
                          f"REFERENCE r AS {path} TIMESTAMP event_time")
            schema = ", ".join(f"{name} DOUBLE" for name, source in columns)
            tdSql.execute(f"CREATE STABLE {stable} (ts TIMESTAMP, {schema}) "
                          "TAGS (group_id INT) VIRTUAL 1")
            bindings = ", ".join(f"{name} FROM {source}" for name, source in columns)
            tdSql.execute(f"CREATE VTABLE {child} ({bindings}) USING {stable} TAGS (1) "
                          f"REFERENCE r AS {path} TIMESTAMP event_time")
            for table in (normal, child, stable):
                self.check_rows(f"SELECT CAST(ts AS BIGINT) FROM {table} ORDER BY ts",
                                [(row[0],) for row in expected])
                self.check_rows(f"SELECT CAST(ts AS BIGINT), direct_value, ref_value "
                                f"FROM {table} ORDER BY ts", expected)
                self.check_rows(f"SELECT CAST(ts AS BIGINT), direct_value FROM {table} ORDER BY ts",
                                [(row[0], row[1]) for row in expected[:2]])
                self.check_rows(f"SELECT CAST(ts AS BIGINT), ref_value FROM {table} ORDER BY ts",
                                [(row[0], row[2]) for row in expected[2:]])
        print("reference and direct bindings retain independent time axes ............ [ passed ]")

    def do_validate_column_case(self):
        self.create_table("case_validation", f"event_time {self.time_type}, "
                          f"{self.quote('Reading')} {self.float_type}",
                          [f"({self.time_literal(0)}, 7)"])
        column = "reading" if self.backend in ("mysql", "sqlserver") else "`Reading`"
        path = self.reference_path("case_validation")
        tdSql.execute(f"CREATE VTABLE v_case (ts TIMESTAMP, reading DOUBLE FROM r.{column}) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_case", [(7,)])
        rows = self.validation_rows("v_case")
        assert len(rows) == 1 and rows[0]["err_code"] == 0 and rows[0]["reference_alias"] == "r", rows
        self.check_rows("SELECT err_code FROM information_schema.ins_virtual_tables_referencing "
                        f"WHERE virtual_db_name = '{self.local_db}' AND virtual_table_name = 'v_case'",
                        [(0,)])
        if self.backend == "mysql":
            self.remote_execute([f"ALTER TABLE {self.remote_object('case_validation')} "
                                 f"RENAME COLUMN {self.quote('Reading')} TO {self.quote('reading')}"])
        elif self.backend == "sqlserver":
            self.remote_execute(["EXEC sp_rename 'dbo.case_validation.Reading', 'reading', 'COLUMN'"])
        else:
            tdSql.error("CREATE VTABLE v_case_bad (ts TIMESTAMP, reading DOUBLE FROM r.reading) "
                        f"REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_case", [(7,)])
        rows = self.validation_rows("v_case")
        assert len(rows) == 1 and rows[0]["err_code"] == 0 and rows[0]["reference_alias"] == "r", rows
        print("reference validation follows source column case rules ............ [ passed ]")

    def do_validate_wide_reference_table(self):
        paths = [self.reference_path("readings_view"), self.reference_path("time_key")]
        columns = ", ".join(f"v{index} DOUBLE FROM r{index % 2}.reading" for index in range(128))
        tdSql.execute(f"CREATE VTABLE v_wide (ts TIMESTAMP, {columns}) "
                      f"REFERENCE r0 AS {paths[0]} TIMESTAMP event_time "
                      f"REFERENCE r1 AS {paths[1]} TIMESTAMP event_time "
                      f"REFERENCE unused AS {paths[0]} TIMESTAMP collected_time")
        for unused in (True, False):
            rows = self.validation_rows("v_wide")
            assert len(rows) == 128 + int(unused), rows
            assert all(row["err_code"] == 0 for row in rows), rows
            assert len({row["reference_object"] for row in rows}) == 2, rows
            if unused:
                tdSql.execute("ALTER VTABLE v_wide REMOVE REFERENCE unused")
        self.drop_view("readings_view")
        try:
            rows = self.validation_rows("v_wide")
            assert len(rows) == 128, rows
            assert all(row["err_code"] != 0 for row in rows if row["reference_alias"] == "r0"), rows
            assert all(row["err_code"] == 0 for row in rows if row["reference_alias"] == "r1"), rows
        finally:
            self.create_view("readings_view", f"SELECT * FROM {self.remote_object('readings')}")
        rows = self.validation_rows("v_wide")
        assert len(rows) == 128 and all(row["err_code"] == 0 for row in rows), rows
        print("reference wide validation reuses object metadata across bindings ............ [ passed ]")

    def do_sparse_alignment(self):
        path = self.reference_path("readings_view")
        tdSql.execute(
            "CREATE VTABLE v_merge (ts TIMESTAMP, a DOUBLE FROM r.reading WHERE device = 'd1', "
            f"b DOUBLE FROM r.reading WHERE device = 'd2') REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT CAST(ts AS BIGINT), a, b FROM v_merge ORDER BY ts",
                        [(_BASE_US, 1, 10), (_BASE_US + 1000000, None, None),
                         (_BASE_US + 2000000, None, 20), (_BASE_US + 3000000, 3, None)])
        self.check_rows("SELECT COUNT(*), COUNT(a), COUNT(b), SUM(a), SUM(b) FROM v_merge",
                        [(4, 2, 2, 4, 30)])
        tdSql.execute(
            "CREATE VTABLE v_axes (ts TIMESTAMP, a DOUBLE FROM event_ref.reading WHERE device = 'd1', "
            "b DOUBLE FROM collected_ref.reading WHERE device = 'd1') "
            f"REFERENCE event_ref AS {path} TIMESTAMP event_time "
            f"REFERENCE collected_ref AS {path} TIMESTAMP collected_time")
        self.check_rows("SELECT CAST(ts AS BIGINT), a, b FROM v_axes ORDER BY ts",
                        [(_BASE_US, 1, None), (_BASE_US + 500000, None, 1),
                         (_BASE_US + 1000000, None, None), (_BASE_US + 1500000, None, None),
                         (_BASE_US + 3000000, 3, None), (_BASE_US + 3500000, None, 3)])
        self.check_rows("SELECT CAST(ts AS BIGINT), b FROM v_axes ORDER BY ts",
                        [(_BASE_US + 500000, 1), (_BASE_US + 1500000, None), (_BASE_US + 3500000, 3)])
        print("reference independent filters and sparse axes ............ [ passed ]")

    def do_add_reference_to_existing_table(self):
        tdSql.execute("CREATE TABLE local_data (ts TIMESTAMP, reading DOUBLE)")
        tdSql.execute(f"INSERT INTO local_data VALUES ({_BASE_US}, 99)")
        tdSql.execute("CREATE VTABLE v_old (ts TIMESTAMP, reading DOUBLE FROM local_data.reading)")
        path = self.reference_path("readings_view")
        tdSql.execute(f"ALTER VTABLE v_old ADD REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_old", [(99,)])
        tdSql.execute("ALTER VTABLE v_old ALTER COLUMN reading SET r.reading WHERE device = 'd1'")
        self.check_rows("SELECT reading FROM v_old ORDER BY ts", [(1,), (None,), (3,)])
        tdSql.error("ALTER VTABLE v_old REMOVE REFERENCE r")
        tdSql.execute("ALTER VTABLE v_old ALTER COLUMN reading SET NULL")
        tdSql.execute("ALTER VTABLE v_old REMOVE REFERENCE r")
        tdSql.execute("ALTER VTABLE v_old ALTER COLUMN reading SET local_data.reading")
        self.check_rows("SELECT reading FROM v_old", [(99,)])
        print("reference additions to existing tables and dependency removal ............ [ passed ]")

    def do_modify_reference_atomically(self):
        self.create_readings_vtable()
        path = self.reference_path("readings_view")
        tdSql.execute(f"ALTER VTABLE v MODIFY REFERENCE r AS {path} TIMESTAMP collected_time")
        expected = [(_BASE_US + 500000, 1, 220), (_BASE_US + 1500000, None, 221),
                    (_BASE_US + 3500000, 3, 223)]
        sql = "SELECT CAST(ts AS BIGINT), reading, voltage FROM v ORDER BY ts"
        self.check_rows(sql, expected)
        invalid_columns = [
            f"event_time {self.time_type}, device {self.text_type}, reading {self.float_type}",
            f"event_time {self.time_type}, device {self.text_type}, reading {self.float_type}, voltage {self.text_type}",
            f"event_time {self.time_type}, reading {self.float_type}, voltage {self.integer_type}",
        ]
        for index, columns in enumerate(invalid_columns):
            name = f"bad_modify_{index}"
            self.create_table(name, columns)
            tdSql.error(f"ALTER VTABLE v MODIFY REFERENCE r AS {self.reference_path(name)} TIMESTAMP event_time")
            self.check_rows(sql, expected)
        tdSql.error(f"ALTER VTABLE v MODIFY REFERENCE r AS {path} TIMESTAMP voltage")
        self.check_rows(sql, expected)
        tdSql.execute(f"ALTER VTABLE v MODIFY REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows(sql, [(_BASE_US, 1, 220), (_BASE_US + 1000000, None, 221),
                              (_BASE_US + 3000000, 3, 223)])
        print("reference multi-column modify and failure atomicity ............ [ passed ]")

    def do_column_binding_lifecycle(self):
        self.create_readings_vtable()
        path = self.reference_path("readings_view")
        tdSql.execute(f"ALTER VTABLE v ADD REFERENCE r2 AS {path} TIMESTAMP collected_time")
        tdSql.execute("ALTER VTABLE v ADD COLUMN extra DOUBLE FROM r2.reading WHERE device = 'd1'")
        self.check_rows("SELECT COUNT(*) FROM v", [(6,)])
        tdSql.error("ALTER VTABLE v REMOVE REFERENCE r2")
        tdSql.execute("ALTER VTABLE v ALTER COLUMN extra SET r.reading WHERE device = 'd1'")
        self.check_rows("SELECT reading, extra FROM v ORDER BY ts", [(1, 1), (None, None), (3, 3)])
        tdSql.execute("ALTER VTABLE v ALTER COLUMN extra SET NULL")
        tdSql.execute("ALTER VTABLE v REMOVE REFERENCE r2")
        tdSql.execute("ALTER VTABLE v DROP COLUMN extra")
        tdSql.execute("ALTER VTABLE v ALTER COLUMN reading SET NULL")
        tdSql.execute("ALTER VTABLE v ALTER COLUMN voltage SET NULL")
        tdSql.execute("ALTER VTABLE v REMOVE REFERENCE r")
        self.check_rows("SELECT COUNT(*) FROM v", [(0,)])
        print("reference add, rebind, clear and drop column dependencies ............ [ passed ]")

    def do_display_validate_and_replay(self):
        self.create_readings_vtable()
        path = self.reference_path("readings_view")
        tdSql.execute(f"ALTER VTABLE v ADD REFERENCE unused AS {path} TIMESTAMP collected_time")
        tdSql.query("DESCRIBE v")
        source = str(tdSql.getData(1, 4)).lower()
        assert "timestamp event_time" in source and "where" in source and "d1" in source
        tdSql.query("SELECT col_source FROM information_schema.ins_columns "
                    f"WHERE db_name = '{self.local_db}' AND table_name = 'v' AND col_name = 'reading'")
        tdSql.checkRows(1)
        assert "timestamp event_time" in str(tdSql.getData(0, 0)).lower()
        rows = self.validation_rows("v")
        assert len(rows) == 3, rows
        for row in rows:
            assert row["err_code"] == 0, row
        unused = [row for row in rows if row["reference_alias"] == "unused"]
        assert len(unused) == 1, rows
        assert unused[0]["type"] == 2 and unused[0]["virtual_col_name"] is None, unused
        assert unused[0]["src_column_name"] is None, unused
        assert unused[0]["reference_time_column"].lower() == "collected_time", unused
        bound = [row for row in rows if row["reference_alias"] == "r"]
        assert {row["virtual_col_name"] for row in bound} == {"reading", "voltage"}, rows
        for row in bound:
            assert row["reference_time_column"].lower() == "event_time", row
            assert row["reference_object"], row
        definition = extract_show_create_sql("SHOW CREATE VTABLE v")
        assert definition.upper().count("REFERENCE ") == 2, definition
        assert definition.index("unused") > definition.index("event_time"), definition
        tdSql.execute(definition.replace("`v`", "`v_replayed`", 1))
        self.check_rows("SELECT reading, voltage FROM v_replayed ORDER BY ts",
                        [(1, 220), (None, 221), (3, 223)])
        replayed = self.validation_rows("v_replayed")
        assert len(replayed) == 3, replayed
        for row in replayed:
            assert row["err_code"] == 0, row
        unused = [row for row in replayed if row["reference_alias"] == "unused"]
        assert len(unused) == 1, replayed
        assert unused[0]["type"] == 2 and unused[0]["virtual_col_name"] is None, unused
        assert unused[0]["reference_time_column"].lower() == "collected_time", unused
        print("reference display, unused validation and definition replay ............ [ passed ]")

    def do_alias_resolution(self):
        path = self.reference_path("readings_view")
        tdSql.execute("CREATE TABLE r (ts TIMESTAMP, reading DOUBLE)")
        tdSql.execute(f"INSERT INTO r VALUES ({_BASE_US}, 99)")
        tdSql.execute("CREATE VTABLE v_alias (ts TIMESTAMP, reading DOUBLE FROM r.reading WHERE device = 'd1', "
                      f"local_value DOUBLE FROM {self.local_db}.r.reading) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_alias ORDER BY ts", [(1,), (None,), (3,)])
        self.check_rows("SELECT local_value FROM v_alias", [(99,)])
        tdSql.execute("ALTER VTABLE v_alias ADD COLUMN extra DOUBLE FROM r.reading WHERE device = 'd1'")
        tdSql.execute("ALTER VTABLE v_alias ALTER COLUMN local_value SET r.reading WHERE device = 'd1'")
        self.check_rows("SELECT reading, local_value, extra FROM v_alias ORDER BY ts",
                        [(1, 1, 1), (None, None, None), (3, 3, 3)])
        tdSql.execute("CREATE VTABLE v_fallback (ts TIMESTAMP, reading DOUBLE FROM refcase.reading WHERE device = 'd1') "
                      f"REFERENCE `RefCase` AS {path} TIMESTAMP event_time")
        tdSql.execute("ALTER VTABLE v_fallback ADD COLUMN extra DOUBLE FROM refcase.reading WHERE device = 'd1'")
        self.check_rows("SELECT extra FROM v_fallback ORDER BY ts", [(1,), (None,), (3,)])
        tdSql.execute("ALTER VTABLE v_fallback ALTER COLUMN extra SET refcase.reading WHERE device = 'd1'")
        self.check_rows("SELECT extra FROM v_fallback ORDER BY ts", [(1,), (None,), (3,)])
        tdSql.execute("CREATE TABLE refcase (ts TIMESTAMP, reading DOUBLE)")
        tdSql.execute(f"INSERT INTO refcase VALUES ({_BASE_US}, 77)")
        tdSql.execute("ALTER VTABLE v_fallback ALTER COLUMN extra SET refcase.reading")
        self.check_rows("SELECT extra FROM v_fallback", [(77,)])
        tdSql.execute("CREATE VTABLE v_local (ts TIMESTAMP, reading DOUBLE FROM refcase.reading) "
                      f"REFERENCE `RefCase` AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_local", [(77,)])
        tdSql.execute("ALTER VTABLE v_local ADD COLUMN extra DOUBLE FROM refcase.reading")
        self.check_rows("SELECT extra FROM v_local", [(77,)])
        tdSql.execute("CREATE VTABLE v_case (ts TIMESTAMP, reading DOUBLE) "
                      f"REFERENCE `UnusedCase` AS {path} TIMESTAMP event_time")
        tdSql.error("ALTER VTABLE v_case REMOVE REFERENCE unusedcase")
        tdSql.execute("ALTER VTABLE v_case REMOVE REFERENCE `UnusedCase`")
        print("reference exact, local and case-fallback alias resolution ............ [ passed ]")

    def do_children_and_batches(self):
        path = self.reference_path("readings_view")
        tdSql.execute("CREATE STABLE vst (ts TIMESTAMP, reading DOUBLE) TAGS (device VARCHAR(16), group_id INT) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE c1 (reading FROM r.reading WHERE device = 'd1', "
            "extra DOUBLE FROM r.reading WHERE device = 'd1', pending DOUBLE) USING vst TAGS ('d1', 1) "
            f"REFERENCE r AS {path} TIMESTAMP event_time "
            "c2 (reading FROM r.reading WHERE device = 'd2') USING vst TAGS ('d2', 2) "
            f"REFERENCE r AS {path} TIMESTAMP collected_time")
        self.check_rows("SELECT reading, extra, pending FROM c1 ORDER BY ts",
                        [(1, 1, None), (None, None, None), (3, 3, None)])
        tdSql.execute("ALTER VTABLE c1 ALTER COLUMN pending SET r.reading WHERE device = 'd1'")
        self.check_rows("SELECT pending FROM c1 ORDER BY ts", [(1,), (None,), (3,)])
        self.check_rows("SELECT group_id, COUNT(reading), SUM(reading) FROM vst GROUP BY group_id ORDER BY group_id",
                        [(1, 2, 4), (2, 2, 30)])
        self.check_rows("SELECT reading FROM vst WHERE group_id = 2 ORDER BY ts", [(10,), (20,)])
        self.check_rows("SELECT CAST(ts AS BIGINT) FROM c2 ORDER BY ts",
                        [(_BASE_US + 500000,), (_BASE_US + 2500000,)])
        tdSql.execute("CREATE VTABLE c_key (reading FROM r.reading, chosen TIMESTAMP FROM r.event_time, "
                      "original TIMESTAMP FROM r.key_time) USING vst TAGS ('key', 3) "
                      f"REFERENCE r AS {self.reference_path('time_key')} TIMESTAMP event_time")
        self.check_rows("SELECT reading, CAST(chosen AS BIGINT), CAST(original AS BIGINT) FROM c_key ORDER BY ts",
                        [(20, _BASE_US, _BASE_US + 1000000),
                         (30, _BASE_US + 1000000, _BASE_US + 2000000),
                         (10, _BASE_US + 2000000, _BASE_US)])
        print("reference batched children, owned columns and dynamic queries ............ [ passed ]")

    def do_null_timestamps(self):
        path = self.reference_path("invalid_times")
        tdSql.execute("CREATE VTABLE v_null (ts TIMESTAMP, reading BIGINT FROM r.reading) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        tdSql.execute("CREATE STABLE vst_null (ts TIMESTAMP, reading BIGINT) TAGS (group_id INT) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE c_null (reading FROM r.reading) USING vst_null TAGS (1) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        for table in ("v_null", "c_null", "vst_null"):
            for sql in (
                f"SELECT * FROM {table}", f"SELECT * FROM {table} LIMIT 1",
                f"SELECT * FROM {table} LIMIT 1 OFFSET 1",
                f"SELECT * FROM {table} WHERE ts > '2024-01-02' LIMIT 1",
                f"SELECT * FROM {table} WHERE reading > 10", f"SELECT COUNT(*) FROM {table}",
            ):
                tdSql.error(sql, expectedErrno=0x6219)
        tdSql.execute("CREATE VTABLE v_valid (ts TIMESTAMP, reading BIGINT FROM r.reading WHERE row_id = 4) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_valid", [(13,)])
        tdSql.execute("CREATE VTABLE c_valid (reading FROM r.reading WHERE row_id = 4) USING vst_null TAGS (2) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM c_valid", [(13,)])
        self.check_rows("SELECT reading FROM vst_null WHERE group_id = 2", [(13,)])
        print("reference NULL timestamps and filtering order ............ [ passed ]")

    def do_duplicate_timestamps_and_limits(self):
        path = self.reference_path("invalid_times")
        tdSql.execute("CREATE VTABLE v_dup (ts TIMESTAMP, reading BIGINT FROM r.reading WHERE row_id > 1) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        tdSql.execute("CREATE STABLE vst_dup (ts TIMESTAMP, reading BIGINT) TAGS (group_id INT) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE c_dup (reading FROM r.reading WHERE row_id > 1) USING vst_dup TAGS (1) "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        for table in ("v_dup", "c_dup", "vst_dup"):
            for attempt in range(2):
                tdSql.query(f"SELECT ts, reading FROM {table} LIMIT 1")
                tdSql.checkRows(1)
                assert tdSql.getData(0, 1) in (11, 12)
                for sql in (f"SELECT * FROM {table} LIMIT 2", f"SELECT * FROM {table} LIMIT 1 OFFSET 1",
                            f"SELECT COUNT(*) FROM {table} LIMIT 1", f"SELECT * FROM {table} WHERE reading > 12"):
                    tdSql.error(sql, expectedErrno=0x621A)
            self.check_rows(f"SELECT reading FROM {table} WHERE ts > '2024-01-01 00:00:00'", [(13,)])
        print("reference duplicate timestamps and consumed LIMIT prefixes ............ [ passed ]")

    def do_windows_and_reference_chains(self):
        self.create_readings_vtable(time_column="collected_time")
        tdSql.execute("CREATE VTABLE v_chain (ts TIMESTAMP, reading DOUBLE FROM v.reading)")
        tdSql.execute("CREATE VTABLE v_chain2 (ts TIMESTAMP, reading DOUBLE FROM v_chain.reading)")
        self.check_rows("SELECT CAST(ts AS BIGINT), reading FROM v_chain2 ORDER BY ts",
                        [(_BASE_US + 500000, 1), (_BASE_US + 1500000, None), (_BASE_US + 3500000, 3)])
        self.check_rows("SELECT COUNT(*), SUM(reading) FROM v_chain2 INTERVAL(1s)",
                        [(1, 1), (1, None), (1, 3)])
        self.check_rows("SELECT reading FROM v_chain2 WHERE reading >= 2 ORDER BY ts DESC LIMIT 1", [(3,)])
        print("reference ordinary windows, value predicates and multi-hop chains ............ [ passed ]")

    def do_precision_and_timezone(self):
        precise_type = {"mysql": "DATETIME(6)", "postgres": "TIMESTAMP(6)",
                        "sqlserver": "DATETIME2(6)", "oracle": "TIMESTAMP(6)"}[self.backend]
        self.create_table("precise", f"event_time {precise_type}, reading {self.float_type}",
                          [f"({self.time_literal(123, 456)}, 8)"])
        path = self.reference_path("precise")
        small_db = f"{self.local_db}_ms"
        tdSql.execute(f"CREATE DATABASE {small_db} PRECISION 'ms'")
        try:
            tdSql.error(f"CREATE VTABLE {small_db}.v (ts TIMESTAMP, reading DOUBLE FROM r.reading) "
                        f"REFERENCE r AS {path} TIMESTAMP event_time")
            tdSql.execute(f"CREATE STABLE {small_db}.vst (ts TIMESTAMP, reading DOUBLE, value_time TIMESTAMP) "
                          "TAGS (group_id INT) VIRTUAL 1")
            tdSql.error(f"CREATE VTABLE {small_db}.c (reading FROM r.reading, value_time FROM r.event_time) "
                        f"USING {small_db}.vst TAGS (1) REFERENCE r AS {path} TIMESTAMP event_time")
            tdSql.execute("CREATE VTABLE v_precise (ts TIMESTAMP, reading DOUBLE FROM r.reading, "
                          "value_time TIMESTAMP FROM r.event_time) "
                          f"REFERENCE r AS {path} TIMESTAMP event_time")
            tdSql.execute("CREATE STABLE vst_precise (ts TIMESTAMP, reading DOUBLE, value_time TIMESTAMP) "
                          "TAGS (group_id INT) VIRTUAL 1")
            tdSql.execute("CREATE VTABLE c_precise (reading FROM r.reading, value_time FROM r.event_time) "
                          f"USING vst_precise TAGS (1) REFERENCE r AS {path} TIMESTAMP event_time")
            for table in ("v_precise", "c_precise", "vst_precise"):
                self.check_rows(f"SELECT CAST(ts AS BIGINT), CAST(value_time AS BIGINT), reading FROM {table}",
                                [(_BASE_US + 123456, _BASE_US + 123456, 8)])
            tdSql.execute('ALTER LOCAL "timezone" "Asia/Shanghai"')
            for table in ("v_precise", "c_precise", "vst_precise"):
                self.check_rows(f"SELECT CAST(ts AS BIGINT), CAST(value_time AS BIGINT) FROM {table}",
                                [(_BASE_US + 123456 - 8 * 3600000000, _BASE_US + 123456 - 8 * 3600000000)])
        finally:
            tdSql.execute('ALTER LOCAL "timezone" "UTC"')
            tdSql.execute(f"DROP DATABASE IF EXISTS {small_db}")
        print("reference declared precision and client timezone conversion ............ [ passed ]")

    def do_namespace_binding(self):
        path = self.reference_path("readings_view")
        tdSql.execute("CREATE VTABLE v_namespace (ts TIMESTAMP, reading DOUBLE FROM r.reading WHERE device = 'd1') "
                      f"REFERENCE r AS {path} TIMESTAMP event_time")
        other_db = f"{self.local_db}_use"
        tdSql.execute(f"CREATE DATABASE {other_db}")
        try:
            tdSql.execute(f"USE {other_db}")
            self.check_rows(f"SELECT reading FROM {self.local_db}.v_namespace ORDER BY ts", [(1,), (None,), (3,)])
        finally:
            tdSql.execute(f"USE {self.local_db}")
            tdSql.execute(f"DROP DATABASE {other_db}")
        if self.backend in ("postgres", "sqlserver"):
            schema = "vreference_alt"
            if self.backend == "postgres":
                self.remote_execute([f"CREATE SCHEMA IF NOT EXISTS {schema}"])
            else:
                self.remote_execute([f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA {schema}')"])
            self.create_view("readings_view", f"SELECT {self.quote('event_time')}, {self.quote('device')}, "
                             f"{self.quote('reading')} + 100 AS {self.quote('reading')} "
                             f"FROM {self.remote_object('readings')}", schema)
            tdSql.execute("CREATE VTABLE v_schemas (ts TIMESTAMP, a DOUBLE FROM r1.reading WHERE device = 'd1', "
                          "b DOUBLE FROM r2.reading WHERE device = 'd1') "
                          f"REFERENCE r1 AS {path} TIMESTAMP event_time "
                          f"REFERENCE r2 AS {self.reference_path('readings_view', schema)} TIMESTAMP event_time")
            self.check_rows("SELECT a, b FROM v_schemas ORDER BY ts", [(1, 101), (None, None), (3, 103)])
        print("reference complete namespace identity and USE stability ............ [ passed ]")

    def do_remote_schema_changes(self):
        columns = f"event_time {self.time_type}, device {self.text_type}, reading {self.float_type}"
        rows = [f"({self.time_literal(0)}, 'd1', 9)"]
        self.create_table("evolve", columns, rows)
        path = self.reference_path("evolve")
        tdSql.execute("CREATE VTABLE v_evolve (ts TIMESTAMP, reading DOUBLE FROM r.reading WHERE device = 'd1') "
                      f"REFERENCE r AS {path} TIMESTAMP event_time REFERENCE unused AS {path} TIMESTAMP event_time")
        self.check_rows("SELECT reading FROM v_evolve", [(9,)])
        self.remote_execute([f"ALTER TABLE {self.remote_object('evolve')} DROP COLUMN {self.quote('reading')}"])
        tdSql.error("SELECT reading FROM v_evolve")
        rows = self.validation_rows("v_evolve")
        assert any(row["reference_alias"] == "r" and row["err_code"] != 0 for row in rows), rows
        self.remote_execute([f"ALTER TABLE {self.remote_object('evolve')} DROP COLUMN {self.quote('event_time')}"])
        rows = self.validation_rows("v_evolve")
        assert any(row["reference_alias"] == "unused" and row["err_code"] != 0 for row in rows), rows
        self.create_table("evolve", columns, [f"({self.time_literal(0)}, 'd1', 19)"])
        self.check_rows("SELECT reading FROM v_evolve", [(19,)])
        print("reference current remote schema validation and recovery ............ [ passed ]")

    def do_definition_errors(self):
        path = self.reference_path("readings_view")
        prefix = "CREATE VTABLE v_bad (ts TIMESTAMP, reading DOUBLE FROM r.reading) "
        for declaration in (
            f"REFERENCE r AS {path} TIMESTAMP missing_time",
            f"REFERENCE r AS {path} TIMESTAMP voltage",
            f"REFERENCE r AS {path} TIMESTAMP now()",
            f"REFERENCE r AS {path}",
            f"REFERENCE r AS {path} TIMESTAMP event_time WHERE device = 'd1'",
            f"REFERENCE r AS {path} TIMESTAMP event_time REFERENCE r AS {path} TIMESTAMP collected_time",
            f"REFERENCE r AS {self.reference_path('missing_object')} TIMESTAMP event_time",
        ):
            tdSql.error(prefix + declaration)
        tdSql.error("CREATE VTABLE v_bad (ts TIMESTAMP, reading DOUBLE FROM missing.reading) "
                    f"REFERENCE r AS {path} TIMESTAMP event_time")
        tdSql.error("CREATE VTABLE v_bad (ts TIMESTAMP, reading DOUBLE FROM r.reading WHERE device = 'd1' OR voltage = 220) "
                    f"REFERENCE r AS {path} TIMESTAMP event_time")
        tdSql.execute("CREATE TABLE local_only (ts TIMESTAMP, reading DOUBLE)")
        tdSql.error(prefix + f"REFERENCE r AS {self.local_db}.local_only TIMESTAMP ts")
        self.create_readings_vtable()
        tdSql.error(f"ALTER VTABLE v ADD REFERENCE r AS {path} TIMESTAMP event_time")
        tdSql.error(f"ALTER VTABLE v MODIFY REFERENCE absent AS {path} TIMESTAMP event_time")
        tdSql.error("ALTER VTABLE v REMOVE REFERENCE absent")
        tdSql.execute("CREATE STABLE influx_rejection (ts TIMESTAMP, reading DOUBLE) TAGS (group_id INT) VIRTUAL 1")
        for version in ("1.8", "3"):
            influx_source = f"{self.source}_influx{version.replace('.', '')}"
            try:
                tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {influx_source}")
                # Omitting DATABASE keeps metadata prefetch local. Verify the
                # unsupported-provider error code at every DDL entry point.
                tdSql.execute(
                    f"CREATE EXTERNAL SOURCE {influx_source} TYPE='influxdb' "
                    f"HOST='127.0.0.1' PORT=1 USER='unused' PASSWORD='unused' "
                    f"OPTIONS('version'='{version}')")
                tdSql.query("SELECT source_name, type, options FROM information_schema.ins_ext_sources "
                            f"WHERE source_name = '{influx_source}'")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, influx_source)
                tdSql.checkData(0, 1, "influxdb")
                assert json.loads(tdSql.getData(0, 2))["version"] == version
                declaration = f"REFERENCE inf AS {influx_source}.metrics TIMESTAMP time"
                for sql in (
                    "CREATE VTABLE v_bad_influx (ts TIMESTAMP, reading DOUBLE FROM inf.reading) " + declaration,
                    "CREATE VTABLE v_unused_influx (ts TIMESTAMP, reading DOUBLE) " + declaration,
                    "CREATE VTABLE c_bad_influx (reading FROM inf.reading) "
                    "USING influx_rejection TAGS (1) " + declaration,
                    "ALTER VTABLE v ADD " + declaration,
                    f"ALTER VTABLE v MODIFY REFERENCE r AS {influx_source}.metrics TIMESTAMP time",
                ):
                    tdSql.error(sql, expectedErrno=0x6407)
                definition = extract_show_create_sql("SHOW CREATE VTABLE v")
                assert influx_source not in definition, definition
            finally:
                tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {influx_source}")
        self.check_rows("SELECT reading FROM v ORDER BY ts", [(1,), (None,), (3,)])
        print("reference invalid syntax, names, columns and definitions ............ [ passed ]")

    def do_declaration_limit(self):
        path = self.reference_path("readings_view")
        declarations = " ".join(f"REFERENCE r{i} AS {path} TIMESTAMP event_time" for i in range(128))
        tdSql.execute(f"CREATE VTABLE v_limit (ts TIMESTAMP, reading DOUBLE) {declarations}")
        definition = extract_show_create_sql("SHOW CREATE VTABLE v_limit")
        assert definition.upper().count("REFERENCE ") == 128
        tdSql.error(f"ALTER VTABLE v_limit ADD REFERENCE r128 AS {path} TIMESTAMP event_time")
        tdSql.execute("ALTER VTABLE v_limit REMOVE REFERENCE r127")
        tdSql.execute(f"ALTER VTABLE v_limit ADD REFERENCE r128 AS {path} TIMESTAMP event_time")
        tdSql.error(f"CREATE VTABLE v_overflow (ts TIMESTAMP, reading DOUBLE) {declarations} "
                    f"REFERENCE r128 AS {path} TIMESTAMP event_time")
        print("reference declaration limit and released capacity ............ [ passed ]")

    def do_drop_and_recreate(self):
        self.create_readings_vtable()
        tdSql.execute("DROP VTABLE v")
        self.create_readings_vtable(time_column="collected_time")
        self.check_rows("SELECT CAST(ts AS BIGINT), reading FROM v ORDER BY ts",
                        [(_BASE_US + 500000, 1), (_BASE_US + 1500000, None), (_BASE_US + 3500000, 3)])
        print("reference table drop and recreated binding lifetime ............ [ passed ]")

    def do_large_input(self):
        rows = 8200
        self.create_table("large_readings", f"row_id {self.integer_type} PRIMARY KEY, "
                          f"event_time {self.time_type}, reading {self.integer_type}")
        target = self.remote_object("large_readings")
        for start in range(0, rows, 200):
            values = [f"({i}, {self.time_literal(i)}, {i})" for i in range(start, min(start + 200, rows))]
            if self.backend == "oracle":
                statement = ("INSERT ALL\n" + "\n".join(f"INTO {target} VALUES {value}" for value in values)
                             + "\nSELECT 1 FROM dual")
                self.remote_execute([statement, "COMMIT"])
            else:
                self.remote_execute([f"INSERT INTO {target} VALUES " + ", ".join(values)])
        with etool.data_write_log():
            tdSql.execute("CREATE TABLE local_bulk (ts TIMESTAMP, reading BIGINT)")
            values = " ".join(f"({_BASE_US + i * 1000}, {i})" for i in range(rows))
            tdSql.execute("INSERT INTO local_bulk VALUES " + values)
        tdSql.query("SELECT COUNT(*), SUM(reading) FROM local_bulk")
        baseline = list(tdSql.queryResult)
        tdSql.execute("CREATE VTABLE v_bulk (ts TIMESTAMP, reading BIGINT FROM r.reading) "
                      f"REFERENCE r AS {self.reference_path('large_readings')} TIMESTAMP event_time")
        tdSql.execute("CREATE STABLE vst_bulk (ts TIMESTAMP, reading BIGINT) TAGS (group_id INT) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE c_bulk (reading FROM r.reading) USING vst_bulk TAGS (1) "
                      f"REFERENCE r AS {self.reference_path('large_readings')} TIMESTAMP event_time")
        for table in ("v_bulk", "c_bulk", "vst_bulk"):
            self.check_rows(f"SELECT COUNT(*), SUM(reading) FROM {table}", baseline)
            self.check_rows(f"SELECT reading FROM {table} LIMIT 10 OFFSET 8180", [(i,) for i in range(8180, 8190)])
        self.remote_execute([f"UPDATE {target} SET {self.quote('event_time')} = {self.time_literal(4095)} "
                             f"WHERE {self.quote('row_id')} = 4096"] + (["COMMIT"] if self.backend == "oracle" else []))
        for table in ("v_bulk", "c_bulk", "vst_bulk"):
            tdSql.error(f"SELECT COUNT(*) FROM {table}", expectedErrno=0x621A)
            tdSql.query(f"SELECT reading FROM {table} LIMIT 4096")
            tdSql.checkRows(4096)
            tdSql.checkData(0, 0, 0)
            tdSql.error(f"SELECT reading FROM {table} LIMIT 4097", expectedErrno=0x621A)
        print("reference multi-fetch input and late duplicate detection ............ [ passed ]")

    def do_mixed_external_sources(self, influx_source, bucket):
        peer = type(self)()
        peer.configure_backend({"mysql": "postgres", "postgres": "sqlserver",
                                "sqlserver": "oracle", "oracle": "mysql"}[self.backend])
        VtableQueryUtil._EXT_SOURCE_ADAPTERS[peer.backend]().ensure_running()
        if peer.backend not in self.prepared_backends:
            create_remote_db(peer.source_type(), peer.remote_db)
            self.prepared_backends.add(peer.backend)
            peer.prepare_remote_data()
        peer.source += "_peer"
        timestamp = _BASE_US * 1000
        second = 1000000000
        path_a = self.reference_path("readings_view")
        path_b = peer.reference_path("mixed_peer")
        direct = self.reference_path("time_key") + ".reading WHERE reading >= 20"
        try:
            peer.create_table(
                "mixed_peer", f"row_id {peer.integer_type} PRIMARY KEY, device {peer.text_type}, "
                f"event_time {peer.time_type}, collected_time {peer.time_type}, reading {peer.float_type}",
                [f"(1, 'd2', {peer.time_literal(0)}, {peer.time_literal(750)}, 100)",
                 f"(2, 'd2', {peer.time_literal(2000)}, {peer.time_literal(2750)}, 200)",
                 f"(3, 'd1', {peer.time_literal(0)}, {peer.time_literal(750)}, 1000)",
                 f"(4, 'd1', {peer.time_literal(2000)}, {peer.time_literal(2750)}, 2000)"])
            database = ExtSrcEnv.ORACLE_SERVICE if peer.backend == "oracle" else peer.remote_db
            create_ext_source(peer.source, peer.source_type(), database)
            for layout in ("one", "many"):
                declarations = [f"REFERENCE ra AS {path_a} TIMESTAMP event_time",
                                f"REFERENCE rb AS {path_b} TIMESTAMP event_time"]
                groups = [
                    [("a", "ra.reading WHERE device = 'd1' AND voltage >= 220",
                      {timestamp: 1, timestamp + second: None, timestamp + 3 * second: 3}),
                     ("a_other", "ra.reading WHERE device = 'd2' AND reading >= 20", {timestamp + 2 * second: 20}),
                     ("b", "rb.reading WHERE device = 'd2' AND reading >= 150", {timestamp + 2 * second: 200}),
                     ("direct_value", direct, {timestamp + second: 20, timestamp + 2 * second: 30}),
                     ("cpu", "s.value", {timestamp: 10, timestamp + second: 20, timestamp + 2 * second: 30})],
                    [("a", "ra.reading WHERE device = 'd2' AND voltage >= 230",
                      {timestamp: 10, timestamp + 2 * second: 20}),
                     ("a_other", "ra.reading WHERE device = 'd1' AND reading >= 2", {timestamp + 3 * second: 3}),
                     ("b", "rb.reading WHERE device = 'd1' AND reading >= 1500", {timestamp + 2 * second: 2000}),
                     ("direct_value", direct, {timestamp + second: 20, timestamp + 2 * second: 30}),
                     ("cpu", "s.value", {timestamp: 100, timestamp + 3 * second: 400})],
                ]
                if layout == "many":
                    declarations.extend([f"REFERENCE ra_shift AS {path_a} TIMESTAMP collected_time",
                                         f"REFERENCE rb_shift AS {path_b} TIMESTAMP collected_time"])
                    groups[0].extend([
                        ("a_shift", "ra_shift.reading WHERE device = 'd1'",
                         {timestamp + second // 2: 1, timestamp + 3 * second // 2: None,
                          timestamp + 7 * second // 2: 3}),
                        ("b_shift", "rb_shift.reading WHERE device = 'd2'",
                         {timestamp + 3 * second // 4: 100, timestamp + 11 * second // 4: 200}),
                    ])
                    groups[1].extend([
                        ("a_shift", "ra_shift.reading WHERE device = 'd2'",
                         {timestamp + second // 2: 10, timestamp + 5 * second // 2: 20}),
                        ("b_shift", "rb_shift.reading WHERE device = 'd1'",
                         {timestamp + 3 * second // 4: 1000, timestamp + 11 * second // 4: 2000}),
                    ])
                table = f"v_sources_{layout}"
                stable = f"vst_sources_{layout}"
                child = f"c_sources_{layout}"
                other_child = f"c_other_{layout}"
                series = [f"SERIES s AS {influx_source}.{bucket}.mixed_metrics (device='{device}')"
                          for device in ("d1", "d2")]
                reference_sql = " ".join(declarations)
                columns = ", ".join(f"{name} DOUBLE FROM {expression}" for name, expression, sequence in groups[0])
                tdSql.execute(f"CREATE VTABLE {table} (ts TIMESTAMP, {columns}) {reference_sql} {series[0]}")
                schema = ", ".join(f"{name} DOUBLE" for name, expression, sequence in groups[0])
                tdSql.execute(f"CREATE STABLE {stable} (ts TIMESTAMP, {schema}) TAGS (group_id INT) VIRTUAL 1")
                bindings = [", ".join(f"{name} FROM {expression}" for name, expression, sequence in group)
                            for group in groups]
                tdSql.execute(
                    f"CREATE VTABLE {child} ({bindings[0]}) USING {stable} TAGS (1) {reference_sql} {series[0]} "
                    f"{other_child} ({bindings[1]}) USING {stable} TAGS (2) {series[1]} {reference_sql}")
                expected = [self.aligned_rows(group) for group in groups]
                names = [name for name, expression, sequence in groups[0]]
                projection = "CAST(ts AS BIGINT), " + ", ".join(names)
                cpu_slot = names.index("cpu") + 1
                aggregates = ["COUNT(*)"]
                aggregate_values = [len(expected[0])]
                for name, expression, sequence in groups[0]:
                    values = [value for value in sequence.values() if value is not None]
                    aggregates.extend([f"COUNT({name})", f"SUM({name})"])
                    aggregate_values.extend([len(values), sum(values)])
                windows = {}
                for row in expected[0]:
                    start = row[0] - row[0] % second
                    windows.setdefault(start, []).append(row)
                expected_windows = []
                for start, rows in sorted(windows.items()):
                    values = [start, len(rows)]
                    for name in names:
                        slot = names.index(name) + 1
                        present = [row[slot] for row in rows if row[slot] is not None]
                        values.append(sum(present) if present else None)
                    expected_windows.append(tuple(values))
                for target, condition in ((table, ""), (child, ""), (stable, "group_id = 1")):
                    source = f"FROM {target}"
                    selected = source + (f" WHERE {condition}" if condition else "")
                    self.check_rows(f"SELECT {projection} {selected} ORDER BY ts", expected[0])
                    self.check_rows(f"SELECT {', '.join(aggregates)} {selected}", [tuple(aggregate_values)])
                    self.check_rows(
                        "SELECT CAST(_wstart AS BIGINT), COUNT(*), "
                        + ", ".join(f"SUM({name})" for name in names)
                        + f" {selected} INTERVAL(1s)",
                        expected_windows)
                    self.check_rows(f"SELECT {projection} {selected} ORDER BY ts LIMIT 2 OFFSET 1", expected[0][1:3])
                    self.check_rows(f"SELECT {projection} {selected} ORDER BY ts DESC LIMIT 2",
                                    list(reversed(expected[0][-2:])))
                    value_condition = f"{condition} AND cpu >= 30" if condition else "cpu >= 30"
                    self.check_rows(f"SELECT {projection} {source} WHERE {value_condition} ORDER BY ts",
                                    [row for row in expected[0] if row[cpu_slot] is not None and row[cpu_slot] >= 30])
                    time_condition = f"ts >= {timestamp + second} AND ts < {timestamp + 3 * second}"
                    if condition:
                        time_condition = f"{condition} AND {time_condition}"
                    self.check_rows(f"SELECT {projection} {source} WHERE {time_condition} ORDER BY ts",
                                    [row for row in expected[0] if timestamp + second <= row[0] < timestamp + 3 * second])
                    sequence = groups[0][0][2]
                    self.check_rows(f"SELECT CAST(ts AS BIGINT), a {selected} ORDER BY ts",
                                    sorted(sequence.items()))
                self.check_rows(f"SELECT {projection} FROM {other_child} ORDER BY ts", expected[1])
                self.check_rows(f"SELECT {projection} FROM {stable} WHERE group_id = 2 ORDER BY ts", expected[1])
                grouped_values = []
                for group_id, group in enumerate(groups, 1):
                    totals = [sum(value for value in sequence.values() if value is not None)
                              for name, expression, sequence in group]
                    grouped_values.append((group_id, len(expected[group_id - 1]), *totals))
                self.check_rows(
                    "SELECT group_id, COUNT(*), " + ", ".join(f"SUM({name})" for name in names)
                    + f" FROM {stable} GROUP BY group_id ORDER BY group_id", grouped_values)
                definition = extract_show_create_sql(f"SHOW CREATE VTABLE {table}")
                assert definition.upper().count("REFERENCE ") == len(declarations), definition
                replay = table + "_replayed"
                tdSql.execute(definition.replace(f"`{table}`", f"`{replay}`", 1))
                self.check_rows(f"SELECT {projection} FROM {replay} ORDER BY ts", expected[0])
                definition = extract_show_create_sql(f"SHOW CREATE VTABLE {child}")
                child_replay = child + "_replayed"
                tdSql.execute(definition.replace(f"`{child}`", f"`{child_replay}`", 1))
                self.check_rows(f"SELECT {projection} FROM {child_replay} ORDER BY ts", expected[0])
                self.check_rows(f"SELECT {projection} FROM {stable} WHERE tbname = '{child_replay}' ORDER BY ts",
                                expected[0])
                tdSql.execute(f"DROP VTABLE {child_replay}")
                tdSql.execute(f"ALTER VTABLE {child} MODIFY REFERENCE ra AS {path_a} TIMESTAMP collected_time")
                shifted = []
                for name, expression, sequence in groups[0]:
                    if expression.startswith("ra."):
                        sequence = {point + second // 2: value for point, value in sequence.items()}
                    shifted.append((name, expression, sequence))
                self.check_rows(f"SELECT {projection} FROM {child} ORDER BY ts", self.aligned_rows(shifted))
                self.check_rows(f"SELECT {projection} FROM {stable} WHERE group_id = 1 ORDER BY ts",
                                self.aligned_rows(shifted))
                self.check_rows(f"SELECT {projection} FROM {stable} WHERE group_id = 2 ORDER BY ts", expected[1])
        finally:
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {peer.source}")

    def do_mixed_series_and_reference(self, influx_version):
        mixed_db = f"{self.local_db}_mixed"
        bucket = f"vreference_mix_{self.backend}"
        source = f"{self.source}_mix{influx_version.replace('.', '')}"
        adapter = VtableQueryUtil._EXT_SOURCE_ADAPTERS["influx"](
            influx_version, source_names=(source,))
        adapter.DB_NAMES = (bucket,)
        adapter.ensure_running()
        cfg = adapter.config
        timestamp = _BASE_US * 1000
        second = 1000000000
        try:
            ExtSrcEnv.influx_drop_db_cfg(cfg, bucket)
            ExtSrcEnv.influx_create_db_cfg(cfg, bucket)
            ExtSrcEnv.influx_write_cfg(cfg, bucket, [
                f"mixed_metrics,device=d1 value=10.0 {timestamp}",
                f"mixed_metrics,device=d1 value=20.0 {timestamp + second}",
                f"mixed_metrics,device=d1 value=30.0 {timestamp + 2 * second}",
                f"mixed_metrics,device=d2 value=100.0 {timestamp}",
                f"mixed_metrics,device=d2 value=400.0 {timestamp + 3 * second}",
            ])
            for attempt in range(30):
                if adapter._row_count(bucket, "mixed_metrics") == 5:
                    break
                time.sleep(1)
            else:
                raise AssertionError(f"InfluxDB {influx_version} mixed input did not become readable")
            adapter.create_external_source()
            tdSql.execute(f"DROP DATABASE IF EXISTS {mixed_db}")
            tdSql.execute(f"CREATE DATABASE {mixed_db} PRECISION 'ns'")
            tdSql.execute(f"USE {mixed_db}")
            path = self.reference_path("readings_view")
            reference = f"REFERENCE r AS {path} TIMESTAMP event_time"
            series = f"SERIES s AS {source}.{bucket}.mixed_metrics (device='d1')"
            expected = [(timestamp, 1, 10), (timestamp + second, None, 20),
                        (timestamp + 2 * second, None, 30), (timestamp + 3 * second, 3, None)]
            for name, declarations in (
                ("v_ref_first", reference + " " + series),
                ("v_series_first", series + " " + reference),
            ):
                tdSql.execute(
                    f"CREATE VTABLE {name} (ts TIMESTAMP, "
                    "reading DOUBLE FROM r.reading WHERE device = 'd1', "
                    f"cpu DOUBLE FROM s.value) {declarations}")
                self.check_rows(f"SELECT CAST(ts AS BIGINT), reading, cpu FROM {name} ORDER BY ts", expected)
                self.check_rows(f"SELECT CAST(ts AS BIGINT), reading FROM {name} ORDER BY ts",
                                [(timestamp, 1), (timestamp + second, None), (timestamp + 3 * second, 3)])
                self.check_rows(f"SELECT CAST(ts AS BIGINT), cpu FROM {name} ORDER BY ts",
                                [(timestamp, 10), (timestamp + second, 20), (timestamp + 2 * second, 30)])
                self.check_rows(
                    f"SELECT COUNT(*), COUNT(reading), SUM(reading), COUNT(cpu), SUM(cpu), "
                    f"SUM(reading + cpu) FROM {name}", [(4, 2, 4, 3, 60, 11)])
                self.check_rows(
                    f"SELECT CAST(ts AS BIGINT), reading, cpu FROM {name} "
                    "WHERE reading >= 3 OR cpu >= 30 ORDER BY ts", expected[2:])
                self.check_rows(
                    f"SELECT CAST(ts AS BIGINT), reading, cpu FROM {name} "
                    "WHERE ts >= '2024-01-01 00:00:01' AND ts < '2024-01-01 00:00:03' ORDER BY ts",
                    expected[1:3])
                self.check_rows(
                    f"SELECT CAST(ts AS BIGINT), reading, cpu FROM {name} ORDER BY ts LIMIT 2 OFFSET 1",
                    expected[1:3])
                self.check_rows(
                    f"SELECT CAST(ts AS BIGINT), reading, cpu FROM {name} ORDER BY ts DESC LIMIT 2",
                    list(reversed(expected[2:])))
                definition = extract_show_create_sql(f"SHOW CREATE VTABLE {name}")
                assert definition.upper().count("REFERENCE ") == 1, definition
                assert definition.upper().count("SERIES ") == 1, definition
                assert (definition.upper().index("REFERENCE ") < definition.upper().index("SERIES ")) == (
                    name == "v_ref_first"), definition
            definition = extract_show_create_sql("SHOW CREATE VTABLE v_ref_first")
            tdSql.execute(definition.replace("`v_ref_first`", "`v_replayed`", 1))
            self.check_rows("SELECT CAST(ts AS BIGINT), reading, cpu FROM v_replayed ORDER BY ts", expected)
            tdSql.execute(f"ALTER VTABLE v_ref_first MODIFY REFERENCE r AS {path} TIMESTAMP collected_time")
            self.check_rows(
                "SELECT CAST(ts AS BIGINT), reading, cpu FROM v_ref_first ORDER BY ts",
                [(timestamp, None, 10), (timestamp + second // 2, 1, None),
                 (timestamp + second, None, 20), (timestamp + 3 * second // 2, None, None),
                 (timestamp + 2 * second, None, 30), (timestamp + 7 * second // 2, 3, None)])
            self.check_rows("SELECT CAST(ts AS BIGINT), cpu FROM v_ref_first ORDER BY ts",
                            [(timestamp, 10), (timestamp + second, 20), (timestamp + 2 * second, 30)])
            tdSql.execute("CREATE STABLE mixed_stable (ts TIMESTAMP, reading DOUBLE, cpu DOUBLE) "
                          "TAGS (group_id INT) VIRTUAL 1")
            tdSql.execute(
                "CREATE VTABLE mixed_c1 (reading FROM r.reading WHERE device = 'd1', cpu FROM s.value) "
                f"USING mixed_stable TAGS (1) {reference} {series} "
                "mixed_c2 (reading FROM r.reading WHERE device = 'd2', cpu FROM s.value) "
                f"USING mixed_stable TAGS (2) "
                f"SERIES s AS {source}.{bucket}.mixed_metrics (device='d2') {reference}")
            self.check_rows("SELECT CAST(ts AS BIGINT), reading, cpu FROM mixed_c1 ORDER BY ts", expected)
            self.check_rows(
                "SELECT group_id, COUNT(*), COUNT(reading), SUM(reading), COUNT(cpu), SUM(cpu) "
                "FROM mixed_stable GROUP BY group_id ORDER BY group_id",
                [(1, 4, 2, 4, 3, 60), (2, 3, 2, 30, 2, 500)])
            self.check_rows(
                "SELECT CAST(ts AS BIGINT), reading, cpu FROM mixed_stable WHERE group_id = 1 ORDER BY ts",
                expected)
            self.check_rows(
                "SELECT CAST(ts AS BIGINT), reading, cpu FROM mixed_stable WHERE group_id = 2 ORDER BY ts",
                [(timestamp, 10, 100), (timestamp + 2 * second, 20, None),
                 (timestamp + 3 * second, None, 400)])
            self.do_mixed_external_sources(source, bucket)
        finally:
            try:
                tdSql.execute(f"USE {self.local_db}")
                tdSql.execute(f"DROP DATABASE IF EXISTS {mixed_db}")
            finally:
                try:
                    tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {source}")
                finally:
                    ExtSrcEnv.influx_drop_db_cfg(cfg, bucket)
        print(f"reference mixed with SERIES on InfluxDB {influx_version} ............ [ passed ]")

    # --- main ---

    def test_mixed_direct_reference_axes(self, backend):
        """Keep direct and explicit time axes separate on the same source object

        Verify both column orders and timestamp-only, full and single-column projections on normal, child and supertables.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-16 Joey Sima Cover mixed direct and REFERENCE time axes.
        """
        self.do_mixed_direct_reference_axes()

    def test_validate_column_case(self, backend):
        """Match REFERENCE validation column names using source identifier rules

        Verify valid case-folded bindings on MySQL and SQL Server and exact bindings on PostgreSQL and Oracle.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-16 Joey Sima Align validation and query column-name matching.
        """
        self.do_validate_column_case()

    def test_validate_wide_reference_table(self, backend):
        """Validate wide tables with repeated REFERENCE objects

        Verify 128 dependent columns, distinct objects and an unused declaration across repeated validation calls.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-16 Joey Sima Cover validation metadata reuse and request lifetime.
        """
        self.do_validate_wide_reference_table()

    def test_create_table_and_view(self, backend):
        """Create reference bindings to relational tables and views

        Verify ordinary time columns on integer-primary-key tables, views, and tables without primary keys.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_create_table_and_view()

    def test_selected_axis_and_value_columns(self, backend):
        """Select a time axis independently of the source time primary key

        Verify FIRST, LAST, ordering, offsets, and simultaneous ordinary timestamp value columns.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_selected_axis_and_value_columns()

    def test_sparse_alignment(self, backend):
        """Align independently filtered references and explicit time axes

        Verify sparse unions, NULL values, local aggregates, and projection-dependent timestamp generation.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_sparse_alignment()

    def test_add_reference_to_existing_table(self, backend):
        """Add reference declarations to existing virtual tables

        Verify explicit rebinding, dependency protection, unbinding, and return to a local source.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_add_reference_to_existing_table()

    def test_modify_reference_atomically(self, backend):
        """Modify all dependent reference columns atomically

        Verify selected-time changes and rejection of missing value columns, missing filter columns, and incompatible types.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_modify_reference_atomically()

    def test_column_binding_lifecycle(self, backend):
        """Maintain reference column bindings and alias dependencies

        Verify ADD COLUMN, SET, SET NULL, DROP COLUMN, and independent removal of same-object aliases.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_column_binding_lifecycle()

    def test_display_validate_and_replay(self, backend):
        """Display and replay complete reference definitions

        Verify DESCRIBE, system column sources, unused declarations, VALIDATE fields, and SHOW CREATE replay.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_display_validate_and_replay()

    def test_alias_resolution(self, backend):
        """Resolve reference aliases consistently across DDL entrypoints

        Verify exact aliases, fully qualified local paths, local-table precedence, case fallback, and exact removal.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_alias_resolution()

    def test_children_and_batches(self, backend):
        """Bind references in batched virtual children and dynamic queries

        Verify per-child aliases, owned and initially unbound columns, tag groups, and timestamp value mappings.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_children_and_batches()

    def test_null_timestamps(self, backend):
        """Reject consumed NULL timestamps before query filtering

        Verify ranges, value predicates, aggregates, LIMIT and OFFSET, and exclusion through saved column filters.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_null_timestamps()

    def test_duplicate_timestamps_and_limits(self, backend):
        """Validate duplicate timestamps within consumed query prefixes

        Verify LIMIT 1, LIMIT 2, local OFFSET, aggregate and value filters, time ranges, and query-state reset.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_duplicate_timestamps_and_limits()

    def test_windows_and_reference_chains(self, backend):
        """Query reference data through windows and virtual table chains

        Verify two-hop chains, interval aggregation, query predicates, and descending limited results.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_windows_and_reference_chains()

    def test_precision_and_timezone(self, backend):
        """Apply source precision and existing client timezone rules

        Verify declared microsecond time columns, millisecond admission rejection, and UTC to Asia/Shanghai conversion.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_precision_and_timezone()

    def test_namespace_binding(self, backend):
        """Resolve complete reference namespaces independently of USE

        Verify frozen object identities and same-named PostgreSQL and SQL Server views across schemas.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_namespace_binding()

    def test_remote_schema_changes(self, backend):
        """Validate references against changing remote schemas

        Verify removed value and time columns, unused declaration errors, and recovery after restoring the source.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_remote_schema_changes()

    def test_definition_errors(self, backend):
        """Reject invalid reference syntax and definitions

        Verify invalid definitions and ALTER targets, including InfluxDB 1.8/3.0 rejection for CREATE, ADD, and MODIFY.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
            - 2026-11-13 Joey Sima Cover InfluxDB REFERENCE rejection.
        """
        self.do_definition_errors()

    def test_declaration_limit(self, backend):
        """Enforce reference declaration capacity for zero-dependency aliases

        Verify 128 declarations, overflow rejection, and capacity release after removing an alias.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_declaration_limit()

    def test_drop_and_recreate(self, backend):
        """Release reference bindings when virtual tables are dropped

        Verify recreating the same virtual table name with a different selected time column.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_drop_and_recreate()

    def test_large_input(self, backend):
        """Validate large reference inputs and late duplicate timestamps

        Compare 8200 source rows with a local baseline and verify prefix limits around a late duplicate.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add four-provider REFERENCE coverage.
        """
        self.do_large_input()

    @pytest.mark.parametrize("influx_version", ExtSrcEnv.INFLUX_VERSIONS)
    def test_mixed_series_and_reference(self, backend, influx_version):
        """Query SERIES and REFERENCE bindings together in one virtual table

        Verify declaration order, column filters, SERIES and single/multiple references on two external sources, sparse alignment, scalar/window aggregates, limits, replay, and reference modification across normal, child, and supertable queries.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Joey Sima Add mixed SERIES and REFERENCE end-to-end coverage.
        """
        self.do_mixed_series_and_reference(influx_version)
