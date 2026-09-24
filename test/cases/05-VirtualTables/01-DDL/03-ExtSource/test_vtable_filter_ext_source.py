# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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
from new_test_framework.utils import tdDnodes

import os
import sys

import pytest

_VTABLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _VTABLE_DIR)

from vtable_util import ExtSrcEnv, VtableQueryUtil
from ext_source_helpers import (
    create_ext_source, create_mysql_table, create_pg_table, create_remote_db,
    extract_show_create_sql,
)


_LOCAL_DB = "vfilter_ddl"
_REMOTE_DB = "vfilter_ddl_ext"
_LONG_NOTE = "saved_filter_" * 600
_FILTER_NOT_SUPPORTED = 0x6213  # TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED
_INCOMPLETE_PK_FILTER = 0x6217  # TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER
_FILTER_SQL_FAILED = 0x6214  # TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED
_SCHEMA_CHANGED = 0x6413  # TSDB_CODE_EXT_SCHEMA_CHANGED
_INVALID_FILTER = 0x6215  # TSDB_CODE_VTABLE_INVALID_EXT_FILTER
_DUPLICATE_FILTER_COLUMN = 0x6216  # TSDB_CODE_VTABLE_DUPLICATE_EXT_FILTER_COLUMN
_COLUMN_NOT_EXIST = 0x641B  # TSDB_CODE_EXT_COLUMN_NOT_EXIST
_WRONG_VALUE_TYPE = 0x2605  # TSDB_CODE_PAR_WRONG_VALUE_TYPE


class TestVTableFilterExtSource:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @pytest.fixture(autouse=True)
    def prepare_backend(self, backend):
        VtableQueryUtil._EXT_SOURCE_ADAPTERS[backend]().ensure_running()
        ExtSrcEnv.ensure_qnode()
        tdSql.query("show local variables like 'timezone'")
        original_timezone = str(tdSql.getData(0, 1)).split(" ")[0]
        self.source = f"vfilter_ddl_{backend}"
        try:
            tdSql.execute('alter local "timezone" "UTC"')
            source_type = "postgresql" if backend == "postgres" else "mysql"
            create_remote_db(source_type, _REMOTE_DB)
            create_table = create_pg_table if backend == "postgres" else create_mysql_table
            timestamp_type = "timestamp(3)" if backend == "postgres" else "datetime(3)"
            create_table(
                _REMOTE_DB, "readings",
                f"ts {timestamp_type} not null, device varchar(32) not null, "
                "v int, note varchar(8192), primary key (ts, device)",
                ["('2024-01-01 00:00:00', 'd1', 10, '')",
                 f"('2024-01-01 00:00:01', 'd1', 11, '{_LONG_NOTE}')",
                 "('2024-01-01 00:00:00', 'd2', 20, '')",
                 "('2024-01-01 00:00:01', 'd2', 21, '')"],
            )
            create_table(
                _REMOTE_DB, "single_pk", f"ts {timestamp_type} not null primary key, v int",
                ["('2024-01-01 00:00:00', 100)", "('2024-01-01 00:00:01', 101)"],
            )
            create_ext_source(self.source, source_type, _REMOTE_DB)
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"create database {_LOCAL_DB} precision 'ms'")
            tdSql.execute(f"use {_LOCAL_DB}")
            yield
        finally:
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"drop external source if exists {self.source}")
            if backend == "postgres":
                ExtSrcEnv.pg_drop_db(_REMOTE_DB)
            else:
                ExtSrcEnv.mysql_drop_db(_REMOTE_DB)
            tdSql.execute(f'alter local "timezone" "{original_timezone}"')

    # --- util ---

    @staticmethod
    def check_result(table, columns, expected):
        tdSql.query(f"select {columns} from {_LOCAL_DB}.{table} order by ts")
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    def check_views(self, kind, expected):
        self.check_result("leaf", "a, b, c", expected)
        self.check_result("chain", "a, c", [(row[0], row[2]) for row in expected])
        if kind == "child":
            self.check_result("filter_stable", "a, b, c", expected)

    def check_roundtrip(self, table, filter_count, columns, expected):
        ddl = extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}")
        assert ddl.upper().count(" WHERE ") == filter_count, ddl
        copy = f"{table}_copy"
        tdSql.execute(ddl.replace(table, copy, 1))
        try:
            self.check_result(copy, columns, expected)
        finally:
            tdSql.execute(f"drop vtable {_LOCAL_DB}.{copy}")
        return ddl

    def remote_exec(self, backend, statements):
        execute = ExtSrcEnv.pg_exec if backend == "postgres" else ExtSrcEnv.mysql_exec
        execute(_REMOTE_DB, statements)

    def prepare_binding_views(self, filtered=None):
        if filtered is None:
            filtered = f"{self.source}.{_REMOTE_DB}.readings.v where device = 'd1' and note = ''"
        unfiltered = f"{self.source}.{_REMOTE_DB}.single_pk.v"
        tdSql.execute(f"create vtable leaf (ts timestamp, a int from {filtered}, b int from {unfiltered})")
        tdSql.execute("create stable filter_stable (ts timestamp, a int, b int) tags (site int) virtual 1")
        tdSql.execute(
            f"create vtable leaf_child (a from {filtered}, b from {unfiltered}) using filter_stable tags (1)"
        )
        for table in ("leaf", "leaf_child"):
            tdSql.execute(
                f"create vtable chain_{table} (ts timestamp, a int from {_LOCAL_DB}.{table}.a, "
                f"b int from {_LOCAL_DB}.{table}.b)"
            )
        tdSql.execute("create stable chain_stable (ts timestamp, a int, b int) tags (site int) virtual 1")
        tdSql.execute(
            f"create vtable chain_child (a from {_LOCAL_DB}.leaf_child.a, b from {_LOCAL_DB}.leaf_child.b) "
            "using chain_stable tags (1)"
        )
        return ("leaf", "chain_leaf", "leaf_child", "chain_leaf_child", "filter_stable",
                "chain_child", "chain_stable")

    def check_invalid_views(self, tables, column, error):
        tdSql.execute(f"refresh external source {self.source}")
        for table in tables:
            tdSql.error(f"select {column} from {_LOCAL_DB}.{table}", expectedErrno=error)

    def replace_primary_key(self, backend, table, columns):
        drop = "drop primary key" if backend == "mysql" else f"drop constraint {table}_pkey"
        self.remote_exec(backend, [f"alter table {table} {drop}",
                                   f"alter table {table} add primary key ({columns})"])

    # --- impl ---

    def do_filter_column_case(self, backend, kind):
        is_pg = backend == "postgres"
        create_table = create_pg_table if is_pg else create_mysql_table
        timestamp_type = "timestamp(3)" if is_pg else "datetime(3)"
        quote = '"' if is_pg else '`'
        lower_key = "device varchar(32) not null, " if is_pg else ""
        lower_pk = "device, " if is_pg else ""
        lower_value = "'d1', " if is_pg else ""
        rows = [f"('2024-01-01 00:00:00', {lower_value}'D1', 10, 'keep')",
                f"('2024-01-01 00:00:00', {lower_value}'D2', 20, 'keep')",
                f"('2024-01-01 00:00:01', {lower_value}'D1', 11, 'drop')"]
        if is_pg:
            rows.append("('2024-01-01 00:00:00', 'd2', 'D1', 30, 'keep')")
        create_table(
            _REMOTE_DB, "case_readings",
            f"ts {timestamp_type} not null, {lower_key}{quote}Device{quote} varchar(32) not null, "
            f"v int, {quote}Note{quote} varchar(32), primary key (ts, {lower_pk}{quote}Device{quote})",
            rows,
        )
        ref = f"{self.source}.{_REMOTE_DB}.case_readings.v"
        lower_filter = "device = 'd1' and " if is_pg else ""
        keys = f"{lower_filter}`Device` = 'D1'"
        condition = f"{keys} and `Note` = 'keep'"
        if kind == "normal":
            tdSql.execute(f"create vtable case_leaf (ts timestamp, a int from {ref} where {condition})")
        else:
            tdSql.execute("create stable case_stable (ts timestamp, a int) tags (site int) virtual 1")
            tdSql.execute(f"create vtable case_leaf (a from {ref} where {condition}) "
                          "using case_stable tags (1)")
        tdSql.execute(f"create vtable case_chain (ts timestamp, a int from {_LOCAL_DB}.case_leaf.a)")
        tables = ["case_leaf", "case_chain"]
        if kind == "child":
            tables.append("case_stable")
        for table in tables:
            self.check_result(table, "a", [(10,)])
        ddl = self.check_roundtrip("case_leaf", 1, "a", [(10,)])
        if is_pg:
            assert "`Device`" in ddl and "`Note`" in ddl, ddl
        else:
            # MySQL exposes lowercase column names in external metadata.
            assert "`device`" in ddl and "`note`" in ddl, ddl

        tdSql.error(f"alter vtable case_leaf alter column a set {ref} where {keys} and `Device` = 'D2'",
                    expectedErrno=_DUPLICATE_FILTER_COLUMN)
        if is_pg:
            # Unquoted names fold to lowercase; quoted names must match exactly.
            for name in ("Note", "`NOTE`"):
                tdSql.error(f"create vtable invalid_case (ts timestamp, a int from {ref} "
                            f"where {keys} and {name} = 'keep')", expectedErrno=_COLUMN_NOT_EXIST)
            tdSql.error(f"create vtable incomplete_case (ts timestamp, a int from {ref} where device = 'd1')",
                        expectedErrno=_INCOMPLETE_PK_FILTER)
        else:
            tdSql.error(f"create vtable duplicate_case (ts timestamp, a int from {ref} "
                        "where device = 'D1' and `DEVICE` = 'D2')", expectedErrno=_DUPLICATE_FILTER_COLUMN)
            tdSql.execute(f"alter vtable case_leaf alter column a set {ref} "
                          "where dEvIcE = 'D1' and `NOTE` = 'keep'")
        self.check_result("case_leaf", "a", [(10,)])
        tdSql.execute(f"alter vtable case_leaf alter column a set {ref} where {keys} and `Note` = 'drop'")
        for table in tables:
            self.check_result(table, "a", [(11,)])
        self.check_roundtrip("case_leaf", 1, "a", [(11,)])

        self.remote_exec(backend, [f"alter table case_readings rename column {quote}Note{quote} to {quote}NOTE{quote}"])
        tdSql.execute(f"refresh external source {self.source}")
        for table in tables:
            if is_pg:
                tdSql.error(f"select a from {_LOCAL_DB}.{table}", expectedErrno=_SCHEMA_CHANGED)
            else:
                self.check_result(table, "a", [(11,)])
        print("external-filter column case rules ........ [ passed ]")

    def do_alter_filter(self, kind):
        reference = f"{self.source}.{_REMOTE_DB}.readings.v"
        if kind == "normal":
            tdSql.execute(
                f"create vtable leaf (ts timestamp, a int from {reference} where device = 'd1', b int)"
            )
            tdSql.execute(f"alter vtable leaf add column c int from {reference} where device = 'd2'")
        else:
            tdSql.execute(
                "create stable filter_stable (ts timestamp, a int, b int, c int) tags (site int) virtual 1"
            )
            tdSql.execute(
                f"create vtable leaf (a from {reference} where device = 'd1') using filter_stable tags (1)"
            )
            tdSql.execute(f"alter vtable leaf alter column c set {reference} where device = 'd2'")
        tdSql.execute(
            f"create vtable chain (ts timestamp, a int from {_LOCAL_DB}.leaf.a, c int from {_LOCAL_DB}.leaf.c)"
        )

        expected = [(10, None, 20), (11, None, 21)]
        self.check_views(kind, expected)
        self.check_roundtrip("leaf", 2, "a, b, c", expected)
        self.check_roundtrip("chain", 0, "a, c", [(10, 20), (11, 21)])

        tdSql.execute(f"alter vtable leaf alter column a set {reference} where device = 'd2'")
        expected = [(20, None, 20), (21, None, 21)]
        self.check_views(kind, expected)
        ddl = self.check_roundtrip("leaf", 2, "a, b, c", expected)
        assert "'d1'" not in ddl, ddl

        tdSql.execute(f"alter vtable leaf alter column b set {reference} where device = 'd1'")
        expected = [(20, 10, 20), (21, 11, 21)]
        self.check_views(kind, expected)
        self.check_roundtrip("leaf", 3, "a, b, c", expected)

        # Rebinding without a filter must discard the previously saved condition.
        tdSql.execute(f"alter vtable leaf alter column a set {self.source}.{_REMOTE_DB}.single_pk.v")
        expected = [(100, 10, 20), (101, 11, 21)]
        self.check_views(kind, expected)
        self.check_roundtrip("leaf", 2, "a, b, c", expected)

        tdSql.execute(f"alter vtable leaf alter column a set {reference} where device = 'd2'")
        tdSql.execute("alter vtable leaf alter column a set null")
        expected = [(None, 10, 20), (None, 11, 21)]
        self.check_views(kind, expected)
        self.check_roundtrip("leaf", 2, "a, b, c", expected)
        self.check_roundtrip("chain", 0, "a, c", [(None, 20), (None, 21)])
        tdSql.error(
            f"alter vtable chain alter column a set {_LOCAL_DB}.leaf.a where device = 'd1'",
            expectedErrno=_FILTER_NOT_SUPPORTED,
        )
        tdSql.execute(f"alter vtable leaf alter column a set {reference} where device = 'd1'")
        self.check_views(kind, [(10, 10, 20), (11, 11, 21)])
        print(f"{kind} external-filter ALTER lifecycle ........ [ passed ]")

    def do_long_filter_roundtrip(self):
        reference = f"{self.source}.{_REMOTE_DB}.readings.v"
        tdSql.execute(
            f"create vtable long_filter (ts timestamp, a int from {reference} "
            f"where device = 'd1' and note = '{_LONG_NOTE}')"
        )
        tdSql.execute(f"create vtable long_chain (ts timestamp, a int from {_LOCAL_DB}.long_filter.a)")
        self.check_result("long_filter", "a", [(11,)])
        self.check_result("long_chain", "a", [(11,)])
        ddl = self.check_roundtrip("long_filter", 1, "a", [(11,)])
        assert _LONG_NOTE in ddl, "SHOW CREATE truncated the saved filter literal"
        tdSql.execute("alter vtable long_filter alter column a set null")
        ddl = self.check_roundtrip("long_filter", 0, "a", [])
        assert _LONG_NOTE not in ddl, "SET NULL retained the old filter"
        self.check_result("long_chain", "a", [])
        self.check_roundtrip("long_chain", 0, "a", [])
        print("long external-filter SHOW CREATE ........ [ passed ]")

    def do_refresh_binding(self, backend):
        tables = self.prepare_binding_views()
        for table in tables:
            self.check_result(table, "a, b", [(10, 100), (None, 101)])

        # A changed filter column must not invalidate an unrelated projection.
        self.remote_exec(backend, ["alter table readings rename column note to old_note"])
        self.check_invalid_views(tables, "a", _SCHEMA_CHANGED)
        for table in tables:
            self.check_result(table, "b", [(100,), (101,)])
        self.remote_exec(backend, ["alter table readings rename column old_note to note"])

        self.remote_exec(backend, ["update readings set note = '0'"])
        change_note = "modify column note int" if backend == "mysql" else "alter column note type int using note::int"
        self.remote_exec(backend, [f"alter table readings {change_note}"])
        self.check_invalid_views(tables, "a", _SCHEMA_CHANGED)
        restore_note = ("modify column note varchar(8192)" if backend == "mysql"
                        else "alter column note type varchar(8192) using note::varchar")
        self.remote_exec(backend, [f"alter table readings {restore_note}", "update readings set note = ''",
                                   f"update readings set note = '{_LONG_NOTE}' where v = 11"])

        change_type = ("modify column v varchar(32)" if backend == "mysql"
                       else "alter column v type varchar(32) using v::varchar")
        self.remote_exec(backend, [f"alter table readings {change_type}"])
        self.check_invalid_views(tables, "a", _SCHEMA_CHANGED)
        for table in tables:
            self.check_result(table, "b", [(100,), (101,)])
        restore_type = "modify column v int" if backend == "mysql" else "alter column v type int using v::int"
        self.remote_exec(backend, [f"alter table readings {restore_type}"])

        self.replace_primary_key(backend, "readings", "ts, device, v")
        self.check_invalid_views(tables, "a", _INCOMPLETE_PK_FILTER)
        self.replace_primary_key(backend, "readings", "ts, device")

        # A pre-existing unfiltered binding becomes invalid after the remote PK grows.
        self.replace_primary_key(backend, "single_pk", "ts, v")
        self.check_invalid_views(tables, "b", _INCOMPLETE_PK_FILTER)
        for table in tables:
            self.check_result(table, "a", [(10,)])
            tdSql.error(f"select b from {_LOCAL_DB}.{table} where b = 100",
                        expectedErrno=_INCOMPLETE_PK_FILTER)
        self.replace_primary_key(backend, "single_pk", "ts")

        timestamp_type = "datetime(3)" if backend == "mysql" else "timestamp(3)"
        self.remote_exec(backend, [f"alter table readings add column ts2 {timestamp_type} "
                                   "not null default '2024-01-01 00:00:00'"])
        self.replace_primary_key(backend, "readings", "ts, ts2, device")
        self.check_invalid_views(tables, "a", _SCHEMA_CHANGED)
        drop_key = "drop primary key" if backend == "mysql" else "drop constraint readings_pkey"
        self.remote_exec(backend, [f"alter table readings {drop_key}"])
        self.check_invalid_views(tables, "a", _SCHEMA_CHANGED)
        self.remote_exec(backend, ["alter table readings add primary key (ts, device)"])

        self.remote_exec(backend, ["alter table readings drop column v"])
        self.check_invalid_views(tables, "a", _SCHEMA_CHANGED)
        for table in tables:
            self.check_result(table, "b", [(100,), (101,)])
        print("external-filter REFRESH revalidation ........ [ passed ]")

    def do_restart_binding(self):
        tables = self.prepare_binding_views()
        tdSql.execute(f"alter vtable leaf alter column a set {self.source}.{_REMOTE_DB}.readings.v "
                      "where device = 'd2'")
        tdSql.execute("alter vtable leaf_child alter column b set null")
        ddls = {table: extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}")
                for table in tables if table not in ("filter_stable", "chain_stable")}
        tdDnodes.stop(1)
        tdDnodes.start(1)
        ExtSrcEnv.ensure_qnode()
        for table, ddl in ddls.items():
            assert extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}") == ddl
        for table in ("leaf", "chain_leaf"):
            self.check_result(table, "a, b", [(20, 100), (21, 101)])
        for table in ("leaf_child", "chain_leaf_child", "filter_stable", "chain_child", "chain_stable"):
            self.check_result(table, "a, b", [(10, None)])
            self.check_result(table, "b", [])
        tdSql.execute(f"alter vtable {_LOCAL_DB}.leaf_child alter column b set "
                      f"{self.source}.{_REMOTE_DB}.single_pk.v")
        self.check_result("chain_stable", "a, b", [(10, 100), (None, 101)])
        print("external-filter restart persistence ........ [ passed ]")

    def do_remote_sql_failure(self):
        role = f"vfilter_reader_{os.getpid()}"
        source = f"{self.source}_reader"
        self.remote_exec("postgres", [f"create role {role} login password 'filter_test'"])
        try:
            self.remote_exec("postgres", [f"grant usage on schema public to {role}",
                                          f"grant select on readings to {role}",
                                          "alter table readings enable row level security",
                                          "create policy reject_reads on readings using (1 / (v - v) > 0)"])
            tdSql.execute(
                f"create external source {source} type='postgresql' host='{ExtSrcEnv.PG_HOST}' "
                f"port={ExtSrcEnv.PG_PORT} user='{role}' password='filter_test' "
                f"database={_REMOTE_DB} schema=public"
            )
            ref = f"{source}.{_REMOTE_DB}.readings.v where device = 'd1'"
            tdSql.execute(f"create vtable rejected (ts timestamp, a int from {ref})")
            tdSql.execute("create stable rejected_stable (ts timestamp, a int) tags (site int) virtual 1")
            tdSql.execute(f"create vtable rejected_child (a from {ref}) using rejected_stable tags (1)")
            tdSql.execute(f"create vtable rejected_chain (ts timestamp, a int from {_LOCAL_DB}.rejected.a)")
            for table in ("rejected", "rejected_child", "rejected_chain", "rejected_stable"):
                tdSql.error(f"select a from {table}", expectedErrno=_FILTER_SQL_FAILED)
        finally:
            tdSql.execute(f"drop external source if exists {source}")
            self.remote_exec("postgres", ["drop policy if exists reject_reads on readings",
                                          "alter table readings disable row level security",
                                          f"drop owned by {role}", f"drop role {role}"])
        print("mandatory external-filter SQL failure ........ [ passed ]")

    def do_invalid_filters(self, backend):
        self.remote_exec(backend, ["alter table readings add column rate double precision default 1.5",
                                   "alter table readings add column active boolean default true"])
        tdSql.execute(f"refresh external source {self.source}")
        ref = f"{self.source}.{_REMOTE_DB}.readings.v"
        invalid = [
            ("", _INCOMPLETE_PK_FILTER),
            ("device != 'd1'", _INCOMPLETE_PK_FILTER),
            ("device = 'd1' and DEVICE = 'd2'", _DUPLICATE_FILTER_COLUMN),
            ("device = 'd1' or v = 10", _INVALID_FILTER),
            ("readings.device = 'd1'", _INVALID_FILTER),
            ("device = note", _INVALID_FILTER),
            ("'d1' = device", _INVALID_FILTER),
            ("device like 'd%'", _INVALID_FILTER),
            ("device in ('d1', 'd2')", _INVALID_FILTER),
            ("device = null", _INVALID_FILTER),
            ("not (device = 'd1')", _INVALID_FILTER),
            # BETWEEN is lowered to two comparisons against the same column.
            ("device = 'd1' and v between 10 and 11", _DUPLICATE_FILTER_COLUMN),
            ("device = 'd1' and abs(v) = 10", _INVALID_FILTER),
            ("device = 'd1' and v + 1 = 11", _INVALID_FILTER),
            ("device = 'd1' and v = cast(10 as int)", _INVALID_FILTER),
            ("device = 'd1' and v = 'invalid'", _WRONG_VALUE_TYPE),
            ("device = 'd1' and v = ''", _WRONG_VALUE_TYPE),
            ("device = 'd1' and rate = 'invalid'", _WRONG_VALUE_TYPE),
            ("device = 'd1' and rate = '1invalid'", _WRONG_VALUE_TYPE),
            ("device = 'd1' and active = 'not_bool'", _WRONG_VALUE_TYPE),
            ("device = 'd1' and ts > '9223372036854775808'", _WRONG_VALUE_TYPE),
            ("device = 'd1' and ts > '-9223372036854775809'", _WRONG_VALUE_TYPE),
            ("device = 'd1' and ts > ''", _WRONG_VALUE_TYPE),
        ]
        for condition, error in invalid:
            where = f" where {condition}" if condition else ""
            tdSql.error(f"create vtable invalid_filter (ts timestamp, v int from {ref}{where})",
                        expectedErrno=error)
        for literal, expected in (("1", [(10,), (11,)]), ("true", [(10,), (11,)]),
                                   ("'true'", [(10,), (11,)]), ("0", []), ("false", [])):
            tdSql.execute(f"create vtable scalar_filter (ts timestamp, v int from {ref} "
                          f"where device = 'd1' and rate >= '1.5' and active = {literal})")
            self.check_roundtrip("scalar_filter", 1, "v", expected)
            tdSql.execute("drop vtable scalar_filter")
        tdSql.execute("create table local_readings (ts timestamp, v int)")
        tdSql.error(f"create vtable invalid_local (ts timestamp, v int from {_LOCAL_DB}.local_readings.v "
                    "where v = 10)", expectedErrno=_FILTER_NOT_SUPPORTED)
        print("external-filter DDL error codes ........ [ passed ]")

    def do_multiple_business_keys(self, backend):
        create_table = create_pg_table if backend == "postgres" else create_mysql_table
        timestamp_type = "timestamp(3)" if backend == "postgres" else "datetime(3)"
        create_table(
            _REMOTE_DB, "tenant_readings",
            f"ts {timestamp_type} not null, device varchar(32) not null, "
            "tenant int not null, v int, primary key (ts, device, tenant)",
            [f"('2024-01-01 00:00:0{second}', '{device}', {tenant}, {value + second})"
             for device, tenant, value in (("d1", 1, 10), ("d1", 2, 20), ("d2", 1, 30))
             for second in (0, 1)],
        )
        tdSql.execute(f"refresh external source {self.source}")
        ref = f"{self.source}.{_REMOTE_DB}.tenant_readings.v"
        tables = self.prepare_binding_views(f"{ref} where (device = 'd1') and (tenant = '1')")
        for table in tables:
            self.check_result(table, "a", [(10,), (11,)])
        self.check_roundtrip("leaf", 1, "a", [(10,), (11,)])
        self.check_roundtrip("leaf_child", 1, "a", [(10,), (11,)])

        for condition in ("device = 'd1'", "tenant = 1", "device = 'd1' and v = 10",
                          "device = 'd1' and tenant > 0", "device != 'd1' and tenant = 1"):
            tdSql.error(f"create vtable incomplete (ts timestamp, a int from {ref} where {condition})",
                        expectedErrno=_INCOMPLETE_PK_FILTER)
            tdSql.error(f"create vtable incomplete_child (a from {ref} where {condition}) "
                        "using filter_stable tags (2)", expectedErrno=_INCOMPLETE_PK_FILTER)

        # Reordered predicates and a negative non-key comparison remain valid.
        tdSql.execute(f"alter vtable leaf_child alter column a set {ref} "
                      "where tenant = 2 and ((device = 'd1') and (v != -1))")
        for table in ("leaf_child", "chain_leaf_child", "filter_stable", "chain_child", "chain_stable"):
            self.check_result(table, "a", [(20,), (21,)])
        self.check_result("leaf", "a", [(10,), (11,)])
        print("multiple external business keys ........ [ passed ]")

    def do_failed_alter_preserves_binding(self):
        tables = self.prepare_binding_views()
        ref = f"{self.source}.{_REMOTE_DB}.readings.v"
        invalid = (("device = 'd1' or v = 10", _INVALID_FILTER),
                   ("device = 'd1' and device = 'd2'", _DUPLICATE_FILTER_COLUMN),
                   ("device != 'd1'", _INCOMPLETE_PK_FILTER),
                   ("device = 'd1' and v = 'invalid'", _WRONG_VALUE_TYPE))
        for table in ("leaf", "leaf_child"):
            before = extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}")
            for condition, error in invalid:
                tdSql.error(f"alter vtable {table} alter column a set {ref} where {condition}",
                            expectedErrno=error)
                assert extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}") == before
                for view in tables:
                    self.check_result(view, "a, b", [(10, 100), (None, 101)])
                if table == "leaf":
                    tdSql.error(f"alter vtable {table} add column rejected int from {ref} where {condition}",
                                expectedErrno=error)
                    assert extract_show_create_sql(f"show create vtable {_LOCAL_DB}.{table}") == before

        tdSql.execute("create table local_readings (ts timestamp, v int)")
        for ref in (f"{_LOCAL_DB}.local_readings.v", f"{_LOCAL_DB}.leaf.a"):
            tdSql.error(f"alter vtable chain_leaf alter column a set {ref} where v = 10",
                        expectedErrno=_FILTER_NOT_SUPPORTED)
            tdSql.error(f"alter vtable chain_leaf add column rejected int from {ref} where v = 10",
                        expectedErrno=_FILTER_NOT_SUPPORTED)
        self.check_roundtrip("chain_leaf", 0, "a, b", [(10, 100), (None, 101)])
        print("failed external-filter ALTER preserves binding ........ [ passed ]")

    def do_filter_literal_boundaries(self, backend):
        self.remote_exec(backend, ["alter table readings add column observed_at date",
                                   "update readings set observed_at = '2024-01-01'"])
        tdSql.execute(f"refresh external source {self.source}")
        ref = f"{self.source}.{_REMOTE_DB}.readings.v"
        # Every allowed comparison operator is exercised on a non-key column.
        conditions = (("v = 10", [(10,)]), ("v != 10", [(11,)]),
                      ("v < 11", [(10,)]), ("v <= 10", [(10,)]),
                      ("v > 10", [(11,)]), ("v >= 11", [(11,)]),
                      ("v > -1", [(10,), (11,)]),
                      ("ts >= '2024-01-01 00:00:01'", [(11,)]),
                      ("ts < '2024-01-01 00:00:01'", [(10,)]),
                      ("observed_at = '2024-01-01'", [(10,), (11,)]),
                      ("observed_at > '2024-01-01'", []))
        for condition, expected in conditions:
            tdSql.execute(f"create vtable literal_filter (ts timestamp, v int from {ref} "
                          f"where ((device = 'd1') and ({condition})))")
            self.check_roundtrip("literal_filter", 1, "v", expected)
            tdSql.execute("drop vtable literal_filter")

        # SQL-looking text is a literal, not an extra predicate or statement.
        for literal in ("'it''s a sensor'", "'d1'' OR ''1''=''1'", "'设备甲;--%_|&'"):
            self.remote_exec(backend, [f"update readings set note = {literal} where device = 'd1'"])
            tdSql.execute(f"create vtable escaped_filter (ts timestamp, v int from {ref} "
                          f"where device = 'd1' and note = {literal})")
            self.check_roundtrip("escaped_filter", 1, "v", [(10,), (11,)])
            tdSql.execute("drop vtable escaped_filter")
        print("external-filter comparison and literal boundaries ........ [ passed ]")

    # --- main ---

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    @pytest.mark.parametrize("kind", ("normal", "child"))
    def test_filter_column_case(self, backend, kind):
        """Match external filter column names using source-specific case rules

        1. Distinguish quoted PostgreSQL columns and primary keys differing only in case
        2. Keep MySQL column matching and duplicate detection case insensitive
        3. Preserve filter semantics through CREATE, ALTER, chained queries and SHOW CREATE replay
        4. Revalidate filters after a remote column changes case

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-09 Jing Sima Cover external filter column case rules

        """
        self.do_filter_column_case(backend, kind)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    @pytest.mark.parametrize("kind", ("normal", "child"))
    def test_alter_filter(self, backend, kind):
        """Preserve and clear saved filters through virtual-table ALTER

        1. Add and replace filtered bindings on normal and child virtual tables
        2. Read updated leaf filters through chained and super-table queries
        3. Rebind without a filter and clear a filtered reference with SET NULL
        4. Re-execute SHOW CREATE and reject an extra filter on a virtual reference

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add external-filter DDL lifecycle coverage

        """
        self.do_alter_filter(kind)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_long_filter_roundtrip(self, backend):
        """Round-trip long saved filter conditions through SHOW CREATE

        1. Preserve a long literal without truncating SHOW CREATE output
        2. Query the recreated binding and remove its condition with SET NULL

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add long-filter SHOW CREATE coverage

        """
        self.do_long_filter_roundtrip()

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_refresh_binding(self, backend):
        """Revalidate only accessed external bindings after REFRESH

        1. Reject changed filter columns and incompatible referenced column types
        2. Reject incomplete business keys, including existing unfiltered bindings
        3. Ignore query WHERE for coverage and leave unrelated columns queryable

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add external binding refresh coverage

        """
        self.do_refresh_binding(backend)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_restart_binding(self, backend):
        """Preserve saved column filters across a dnode restart

        1. Persist SET and SET NULL on normal and child virtual tables
        2. Verify SHOW CREATE and direct, chained and super-table query results

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add saved-filter restart coverage

        """
        self.do_restart_binding()

    @pytest.mark.parametrize("backend", ("postgres",))
    def test_remote_sql_failure(self, backend):
        """Do not demote a failed mandatory remote filter to local execution

        1. Use a remote row policy that rejects execution without changing metadata
        2. Require the stored-filter SQL error for direct, chained and super-table scans

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add remote SQL rejection coverage

        """
        self.do_remote_sql_failure()

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_invalid_filters(self, backend):
        """Return distinct errors for invalid external column filters

        1. Reject invalid expression structure, duplicate columns and missing business keys
        2. Check numeric and boolean conversion, including saved-filter round trips
        3. Reject filters on internal table references

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add real-source parser error coverage
            - 2026-09-08 Simon Guan Reject empty and overflowing timestamp literals

        """
        self.do_invalid_filters(backend)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_multiple_business_keys(self, backend):
        """Require every business key in three-column remote primary keys

        1. Separate rows sharing a timestamp and only one of two business keys
        2. Reject missing or non-equality business-key predicates in CREATE and USING
        3. Round-trip converted literals and propagate a leaf rebind through chains

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add multiple-business-key coverage

        """
        self.do_multiple_business_keys(backend)

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_failed_alter_preserves_binding(self, backend):
        """Keep the previous binding after a rejected filtered ALTER

        1. Reject invalid SET and ADD without changing SHOW CREATE or query results
        2. Check normal, child, chained and super-table views after each failure
        3. Reject new filters on internal and virtual column references

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add failed-ALTER atomicity coverage

        """
        self.do_failed_alter_preserves_binding()

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
    def test_filter_literal_boundaries(self, backend):
        """Round-trip comparison, date and escaped-string filter literals

        1. Exercise all six comparison operators with nested parentheses
        2. Convert timestamp and date strings and test matching and empty results
        3. Preserve apostrophes, Unicode and SQL-looking text as literal values

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add filter literal boundary coverage

        """
        self.do_filter_literal_boundaries(backend)
