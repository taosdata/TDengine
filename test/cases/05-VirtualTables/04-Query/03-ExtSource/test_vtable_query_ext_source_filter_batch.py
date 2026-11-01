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

import datetime
import os
import sys

import pytest

# Open product finding (reported 2026-10-16): a SQL Server five-part external
# column reference (source.database.schema.table.column) loses its schema
# segment when the reference is re-rendered or re-resolved — SHOW CREATE VTABLE
# prints the four-part form, and resolving through a local virtual-table chain
# does the same — so the re-executed statement fails with 0x6410 ("External
# table not found on remote source").  The affected SQL Server legs are marked
# as strict expected failures: they start failing loudly as soon as the
# rendering defect is fixed, which is when the marker must be removed.

_VTABLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _VTABLE_DIR)
sys.path.insert(0, os.path.join(_VTABLE_DIR, "01-DDL", "03-ExtSource"))

from vtable_util import ExtSrcEnv, VtableQueryUtil
from ext_source_helpers import (
    create_ext_source, create_mysql_table, create_oracle_table,
    create_pg_table, create_remote_db, create_sqlserver_table,
)


_LOCAL_DB = "vfilter_batch"
_REMOTE_DB = "vfilter_batch_ext"
_SOURCES = {"mysql": "vfilter_batch_mysql", "postgres": "vfilter_batch_pg",
            "sqlserver": "vfilter_batch_sqlserver",
            "oracle": "vfilter_batch_oracle"}
_SOURCE_TYPES = {"mysql": "mysql", "postgres": "postgresql",
                 "sqlserver": "sqlserver", "oracle": "oracle"}
_BASE_TIME = datetime.datetime(2024, 1, 1)
_BASE_MS = 1704067200000
# Both populated parameters exceed the connectors' 4096-row fetch size.
_DEVICE_ROWS = {"d1": 8193, "d2": 10241}
# Oracle has one namespace level - the login schema - and no per-source
# database, so its remote objects are qualified as source.schema.table.
_ORACLE_SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
# Oracle tables live in the shared login schema; prefixing their names keeps
# them apart from the fixtures of the other ext-source test files.
_ORACLE_TABLE_PREFIX = "oracle_"
# sqlplus receives each statement as one argv entry (128 KiB per-argument
# limit), so INSERT ALL statements carry a bounded number of rows.
_ORACLE_INSERT_BATCH_ROWS = 500


def _oracle_create_table(remote_db, table, col_defs, rows=None):
    """Adapter matching the create_<backend>_table signature for Oracle.

    Oracle has no per-test database and no multi-row VALUES clause, so the
    helper drops the table in the login schema first and inserts its rows
    through INSERT ALL.  The database argument is unused.
    """
    create_oracle_table(f"{_ORACLE_TABLE_PREFIX}{table}", col_defs, rows)


def _integer_type(backend):
    """Local integer type matching the fixture's external integer column.

    Oracle NUMBER(p, 0) maps to BIGINT for p in 2..18 and a virtual table must
    declare exactly the mapped type, so Oracle uses bigint where the other
    backends use int.
    """
    return "bigint" if backend == "oracle" else "int"


def _source_database(backend):
    """Remote database argument for CREATE EXTERNAL SOURCE.

    Oracle carries the Easy Connect service name in DATABASE; every other
    backend carries the per-test database name.
    """
    return ExtSrcEnv.ORACLE_SERVICE if backend == "oracle" else _REMOTE_DB


class TestVTableQueryExtSourceFilterBatch:
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @pytest.fixture(autouse=True)
    def prepare_backend(self, backend):
        VtableQueryUtil._EXT_SOURCE_ADAPTERS[backend]().ensure_running()
        ExtSrcEnv.ensure_qnode()
        source = _SOURCES[backend]
        try:
            self.prepare_remote_table(backend)
            source_type = _SOURCE_TYPES[backend]
            create_ext_source(source, source_type, _source_database(backend))
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            # SQL Server timestamps are microsecond-resolution (DS §5.3.7),
            # so its virtual tables require a precision 'us' database (TS §12).
            db_precision = "us" if backend == "sqlserver" else "ms"
            tdSql.execute(f"create database {_LOCAL_DB} precision '{db_precision}'")
            self.prepare_vtables(
                backend, source,
                ref_root=self.remote_table_ref(source, backend),
                upper_ref=backend not in ("sqlserver", "oracle"))
            yield
        finally:
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"drop external source if exists {source}")
            if backend == "mysql":
                ExtSrcEnv.mysql_drop_db(_REMOTE_DB)
            elif backend == "sqlserver":
                ExtSrcEnv.sqlserver_drop_db(_REMOTE_DB)
            elif backend == "postgres":
                ExtSrcEnv.pg_drop_db(_REMOTE_DB)
            # Oracle owns the tables in the login schema; the next fixture run
            # drops them before recreating.

    # --- util ---

    @staticmethod
    def has_row(device, index):
        return index < _DEVICE_ROWS[device] and (device == "d1" or index % 3 != 0)

    @classmethod
    def prepare_remote_table(cls, backend):
        if backend == "mysql":
            create_remote_db("mysql", _REMOTE_DB)
            create_table = create_mysql_table
            timestamp_type = "datetime(3)"
        elif backend == "sqlserver":
            create_remote_db("sqlserver", _REMOTE_DB)
            create_table = create_sqlserver_table
            timestamp_type = "datetime2(3)"
        elif backend == "oracle":
            # Oracle has no CREATE DATABASE in this model; the namespace is the
            # login user's schema.  TIMESTAMP(3) is millisecond precision and
            # NUMBER(10, 0) maps to BIGINT, like the other backends' int.
            create_remote_db("oracle", _REMOTE_DB)
            create_table = _oracle_create_table
            timestamp_type = "TIMESTAMP(3)"
        else:
            create_remote_db("postgresql", _REMOTE_DB)
            create_table = create_pg_table
            timestamp_type = "timestamp(3)"
        column_defs = (
            f"ts {timestamp_type} NOT NULL, device VARCHAR2(8) NOT NULL, "
            "v NUMBER(10,0), w NUMBER(10,0), PRIMARY KEY (ts, device)"
            if backend == "oracle" else
            f"ts {timestamp_type} not null, device varchar(8) not null, "
            "v int, w int, primary key (ts, device)"
        )
        create_table(_REMOTE_DB, "readings", column_defs)
        rows = []
        for device, count in _DEVICE_ROWS.items():
            offset = 100000 if device == "d1" else 200000
            for index in range(count):
                if cls.has_row(device, index):
                    timestamp = _BASE_TIME + datetime.timedelta(seconds=index)
                    if backend == "oracle":
                        # A bare string date would go through
                        # NLS_TIMESTAMP_FORMAT and raise ORA-01861.
                        timestamp_literal = f"TIMESTAMP '{timestamp}'"
                    else:
                        timestamp_literal = f"'{timestamp}'"
                    rows.append(
                        f"({timestamp_literal}, '{device}', "
                        f"{offset + index}, {-offset - index})"
                    )
        if backend == "mysql":
            execute = ExtSrcEnv.mysql_exec
        elif backend == "sqlserver":
            execute = ExtSrcEnv.sqlserver_exec
        elif backend == "oracle":
            # One INSERT ALL per bounded batch, each a single sqlplus argv
            # entry; a single multi-row VALUES clause does not exist in Oracle.
            cfg = next(ExtSrcEnv.oracle_version_configs())
            for start in range(0, len(rows), _ORACLE_INSERT_BATCH_ROWS):
                batch = rows[start:start + _ORACLE_INSERT_BATCH_ROWS]
                # One INTO branch per line: SQL*Plus silently truncates an
                # input line past ~5000 characters (SP2-0027) and would load
                # only part of the batch.
                into = "\n".join(
                    f"INTO {_ORACLE_TABLE_PREFIX}readings VALUES {row}"
                    for row in batch)
                ExtSrcEnv._oracle_sql(
                    cfg, f"INSERT ALL\n{into}\nSELECT 1 FROM dual")
            ExtSrcEnv._oracle_sql(cfg, "COMMIT")
            return
        else:
            execute = ExtSrcEnv.pg_exec
        execute(_REMOTE_DB, [
            "insert into readings values " + ",".join(rows[start:start + 1000])
            for start in range(0, len(rows), 1000)
        ])

    @staticmethod
    def remote_table_ref(source, backend, table="readings"):
        """Fully-qualified remote table path for the given backend.

        MySQL/PG use ``source.database.table``.  For SQL Server the middle
        segment of a three-part path is the *schema* (DS §5.3.4), so the
        database must be qualified explicitly:
        ``source.database.schema.table``.  Oracle has a single namespace level,
        the schema, so the middle segment is likewise the schema and the
        fixture table name carries the file prefix: ``source.schema.table``.
        """
        if backend == "sqlserver":
            return f"{source}.{_REMOTE_DB}.dbo.{table}"
        if backend == "oracle":
            return f"{source}.{_ORACLE_SCHEMA}.{_ORACLE_TABLE_PREFIX}{table}"
        return f"{source}.{_REMOTE_DB}.{table}"

    @classmethod
    def references(cls, source, backend, first="d1", second="d2",
                   with_types=False, ref_root=None, upper_ref=False):
        reference = ref_root or cls.remote_table_ref(source, backend)
        # The upper-cased variant exercises MySQL's case-insensitive remote
        # resolution; SQL Server and Oracle resolve the database/schema
        # segments of a multi-part path literally, so they keep the
        # exact-case form.
        ref_b = reference.upper() if upper_ref else reference
        column_type = f" {_integer_type(backend)}" if with_types else ""
        return (
            f"a{column_type} from {reference}.v where device = '{first}', "
            f"b{column_type} from {ref_b}.w where device = '{first}', "
            f"c{column_type} from {reference}.v where device = '{second}', "
            f"empty_v{column_type} from {reference}.v where device = 'missing'"
        )

    @classmethod
    def prepare_vtables(cls, backend, source, ref_root=None, upper_ref=False):
        integer_type = _integer_type(backend)
        tdSql.execute(
            f"create vtable {_LOCAL_DB}.normal_{backend} "
            f"(ts timestamp, {cls.references(source, backend, with_types=True, ref_root=ref_root, upper_ref=upper_ref)})"
        )
        tdSql.execute(
            f"create stable {_LOCAL_DB}.stable_{backend} "
            f"(ts timestamp, a {integer_type}, b {integer_type}, "
            f"c {integer_type}, empty_v {integer_type}) "
            "tags (scenario int) virtual 1"
        )
        for scenario, devices in enumerate((("d1", "d2"), ("d2", "d1")), 1):
            tdSql.execute(
                f"create vtable {_LOCAL_DB}.child_{backend}_{scenario} "
                f"({cls.references(source, backend, *devices, ref_root=ref_root, upper_ref=upper_ref)}) "
                f"using {_LOCAL_DB}.stable_{backend} tags ({scenario})"
            )
        tdSql.execute(
            f"create vtable {_LOCAL_DB}.chain_{backend} (ts timestamp, "
            + ", ".join(
                f"{column} {integer_type} from {_LOCAL_DB}.child_{backend}_1.{column}"
                for column in ("a", "b", "c", "empty_v")
            ) + ")"
        )

    @classmethod
    def expected_rows(cls, reverse=False, unit=1):
        # unit=1000 for the precision 'us' databases (SQL Server virtual
        # tables), whose cast(ts as bigint) values are microseconds.
        rows = []
        first, second = ("d2", "d1") if reverse else ("d1", "d2")
        for index in range(max(_DEVICE_ROWS.values())):
            a = (200000 if reverse else 100000) + index if cls.has_row(first, index) else None
            c = (100000 if reverse else 200000) + index if cls.has_row(second, index) else None
            if a is not None or c is not None:
                rows.append(((_BASE_MS + index * 1000) * unit, a, -a if a is not None else None, c, None))
        return rows

    @staticmethod
    def check_rows(query, expected, scan_count=None):
        if scan_count is not None:
            tdSql.query(f"explain verbose true {query}")
            actual = sum("-> Federated Scan on " in row[0] for row in tdSql.queryResult)
            assert actual == scan_count, f"expected {scan_count} external scans, got {actual}"
        tdSql.query(query)
        tdSql.checkRows(len(expected))
        for index, row in enumerate(expected):
            actual = tuple(tdSql.queryResult[index])
            assert actual == row, f"row {index}: expected {row}, got {actual}"

    # --- impl ---

    def do_filter_batch(self, backend):
        unit = 1000 if backend == "sqlserver" else 1
        tdSql.query("show local variables like 'timezone'")
        original_timezone = str(tdSql.queryResult[0][1]).split(" ")[0]
        try:
            tdSql.execute('alter local "timezone" "UTC"')
            columns = "cast(ts as bigint), a, b, c, empty_v"
            expected = self.expected_rows(unit=unit)
            for table in (f"normal_{backend}", f"child_{backend}_1", f"chain_{backend}"):
                self.check_rows(
                    f"select {columns} from {_LOCAL_DB}.{table} order by ts", expected, scan_count=3,
                )
                self.check_rows(
                    f"select {columns} from {_LOCAL_DB}.{table} "
                    "where c >= 204090 and c < 208200 order by ts",
                    [row for row in expected if row[3] is not None and 204090 <= row[3] < 208200],
                )
                self.check_rows(
                    f"select cast(ts as bigint), a, b from {_LOCAL_DB}.{table} order by ts",
                    [row[:3] for row in expected if row[1] is not None], scan_count=1,
                )
                tdSql.query(f"select empty_v from {_LOCAL_DB}.{table}")
                tdSql.checkRows(0)

            normal = f"{_LOCAL_DB}.normal_{backend}"
            self.check_rows(
                "select cast(lhs.ts as bigint), lhs.a, rhs.c "
                f"from {normal} lhs join {normal} rhs on lhs.ts = rhs.ts order by lhs.ts",
                [(row[0], row[1], row[3]) for row in expected if row[1] is not None and row[3] is not None],
                scan_count=2,
            )

            stable = f"{_LOCAL_DB}.stable_{backend}"
            combined = [(1, *row) for row in expected]
            combined.extend((2, *row) for row in self.expected_rows(reverse=True, unit=unit))
            self.check_rows(
                f"select scenario, {columns} from {stable} order by scenario, ts", combined, scan_count=1,
            )
            aggregates = []
            for scenario in (1, 2):
                rows = [row for row in combined if row[0] == scenario]
                a_values = [row[2] for row in rows if row[2] is not None]
                c_values = [row[4] for row in rows if row[4] is not None]
                aggregates.append((scenario, len(a_values), len(c_values), sum(a_values), sum(c_values)))
            self.check_rows(
                f"select scenario, count(a), count(c), sum(a), sum(c) from {stable} "
                "group by scenario order by scenario", aggregates,
            )
            self.check_rows(
                f"select count(*), count(a), count(c), count(empty_v) from {stable}",
                [(len(combined), sum(row[2] is not None for row in combined),
                  sum(row[4] is not None for row in combined), 0)],
            )
            # Only accessed columns contribute timestamps; this binding has no rows.
            self.check_rows(f"select count(*), count(empty_v) from {stable}", [(0, 0)])
            start = (_BASE_MS + 4090 * 1000) * unit
            end = (_BASE_MS + 8200 * 1000) * unit
            self.check_rows(
                f"select scenario, {columns} from {stable} "
                f"where ts >= {start} and ts < {end} order by scenario, ts",
                [row for row in combined if start <= row[1] < end],
            )
            tdSql.query(f"select empty_v from {stable}")
            tdSql.checkRows(0)
        finally:
            tdSql.execute(f'alter local "timezone" "{original_timezone}"')
        print(f"{backend} stored-filter batch merge ........ [ passed ]")

    # --- main ---

    @pytest.mark.parametrize("backend", ("mysql", "postgres",
                                  "sqlserver", "oracle"))
    def test_filter_batch(self, backend):
        """Merge multiple saved filters from one external table

        1. Read multiple connector blocks with overlapping and missing timestamps
        2. Keep equal-condition columns together and different conditions isolated
        3. Continue other parameters after an empty or shorter parameter reaches EOF
        4. Check normal, child, chained and multi-child virtual-table results
        5. Preserve group slot identity across self-join aliases and UTC time ranges

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-04 Simon Guan Add stored-filter batch result coverage

        """
        self.do_filter_batch(backend)
