###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

from new_test_framework.utils import tdLog, tdSql, etool

import datetime
import os
import sys

import pytest

_VTABLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _VTABLE_DIR)
sys.path.insert(0, os.path.join(_VTABLE_DIR, "01-DDL", "03-ExtSource"))

from vtable_util import ExtSrcEnv, VtableQueryUtil
from ext_source_helpers import (
    create_ext_source, create_mysql_table, create_pg_table, create_remote_db,
)


_LOCAL_DB = "vfilter_batch"
_REMOTE_DB = "vfilter_batch_ext"
_SOURCES = {"mysql": "vfilter_batch_mysql", "postgres": "vfilter_batch_pg"}
_BASE_TIME = datetime.datetime(2024, 1, 1)
_BASE_MS = 1704067200000
# Both populated parameters exceed the connectors' 4096-row fetch size.
_DEVICE_ROWS = {"d1": 8193, "d2": 10241}


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
            source_type = "postgresql" if backend == "postgres" else "mysql"
            create_ext_source(source, source_type, _REMOTE_DB)
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"create database {_LOCAL_DB} precision 'ms'")
            self.prepare_vtables(backend, source)
            yield
        finally:
            tdSql.execute(f"drop database if exists {_LOCAL_DB}")
            tdSql.execute(f"drop external source if exists {source}")
            if backend == "mysql":
                ExtSrcEnv.mysql_drop_db(_REMOTE_DB)
            else:
                ExtSrcEnv.pg_drop_db(_REMOTE_DB)

    # --- util ---

    @staticmethod
    def has_row(device, index):
        return index < _DEVICE_ROWS[device] and (device == "d1" or index % 3 != 0)

    @classmethod
    def prepare_remote_table(cls, backend):
        is_mysql = backend == "mysql"
        create_remote_db("mysql" if is_mysql else "postgresql", _REMOTE_DB)
        create_table = create_mysql_table if is_mysql else create_pg_table
        timestamp_type = "datetime(3)" if is_mysql else "timestamp(3)"
        create_table(
            _REMOTE_DB, "readings",
            f"ts {timestamp_type} not null, device varchar(8) not null, "
            "v int, w int, primary key (ts, device)",
        )
        rows = []
        for device, count in _DEVICE_ROWS.items():
            offset = 100000 if device == "d1" else 200000
            for index in range(count):
                if cls.has_row(device, index):
                    timestamp = _BASE_TIME + datetime.timedelta(seconds=index)
                    rows.append(
                        f"('{timestamp}', '{device}', "
                        f"{offset + index}, {-offset - index})"
                    )
        execute = ExtSrcEnv.mysql_exec if is_mysql else ExtSrcEnv.pg_exec
        execute(_REMOTE_DB, [
            "insert into readings values " + ",".join(rows[start:start + 1000])
            for start in range(0, len(rows), 1000)
        ])

    @staticmethod
    def references(source, first="d1", second="d2", with_types=False):
        reference = f"{source}.{_REMOTE_DB}.readings"
        column_type = " int" if with_types else ""
        return (
            f"a{column_type} from {reference}.v where device = '{first}', "
            f"b{column_type} from {reference.upper()}.w where device = '{first}', "
            f"c{column_type} from {reference}.v where device = '{second}', "
            f"empty_v{column_type} from {reference}.v where device = 'missing'"
        )

    @classmethod
    def prepare_vtables(cls, backend, source):
        tdSql.execute(
            f"create vtable {_LOCAL_DB}.normal_{backend} "
            f"(ts timestamp, {cls.references(source, with_types=True)})"
        )
        tdSql.execute(
            f"create stable {_LOCAL_DB}.stable_{backend} "
            "(ts timestamp, a int, b int, c int, empty_v int) "
            "tags (scenario int) virtual 1"
        )
        for scenario, devices in enumerate((("d1", "d2"), ("d2", "d1")), 1):
            tdSql.execute(
                f"create vtable {_LOCAL_DB}.child_{backend}_{scenario} "
                f"({cls.references(source, *devices)}) "
                f"using {_LOCAL_DB}.stable_{backend} tags ({scenario})"
            )
        tdSql.execute(
            f"create vtable {_LOCAL_DB}.chain_{backend} (ts timestamp, "
            + ", ".join(
                f"{column} int from {_LOCAL_DB}.child_{backend}_1.{column}"
                for column in ("a", "b", "c", "empty_v")
            ) + ")"
        )

    @classmethod
    def expected_rows(cls, reverse=False):
        rows = []
        first, second = ("d2", "d1") if reverse else ("d1", "d2")
        for index in range(max(_DEVICE_ROWS.values())):
            a = (200000 if reverse else 100000) + index if cls.has_row(first, index) else None
            c = (100000 if reverse else 200000) + index if cls.has_row(second, index) else None
            if a is not None or c is not None:
                rows.append((_BASE_MS + index * 1000, a, -a if a is not None else None, c, None))
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
        tdSql.query("show local variables like 'timezone'")
        original_timezone = str(tdSql.queryResult[0][1]).split(" ")[0]
        try:
            tdSql.execute('alter local "timezone" "UTC"')
            columns = "cast(ts as bigint), a, b, c, empty_v"
            expected = self.expected_rows()
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
            combined.extend((2, *row) for row in self.expected_rows(reverse=True))
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
            start = _BASE_MS + 4090 * 1000
            end = _BASE_MS + 8200 * 1000
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

    @pytest.mark.parametrize("backend", ("mysql", "postgres"))
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
