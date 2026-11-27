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

from new_test_framework.utils import tdCom, tdLog, tdSql

from contextlib import contextmanager
from math import isclose
from pathlib import Path
import json
import re
from urllib.parse import parse_qs, urlsplit

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryTestMixin,
    influx18_basic_gate,
    influx_versions,
)


class TestFq24Influx18Pushdown(FederatedQueryTestMixin):
    database = "fq24_influx18_pushdown"
    local_database = "fq24_influx18_local"
    base = 1700000000000000000
    future = 4102444800000000000  # 2100-01-01, within InfluxDB's nanosecond domain.

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_env()

    @contextmanager
    def fixture(self, response_override=None, points=None):
        if points is None:
            points = [
                (self.base, "a", 1, 1.5, "alpha", True),
                (self.base + 1000000000, "b", 3, 2.5, "a/x", False),
                (self.base + 2000000000, None, None, 3.5, "a.b", True),
                (self.base + 3000000000, "a", 9007199254740993, None, None, None),
                (self.base + 4000000000, None, 7, 5.5, "中文", True),
                (self.base + 6000000000, "c", None, None, None, None),
                (self.future, "a", 9, 7.5, "alpha", False),
            ]
        lines = []
        for timestamp, site, integer, floating, label, flag in points:
            fields = ["anchor=1i"]
            if integer is not None:
                fields.append(f"i={integer}i")
            if floating is not None:
                fields.append(f"d={floating}")
            if label is not None:
                fields.append(f'label="{label}"')
            if flag is not None:
                fields.append("flag=" + str(flag).lower())
            tag = "" if site is None else f",site={site}"
            lines.append(f"rows{tag} {','.join(fields)} {timestamp}")
        tdSql.query("SELECT timezone()")
        original_timezone = str(tdSql.queryResult[0][0]).split(" (", 1)[0]
        try:
            tdSql.execute('alter local "timezone" "UTC"')
            tdSql.execute(f"drop database if exists {self.local_database}")
            tdSql.execute(f"create database {self.local_database} precision 'ns' keep 36500")
            local = f"{self.local_database}.points"
            tdSql.execute(f"create table {local} (time timestamp, site nchar(16), i bigint, "
                          "d double, label nchar(32), flag bool, anchor bigint)")
            for point in points:
                values = ["NULL" if value is None else
                          ("'" + value.replace("'", "''") + "'" if isinstance(value, str) else str(value).lower())
                          for value in point]
                tdSql.execute(f"insert into {local} values ({','.join(values)},1)")
            with self._influx_fixture(self.database, lines) as (cfg, _):
                with influx18_basic_gate(cfg, response_override=response_override) as (port, seen):
                    source = self.database + "_gate"
                    try:
                        tdSql.execute(self._mk_influx_source_sql(
                            source, "127.0.0.1", port, "", database=self.database,
                            user="basic18", password="basic18_test_password"))
                        yield f"{source}.{self.database}.rows", local, seen
                    finally:
                        self._cleanup_src(source)
        finally:
            try:
                tdSql.execute(f"drop database if exists {self.local_database}")
            finally:
                tdSql.execute(f'alter local "timezone" "{original_timezone}"')

    @staticmethod
    def select_requests(seen):
        queries = [parse_qs(urlsplit(path).query).get("q", [""])[0] for path in seen]
        return [sql for sql in queries if sql.upper().startswith("SELECT ")]

    @staticmethod
    def fixture_queries(case):
        """Read the one-line SELECTs shared by CLI goldens and request checks."""
        input_file = Path(__file__).parent / "in" / f"test_fq_24_influx18_pushdown_{case}.in"
        return [line.strip().rstrip(";") for line in input_file.read_text(encoding="utf-8").splitlines()
                if line.lstrip().upper().startswith("SELECT ")]

    @staticmethod
    def compare_fixture(case):
        name = f"test_fq_24_influx18_pushdown_{case}"
        directory = Path(__file__).parent
        tdCom.compare_testcase_result(
            str(directory / "in" / f"{name}.in"), str(directory / "ans" / f"{name}.ans"), name,
            float_tolerance=1e-10, result_transform=ExtSrcEnv.influx18_plan_baseline)

    def compare(self, remote_sql, remote, local, seen, required=(), forbidden=(), local_operator=None,
                expected=None):
        if expected is None:
            tdSql.query(remote_sql.replace(remote, local))
            expected = list(tdSql.queryResult)
        seen.clear()
        tdSql.query(remote_sql)
        actual = list(tdSql.queryResult)
        assert len(actual) == len(expected), (remote_sql, actual, expected)
        for left, right in zip(actual, expected):
            assert len(left) == len(right), (remote_sql, actual, expected)
            for value, reference in zip(left, right):
                if isinstance(value, float) and isinstance(reference, (float, int)):
                    assert isclose(value, reference, rel_tol=1e-10, abs_tol=1e-10), (remote_sql, actual, expected)
                else:
                    assert value == reference, (remote_sql, actual, expected)
        requests = self.select_requests(seen)
        assert requests, (remote_sql, seen)
        rendered = "\n".join(requests).upper()
        for fragment in required:
            assert fragment.upper() in rendered, (remote_sql, fragment, requests)
        for fragment in forbidden:
            assert fragment.upper() not in rendered, (remote_sql, fragment, requests)
        if local_operator is not None:
            tdSql.query("EXPLAIN VERBOSE TRUE " + remote_sql)
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult if "Remote SQL:" not in str(row[0]))
            assert re.search(local_operator, plan, re.IGNORECASE), (remote_sql, plan)

    def do_predicates_and_limits(self):
        with self.fixture() as (remote, local, seen):
            queries = iter(self.fixture_queries("predicates_and_limits"))
            for required in (
                ('"i" >= 3',), ("9007199254740993",), ('"d" <',), ('"flag" = true',),
                ('"label" !=',), ('"site" != \'a\'', '"site" != \'\''), ('"site" = \'\'',),
                ('"site" != \'\'',), (" OR ",), (" AND ", '"site" != \'\''), (" OR ",),
                ("time >=", "time <="),
            ):
                self.compare(next(queries), remote, local, seen, required=required)
            for _ in range(2):
                self.compare(next(queries), remote, local, seen,
                             required=("ORDER BY time", "LIMIT", "OFFSET"))
            self.compare(next(queries), remote, local, seen)
            for _ in range(4):
                self.compare(next(queries), remote, local, seen,
                             forbidden=(" WHERE ",), local_operator="Filter|Conditions")
            self.compare(next(queries), remote, local, seen, required=("WHERE", "ORDER BY time ASC"),
                         forbidden=(" JOIN ",), local_operator="Join")
            # The window includes the points at 0s, 1s and 2s around i=3.
            self.compare(next(queries), remote, local, seen, forbidden=(" JOIN ",), local_operator="Join",
                         expected=[(3, 2.5, value) for value in (1.5, 2.5, 3.5)])
            for _ in range(2):
                self.compare(next(queries), remote, local, seen, required=("WHERE",), forbidden=(" UNION ",))
            assert next(queries, None) is None, "Missing predicate request checks"
            self.compare_fixture("predicates_and_limits")
        print("InfluxDB 1.8 predicates and row limits ............. [ passed ]")

    def do_mixed_predicates(self):
        queries = self.fixture_queries("mixed_predicates")

        def override(path):
            query = parse_qs(urlsplit(path).query).get("q", [""])[0]
            if query.upper().startswith("SELECT "):
                return 200, b'{"results":[{"statement_id":0,"error":"mixed predicate remote failure"}]}'
            return None

        with self.fixture(response_override=override) as (_, _, seen):
            tdSql.error(queries[0], expectedErrno=0x6404)
            assert len(self.select_requests(seen)) == 1, seen

        with self.fixture() as (remote, local, seen):
            for sql in queries:
                self.compare(sql, remote, local, seen,
                             forbidden=(" WHERE ", "SUM(", " LIMIT ", " OFFSET "),
                             local_operator="Filter|Conditions")
            self.compare_fixture("mixed_predicates")
        print("InfluxDB 1.8 mixed predicates stay local .......... [ passed ]")

    def do_aggregates_and_windows(self):
        with self.fixture() as (remote, local, seen):
            queries = iter(self.fixture_queries("aggregates_and_windows"))
            for rendered in ("COUNT", "SUM", "MIN", "MAX"):
                self.compare(next(queries), remote, local, seen,
                             required=(rendered + "(",), forbidden=('SELECT "time"',))
            self.compare(next(queries), remote, local, seen,
                         forbidden=("MEAN(",), local_operator="Aggregate")
            self.compare(next(queries), remote, local, seen, required=("SPREAD(",))
            self.compare(next(queries), remote, local, seen, required=("COUNT(", "SUM("))
            for rendered in ("SUM", "MIN", "MAX", "MEAN", "SPREAD", "STDDEV"):
                self.compare(next(queries), remote, local, seen, required=(rendered + "(",))
            self.compare(next(queries), remote, local, seen,
                         required=('GROUP BY "site"',), forbidden=("ORDER BY",))
            for _ in range(3):
                for rendered in ("COUNT", "SUM", "MEAN", "STDDEV"):
                    self.compare(next(queries), remote, local, seen, required=(rendered + "(",))
            for _ in range(2):
                self.compare(next(queries), remote, local, seen,
                             required=("GROUP BY time(", "fill(none)", "time >=", "time <="))
            self.compare(next(queries), remote, local, seen, forbidden=("SUM(",), local_operator="Aggregate")
            self.compare(next(queries), remote, local, seen, forbidden=("COUNT(",), local_operator="Aggregate")
            assert next(queries, None) is None, "Missing aggregate request checks"
            self.compare_fixture("aggregates_and_windows")
        print("InfluxDB 1.8 aggregates and windows ............... [ passed ]")

    def do_bigint_avg_overflow(self):
        positive = 1 << 62
        negative = -positive - 1024
        points = [
            (self.base, "positive", positive, 1.5, "a", True),
            (self.base + 1000000000, "positive", positive, 2.5, "b", False),
            (self.base + 2000000000, "negative", negative, 3.5, "c", True),
            (self.base + 3000000000, "negative", negative, 4.5, "d", False),
        ]
        with self.fixture(points=points) as (remote, local, seen):
            for sql in self.fixture_queries("bigint_avg_overflow"):
                self.compare(sql, remote, local, seen, required=('SELECT "time"',),
                             forbidden=("MEAN(", "GROUP BY"), local_operator="Aggregate|Interval")
            self.compare_fixture("bigint_avg_overflow")
        print("InfluxDB 1.8 BIGINT AVG overflow stays local ...... [ passed ]")

    def do_arithmetic_and_like(self):
        with self.fixture() as (remote, local, seen):
            queries = iter(self.fixture_queries("arithmetic_and_like"))
            for required in ((" + ",), (" / ",), ("=~ /",), ("!~ /",), ("=~ /",),
                             ("=~ /", '"site" != \'\''), ("=~ /",)):
                self.compare(next(queries), remote, local, seen, required=required)
            for _ in range(3):
                self.compare(next(queries), remote, local, seen,
                             forbidden=(" WHERE ",), local_operator="Filter|Conditions")
            for required in (("\"site\" = 'a'",), ("\"label\" = '中文'",)):
                self.compare(next(queries), remote, local, seen, required=required)
            assert next(queries, None) is None, "Missing arithmetic request checks"
            self.compare_fixture("arithmetic_and_like")
        print("InfluxDB 1.8 arithmetic and LIKE .................. [ passed ]")

    def do_replanning_retry(self):
        for message in ("max-select-buckets limit exceeded", "max select bucket count exceeded: 10 buckets"):
            rejected = []

            def override(path):
                query = parse_qs(urlsplit(path).query).get("q", [""])[0]
                if "SUM(" in query.upper():
                    rejected.append(query)
                    return 200, json.dumps({"results": [{"statement_id": 0, "error": message}]}).encode()
                return None

            with self.fixture(response_override=override) as (remote, local, seen):
                self.compare(self.fixture_queries("replanning_retry")[0], remote, local, seen)
                requests = self.select_requests(seen)
                assert len(rejected) == 1 and len(requests) == 2, requests
                assert "SUM(" in requests[0].upper(), requests
                assert "SUM(" not in requests[1].upper(), requests
                self.compare_fixture("replanning_retry")

        with self.fixture() as (remote, local, seen):
            # Stay within the 64-element IN grammar while exceeding the encoded URL cap.
            values = ["'a'"] + ["'" + str(index) + "x" * 500 + "'" for index in range(40)]
            sql = f"SELECT i FROM {remote} WHERE site IN ({','.join(values)}) ORDER BY time"
            tdSql.query("EXPLAIN VERBOSE TRUE " + sql)
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            assert "generation failed" in plan.lower(), plan
            self.compare(sql, remote, local, seen, forbidden=(" WHERE ",))
            assert len(self.select_requests(seen)) == 1, seen

        for message in ("max number of points reached", "max select series count exceeded: 100 series",
                        "max-select-point limit exceeded", "max-select-series limit exceeded"):
            for status in (200, 400):
                def resource_error(path):
                    query = parse_qs(urlsplit(path).query).get("q", [""])[0]
                    if query.upper().startswith("SELECT "):
                        return status, json.dumps({"results": [{"statement_id": 0, "error": message}]}).encode()
                    return None

                with self.fixture(response_override=resource_error) as (_, _, seen):
                    tdSql.error(self.fixture_queries("replanning_retry")[0], expectedErrno=0x6404)
                    assert message in tdSql.error_info, tdSql.error_info
                    assert len(self.select_requests(seen)) == 1, seen

        def late_error(path):
            query = parse_qs(urlsplit(path).query).get("q", [""])[0]
            if query.upper().startswith("SELECT "):
                series = {"name": "rows", "columns": ["time", "i"],
                          "values": [[self.base + index, index + 3] for index in range(4096)]}
                rows = json.dumps({"results": [{"statement_id": 0, "series": [series]}]}).encode()
                error = b'{"results":[{"statement_id":0,"error":"error parsing query: late failure"}]}'
                return 200, rows + b"\n" + error
            return None

        with self.fixture(response_override=late_error) as (_, _, seen):
            tdSql.error(self.fixture_queries("predicates_and_limits")[0], expectedErrno=0x6409)
            assert len(self.select_requests(seen)) == 1, seen
        print("InfluxDB 1.8 limit errors and replanning ........... [ passed ]")

    @staticmethod
    def normalized_explain_sql(sql):
        # Static EXPLAIN may defer RP resolution to prepare time.
        sql = re.sub(r'(?<=\.)"(?:[^"\\]|\\.)*"(?=\.)', '"<rp>"', sql)
        return " ".join(sql.split())

    def do_explain_contract(self):
        with self.fixture() as (remote, _, seen):
            contracts = [
                (("WHERE", "ORDER BY time DESC", "LIMIT 2 OFFSET 1"), ("Aggregate", "Sort")),
                (("SUM(",), ("Aggregate",)),
                (('GROUP BY "site"', "SUM("), ("Aggregate",)),
                (("GROUP BY time(", "fill(none)", "time >=", "time <="), ("Aggregate", "Interval")),
            ]
            queries = self.fixture_queries("explain_contract")
            assert len(queries) == len(contracts)
            for sql, (clauses, local_operators) in zip(queries, contracts):
                snapshots = []
                for prefix in ("EXPLAIN VERBOSE TRUE ", "EXPLAIN ANALYZE VERBOSE TRUE "):
                    seen.clear()
                    tdSql.query(prefix + sql)
                    lines = [str(value) for row in tdSql.queryResult for value in row if value is not None]
                    plan = "\n".join(lines)
                    assert "generation failed" not in plan.lower(), (sql, plan)
                    assert "FederatedScan" in plan or "Federated Scan" in plan, (sql, plan)
                    remote_lines = [line.split("Remote SQL:", 1)[1].strip()
                                    for line in lines if "Remote SQL:" in line]
                    assert len(remote_lines) == 1, (sql, plan)
                    for clause in clauses:
                        assert clause.upper() in remote_lines[0].upper(), (sql, clause, plan)
                    local_plan = "\n".join(line for line in lines if "Remote SQL:" not in line)
                    for operator in local_operators:
                        assert not re.search(r"\b" + operator + r"\b", local_plan), (sql, operator, plan)
                    snapshots.append(self.normalized_explain_sql(remote_lines[0]))
                    if "ANALYZE" in prefix:
                        requests = self.select_requests(seen)
                        assert requests, (sql, seen)
                        for clause in clauses:
                            assert clause.upper() in requests[-1].upper(), (sql, clause, requests)
                assert snapshots[0] == snapshots[1], (sql, snapshots)
            self.compare_fixture("explain_contract")
        print("InfluxDB 1.8 static/runtime EXPLAIN ............... [ passed ]")

    @influx_versions("1.8")
    def test_predicates_and_limits(self):
        """Compare pushed predicates and limits with a local sparse-row mirror.

        1. Compare in/ans results and plans for numeric, string, boolean and missing-Tag predicates.
        2. Verify raw time order, LIMIT/OFFSET, sparse anchors and local fallback boundaries.
        3. Verify filtered temporal-join inputs and local UNION/UNION ALL execution.

        Catalog:
            - FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,influx18

        Jira: None

        History:
            - 2026-11-04 Simon Dominic Add InfluxDB 1.8 pushdown parity coverage
            - 2026-11-09 Simon Dominic Cover temporal-join aliases and local set operations
            - 2026-11-09 Simon Dominic Move SQL results and plans to in/ans fixtures

        """
        self.do_predicates_and_limits()

    @influx_versions("1.8")
    def test_mixed_predicates(self):
        """Keep a mixed AND predicate local using the shared FQ strategy.

        1. Compare in/ans plans and results for mixed predicates with aggregates and limits.
        2. Verify ordinary WHERE conditions preserve remote internal errors without retries.

        Catalog:
            - FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,influx18

        Jira: None

        History:
            - 2026-11-09 Simon Dominic Align mixed-predicate handling with InfluxDB 3.x

        """
        self.do_mixed_predicates()

    @influx_versions("1.8")
    def test_aggregates_and_windows(self):
        """Compare aggregate and interval results, including empty and future input.

        1. Compare in/ans aggregate and window plans/results, including sparse Fields and missing Tags.
        2. Verify empty, one-point and two-point STDDEV and explicit future-window bounds.
        3. Keep BIGINT AVG local for positive and negative sum overflow in aggregates, groups and windows.

        Catalog:
            - FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,influx18

        Jira: None

        History:
            - 2026-11-04 Simon Dominic Add InfluxDB 1.8 aggregate and interval parity
            - 2026-11-09 Simon Dominic Move SQL results and plans to in/ans fixtures
            - 2026-11-11 Simon Dominic Cover BIGINT AVG sum overflow with local execution

        """
        self.do_aggregates_and_windows()
        self.do_bigint_avg_overflow()

    @influx_versions("1.8")
    def test_arithmetic_and_like(self):
        """Compare permitted arithmetic and LIKE patterns with local evaluation.

        1. Compare in/ans plans/results for floating arithmetic, nonzero division and regex translation.
        2. Keep integer-only arithmetic and unsafe division behind local filtering.
        3. Push folded NCHAR Tag and Field equality constants without original literal text.

        Catalog:
            - FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,influx18

        Jira: None

        History:
            - 2026-11-04 Simon Dominic Add InfluxDB 1.8 arithmetic and LIKE parity
            - 2026-11-09 Simon Dominic Move SQL results and plans to in/ans fixtures

        """
        self.do_arithmetic_and_like()

    @influx_versions("1.8")
    def test_replanning_retry(self):
        """Replan server-limit failures before delivering any aggregate result.

        1. Reject remote aggregate requests with max-select-buckets errors.
        2. Require exactly one aggregate rejection followed by one raw-scan retry with equal results.
        3. Preserve point and series resource errors for HTTP 200/400 without replanning.
        4. Preserve a syntax error after a fetched block without whole-query replay.
        5. Fall back after prepare rejects an overlong InfluxQL URL, issuing one remote SELECT.

        Catalog:
            - FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,influx18

        Jira: None

        History:
            - 2026-11-04 Simon Dominic Cover InfluxDB server-limit replanning
            - 2026-11-09 Simon Dominic Move SQL results and plans to in/ans fixtures

        """
        self.do_replanning_retry()

    @influx_versions("1.8")
    def test_explain_contract(self):
        """Verify static and runtime plans describe the actual InfluxQL requests.

        1. Compare static and ANALYZE in/ans plans, allowing runtime RP resolution and timing changes.
        2. Require pushed raw ORDER/LIMIT, aggregate and interval clauses and absent local operators.

        Catalog:
            - FederatedQuery

        Since: v3.5.0.0

        Labels: common,ci,influx18

        Jira: None

        History:
            - 2026-11-04 Simon Dominic Verify InfluxDB 1.8 EXPLAIN contract
            - 2026-11-09 Simon Dominic Move SQL results and plans to in/ans fixtures

        """
        self.do_explain_contract()
