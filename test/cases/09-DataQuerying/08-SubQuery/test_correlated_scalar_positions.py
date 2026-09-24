import os

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql


POSITIONS = (
    "select", "where", "case", "aggregate", "group", "partition", "having",
    "window", "order", "left_join", "right_join", "full_join", "mixed",
)
CASE_NAMES = [f"correlated_scalar_{position}" for position in POSITIONS]


class TestCorrelatedScalarPositions:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_prepare_data(self):
        tdSql.prepare("correlated_scalar_positions", drop=True)
        tdSql.execute("create table readings (ts timestamp, device varchar(8), v int)")
        tdSql.execute("create table thresholds (ts timestamp, device varchar(8), lim int)")
        tdSql.execute(
            "insert into readings values "
            "('2026-09-17 00:00:10', 'd1', 10) "
            "('2026-09-17 00:00:20', 'd1', 20) "
            "('2026-09-17 00:01:10', 'd2', 30) "
            "('2026-09-17 00:01:20', 'd3', 40)"
        )
        tdSql.execute(
            "insert into thresholds values "
            "('2026-09-17 00:00:10', 'd1', 15) "
            "('2026-09-17 00:01:10', 'd2', 25) "
            "('2026-09-17 00:02:10', 'd4', 50)"
        )
        tdSql.execute(
            "create stable tagged_readings (ts timestamp, v int) "
            "tags (device varchar(8), zone int)"
        )
        tdSql.execute("create table tr_d1 using tagged_readings tags ('d1', 1)")
        tdSql.execute("create table tr_d2 using tagged_readings tags ('d2', 2)")
        tdSql.execute("create table tr_d3 using tagged_readings tags ('d3', 3)")
        tdSql.execute(
            "insert into tr_d1 values "
            "('2026-09-17 00:00:10', 10) "
            "('2026-09-17 00:00:20', 20)"
        )
        tdSql.execute("insert into tr_d2 values ('2026-09-17 00:01:10', 30)")
        tdSql.execute("insert into tr_d3 values ('2026-09-17 00:01:20', 40)")
        tdSql.execute(
            "create stable tagged_thresholds (ts timestamp, lim int) "
            "tags (device varchar(8), zone int)"
        )
        for table, device, zone, ts, lim in [
            ("tt_d1", "d1", 1, "2026-09-17 00:00:10", 15),
            ("tt_d2", "d2", 2, "2026-09-17 00:01:10", 25),
            ("tt_d4", "d4", 4, "2026-09-17 00:02:10", 50),
        ]:
            tdSql.execute(
                f"create table {table} using tagged_thresholds tags ('{device}', {zone})"
            )
            tdSql.execute(f"insert into {table} values ('{ts}', {lim})")
        tdSql.execute(
            "create table routed_readings "
            "(ts timestamp, target varchar(16), v int)"
        )
        tdSql.execute(
            "insert into routed_readings values "
            "('2026-09-17 00:00:10', 'tt_d1', 10) "
            "('2026-09-17 00:00:20', 'tt_d1', 20) "
            "('2026-09-17 00:01:10', 'tt_d2', 30) "
            "('2026-09-17 00:01:20', 'tt_d3', 40)"
        )

    def do_compare_results(self, case_name):
        case_dir = os.path.dirname(__file__)
        sql_file = os.path.join(case_dir, "in", f"{case_name}.in")
        ans_file = os.path.join(case_dir, "ans", f"{case_name}.ans")
        tdCom.compare_testcase_result(sql_file, ans_file, case_name)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self, before_test_class):
        self.do_prepare_data()
        # The in/ans helper launches taos with the psim client config.
        client_cfg = os.path.join(tdCom.getClientCfgPath(), "taos.cfg")
        with open(client_cfg, "a", encoding="utf-8") as cfg:
            cfg.write(f"firstEP {self.host}:{self.port}\nserverPort {self.port}\n")

    @pytest.mark.parametrize("case_name", CASE_NAMES, ids=CASE_NAMES)
    def test_correlated_scalar_positions(self, case_name):
        """Compare correlated scalar results across SQL expression positions.

        1. Each in/ans file fixes one expression position and covers three nesting depths.
        2. Within the file, vary scalar shape and bound outer columns.
        3. Cover tags, tbname, both JOIN inputs, NULLs and unmatched joins.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-17 Add correlated scalar position result matrix.

        """
        self.do_compare_results(case_name)

    def test_correlated_scalar_explain(self):
        """Compare correlated scalar EXPLAIN and ANALYZE output.

        1. Verify the owning operator and correlated template in static plans.
        2. Verify JOIN algorithms and plans for each expression stage.
        3. Verify ANALYZE input rows and actual calls without parameter values.
        4. Verify materialized reuse, InitPlan ownership and nested template plans.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-21 Add correlated scalar EXPLAIN result files.

        """
        self.do_compare_results("correlated_scalar_explain")
