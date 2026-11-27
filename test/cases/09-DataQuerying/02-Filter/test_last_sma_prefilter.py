import os
import re
import time
from dataclasses import dataclass
from typing import Callable, Optional, Tuple

from new_test_framework.utils import tdLog, tdSql


DB_NAME = "last_sma_prefilter"
STB_NAME = f"{DB_NAME}.st_last_sma_prefilter"

BLOCK_NAMES = ("old", "mid", "new")
BLOCK_INDEX = {name: idx for idx, name in enumerate(BLOCK_NAMES)}
BLOCK_BASE_TS = {
    "old": 1704067200000,
    "mid": 1704153600000,
    "new": 1704240000000,
}
TIME_RANGE_START = BLOCK_BASE_TS["old"]
TIME_RANGE_END = BLOCK_BASE_TS["new"] + 1

FILTER_MODE_DUMMIES = tuple(f"dummy{i:02d}" for i in range(1, 11))
FILTER_MODES = ("non_scalar", "scalar")
QUERY_SHAPES = ("time_only", "pred_only", "time_and_pred")
PRED_SHAPES = ("sma_friendly", "non_sma")
ROW_COLUMNS = (
    "ts",
    "c_big",
    "c_dbl",
    "c_bin",
    "c_nchar",
    "c_pred",
    "c_scalar",
    "c_noise1",
    "c_noise2",
    "c_keep",
)
TARGET_TYPES = (
    ("c_big",),
    ("c_bin",),
    ("c_big", "c_dbl"),
    ("c_big", "c_bin"),
    ("c_big", "c_nchar"),
    ("c_big", "c_dbl", "c_bin"),
)

def _fmt_sql_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return str(value)


def _same_value(actual, expected) -> bool:
    if actual is None or expected is None:
        return actual is expected
    if isinstance(expected, float):
        return abs(float(actual) - expected) <= 1e-9
    return actual == expected


def _build_big(case_no: int, block_name: str, row_idx: int) -> int:
    return case_no * 1000 + BLOCK_INDEX[block_name] * 10 + row_idx + 1


def _build_dbl(case_no: int, block_name: str, row_idx: int) -> float:
    return case_no * 100.0 + BLOCK_INDEX[block_name] * 10.0 + row_idx + 0.25


def _build_bin(case_no: int, block_name: str, row_idx: int) -> str:
    return f"bin_{case_no}_{block_name}_{row_idx}"


def _build_nchar(case_no: int, block_name: str, row_idx: int) -> str:
    return f"汉字_{case_no}_{block_name}_{row_idx}"


def _case_table_name(case_no: int) -> str:
    return f"ct_{case_no:04d}"


def _case_tag(case_no: int) -> str:
    return f"grp5_case_{case_no:04d}"


def _flatten_rows(rows_by_block: dict[str, list[dict]]) -> list[dict]:
    return [row for block_name in BLOCK_NAMES for row in rows_by_block[block_name]]


NON_NULL_BUILDERS: dict[str, Callable[[int, str, int], object]] = {
    "c_big": _build_big,
    "c_dbl": _build_dbl,
    "c_bin": _build_bin,
    "c_nchar": _build_nchar,
}


@dataclass(frozen=True)
class ApplicableCase:
    filter_mode: str
    last_cols: tuple[str, ...]
    null_pattern: str
    target_block: str
    query_shape: str
    pred_shape: str

    @property
    def case_id(self) -> str:
        type_part = "_".join(col.replace("c_", "") for col in self.last_cols)
        return (
            f"{self.filter_mode}__{type_part}__{self.null_pattern}"
            f"__{self.target_block}__{self.query_shape}__{self.pred_shape}"
        )


class TestLastSmaPrefilter:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_last_sma_prefilter(self):
        """Last SMA hidden not-null prefilter full matrix and regressions.

        Exercise applicable and inapplicable last SMA prefilters, nested queries,
        root OR predicates, last_row scans, and block SMA regressions.

        Since: v3.4.1.13

        Labels: ci,integration,functional,tsma,last

        Jira: None

        History:
            - 2026-07-15 wpan created
        """

        self._prepare_db()
        cases = list(self._iter_applicable_cases())
        limit = int(os.environ.get("LAST_SMA_PREFILTER_CASE_LIMIT", "0"))
        if limit > 0:
            cases = cases[:limit]
        tdLog.info(f"generated {len(cases)} applicable cases for last SMA prefilter")

        rows_by_case = self._prepare_applicable_data(cases)

        for case_no, case in enumerate(cases, start=1):
            if case_no == 1 or case_no % 50 == 0:
                tdLog.info(f"running applicable case {case_no}/{len(cases)}: {case.case_id}")
            self._run_applicable_case(case, case_no, rows_by_case[case_no])

        self._run_deep_nested_and_regression()
        self._run_root_or_shape_regression()
        self._run_last_row_scan_regression()
        self._run_inapplicable_cases()
        self._run_block_sma_mode_regression()

    def _prepare_db(self):
        tdSql.prepare(
            dbname=DB_NAME,
            drop=True,
            cachemodel="both",
            minrows=10,
            stt_trigger=1,
            vgroups=1,
        )
        tdSql.execute(
            f"create stable {STB_NAME} ("
            "ts timestamp, "
            "c_big bigint, "
            "c_dbl double, "
            "c_bin binary(32), "
            "c_nchar nchar(32), "
            "c_pred int, "
            "c_scalar int, "
            "c_noise1 int, "
            "c_noise2 int, "
            "c_keep int"
            ") tags (grp_num int, grp5 binary(32))"
        )
        tdSql.execute("alter local 'querySmaOptimize' '1'")

    def _iter_applicable_cases(self):
        for filter_mode in FILTER_MODES:
            for last_cols in TARGET_TYPES:
                width = len(last_cols)
                for mask in range(1 << width):
                    null_pattern = "".join(
                        "N" if (mask & (1 << idx)) else "A" for idx in range(width)
                    )
                    for target_block in BLOCK_NAMES:
                        for query_shape in QUERY_SHAPES:
                            for pred_shape in PRED_SHAPES:
                                yield ApplicableCase(
                                    filter_mode=filter_mode,
                                    last_cols=last_cols,
                                    null_pattern=null_pattern,
                                    target_block=target_block,
                                    query_shape=query_shape,
                                    pred_shape=pred_shape,
                                )

    def _run_applicable_case(self, case: ApplicableCase, case_no: int, rows: dict[str, list[dict]]):
        sql = self._build_applicable_sql(case, _case_tag(case_no))
        expected = self._compute_expected(_flatten_rows(rows), case.last_cols)

        self._assert_query_result(case.case_id, sql, case.last_cols, expected)
        self._assert_hidden_not_null_plan(case.case_id, sql, case.last_cols, applicable=True)
        self._assert_explain_analyze(case.case_id, sql, case.last_cols, applicable=True)

    def _prepare_applicable_data(self, cases: list[ApplicableCase]) -> dict[int, dict[str, list[dict]]]:
        """Create every case table first, then write block by block.

        `flush database` commits the whole vnode, so flushing once per block covers
        all case tables at once. Flushing per case instead makes every flush pay for
        the data accumulated by all previous cases, which grows quadratically with
        the case count and blows the CI time budget.
        """
        rows_by_case: dict[int, dict[str, list[dict]]] = {}
        for case_no, case in enumerate(cases, start=1):
            rows_by_case[case_no] = {
                block_name: [self._build_row(case, case_no, block_name, row_idx) for row_idx in range(2)]
                for block_name in BLOCK_NAMES
            }
            tdSql.execute(
                f"create table {DB_NAME}.{_case_table_name(case_no)} using {STB_NAME} "
                f"tags ({case_no}, '{_case_tag(case_no)}')"
            )

        for block_name in BLOCK_NAMES:
            for case_no in range(1, len(cases) + 1):
                value_sqls = [
                    "(" + ", ".join(_fmt_sql_value(row[col_name]) for col_name in ROW_COLUMNS) + ")"
                    for row in rows_by_case[case_no][block_name]
                ]
                tdSql.execute(
                    f"insert into {DB_NAME}.{_case_table_name(case_no)} values " + " ".join(value_sqls)
                )
            tdSql.execute(f"flush database {DB_NAME}")

        return rows_by_case

    def _build_row(
        self, case: ApplicableCase, case_no: int, block_name: str, row_idx: int
    ) -> dict[str, object]:
        row = {
            "ts": BLOCK_BASE_TS[block_name] + row_idx,
            "c_big": _build_big(case_no, block_name, row_idx),
            "c_dbl": _build_dbl(case_no, block_name, row_idx),
            "c_bin": _build_bin(case_no, block_name, row_idx),
            "c_nchar": _build_nchar(case_no, block_name, row_idx),
            "c_pred": 7,
            "c_scalar": -7,
            "c_noise1": 100 + BLOCK_INDEX[block_name],
            "c_noise2": 200 + BLOCK_INDEX[block_name],
            "c_keep": 1,
        }

        if block_name == case.target_block:
            for idx, col_name in enumerate(case.last_cols):
                if case.null_pattern[idx] == "A":
                    row[col_name] = None
                else:
                    row[col_name] = NON_NULL_BUILDERS[col_name](case_no, block_name, row_idx)

        return row

    def _build_applicable_sql(self, case: ApplicableCase, grp5: str) -> str:
        select_sql = ", ".join(f"last({col_name})" for col_name in case.last_cols)
        clauses = [self._build_filter_mode_clause(case.filter_mode, grp5)]

        if case.query_shape in ("time_only", "time_and_pred"):
            clauses.append(f"ts >= {TIME_RANGE_START}")
            clauses.append(f"ts <= {TIME_RANGE_END}")

        if case.query_shape in ("pred_only", "time_and_pred"):
            clauses.append(self._build_business_predicate(case.pred_shape))

        return f"select {select_sql} from {STB_NAME} where " + " and ".join(clauses)

    def _build_filter_mode_clause(self, filter_mode: str, grp5: str, col_name: str = "grp5") -> str:
        if filter_mode == "non_scalar":
            return f"{col_name} = '{grp5}'"

        values = [f"'{grp5}'"] + [f"'{dummy}'" for dummy in FILTER_MODE_DUMMIES]
        return f"{col_name} in (" + ", ".join(values) + ")"

    def _build_business_predicate(self, pred_shape: str) -> str:
        if pred_shape == "sma_friendly":
            return "c_pred = 7"
        return "abs(c_scalar) = 7"

    def _compute_expected(self, rows: list[dict[str, object]], last_cols: tuple[str, ...]) -> list[object]:
        ordered_rows = sorted(rows, key=lambda item: item["ts"], reverse=True)
        expected: list[object] = []

        for col_name in last_cols:
            value = None
            for row in ordered_rows:
                if row[col_name] is not None:
                    value = row[col_name]
                    break
            expected.append(value)

        return expected

    def _assert_query_result(
        self, case_id: str, sql: str, last_cols: tuple[str, ...], expected: list[object]
    ):
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)

        for col_idx, (col_name, exp_value) in enumerate(zip(last_cols, expected)):
            actual = tdSql.queryResult[0][col_idx]
            assert _same_value(actual, exp_value), (
                f"{case_id} result mismatch for {col_name}: sql={sql}, "
                f"expected={exp_value}, actual={actual}"
            )

    def _assert_expected_row(self, case_id: str, sql: str, expected: tuple[object, ...]):
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)

        for col_idx, exp_value in enumerate(expected):
            actual = tdSql.queryResult[0][col_idx]
            assert _same_value(actual, exp_value), (
                f"{case_id} result mismatch at col {col_idx}: sql={sql}, "
                f"expected={exp_value}, actual={actual}"
            )

    def _fetch_plan_lines(self, explain_sql: str) -> list[str]:
        tdSql.query(explain_sql, queryTimes=1)
        return [str(row[0]) for row in tdSql.queryResult]

    def _extract_filter_lines(self, plan_lines: list[str]) -> list[str]:
        return [line for line in plan_lines if "Filter:" in line]

    def _assert_hidden_not_null_plan(
        self,
        case_id: str,
        sql: str,
        last_cols: tuple[str, ...],
        applicable: bool,
        must_have_last_row_scan: bool = False,
    ):
        plan_lines = self._fetch_plan_lines(f"explain verbose true {sql}")
        plan_text = "\n".join(plan_lines)
        filter_lines = self._extract_filter_lines(plan_lines)

        if must_have_last_row_scan:
            assert "Last Row Scan" in plan_text, f"{case_id} should keep Last Row Scan path:\n{plan_text}"

        if applicable:
            assert filter_lines, f"{case_id} should have filter lines in explain:\n{plan_text}"
            filter_text = "\n".join(filter_lines)
            for col_name in last_cols:
                assert col_name in filter_text, f"{case_id} missing hidden not-null for {col_name}:\n{plan_text}"
                assert "IS NOT NULL" in filter_text, f"{case_id} missing IS NOT NULL marker:\n{plan_text}"
            if len(last_cols) > 1:
                assert " OR " in filter_text, f"{case_id} multi-last filter should use OR:\n{plan_text}"
        else:
            assert "IS NOT NULL" not in plan_text, (
                f"{case_id} should not inject hidden not-null filter:\n{plan_text}"
            )

    def _assert_explain_analyze(
        self, case_id: str, sql: str, last_cols: tuple[str, ...], applicable: bool
    ):
        plan_lines = self._fetch_plan_lines(f"explain analyze verbose true {sql}")
        plan_text = "\n".join(plan_lines)

        assert "Execution Time:" in plan_text, f"{case_id} explain analyze missing execution time:\n{plan_text}"
        assert "Planning Time:" in plan_text, f"{case_id} explain analyze missing planning time:\n{plan_text}"

        if applicable:
            filter_lines = self._extract_filter_lines(plan_lines)
            assert filter_lines, f"{case_id} explain analyze missing filter lines:\n{plan_text}"
            filter_text = "\n".join(filter_lines)
            for col_name in last_cols:
                assert col_name in filter_text, f"{case_id} explain analyze missing {col_name}:\n{plan_text}"
            assert "IS NOT NULL" in filter_text, f"{case_id} explain analyze missing IS NOT NULL:\n{plan_text}"

    def _assert_block_sma_mode_case(
        self,
        case_id: str,
        sql: str,
        expected: Optional[Tuple[object, ...]],
        expect_hidden_not_null: bool,
    ):
        tdSql.query(sql, queryTimes=1)

        if expected is None:
            tdSql.checkRows(0)
        else:
            tdSql.checkRows(1)
            for col_idx, exp_value in enumerate(expected):
                actual = tdSql.queryResult[0][col_idx]
                assert _same_value(actual, exp_value), (
                    f"{case_id} result mismatch at col {col_idx}: sql={sql}, "
                    f"expected={exp_value}, actual={actual}"
                )

        self._assert_hidden_not_null_plan(
            case_id, sql, ("c_big",), applicable=expect_hidden_not_null
        )
        self._assert_explain_analyze(
            case_id, sql, ("c_big",), applicable=expect_hidden_not_null
        )

    def _run_deep_nested_and_regression(self):
        tb_name = "ct_reg_nested"
        grp5 = "grp5_reg_nested"
        old_ts = BLOCK_BASE_TS["old"]
        mid_ts = BLOCK_BASE_TS["mid"]
        new_ts = BLOCK_BASE_TS["new"]
        tdSql.execute(f"create table {DB_NAME}.{tb_name} using {STB_NAME} tags (900001, '{grp5}')")

        tdSql.execute(
            f"insert into {DB_NAME}.{tb_name} values "
            f"({old_ts}, 11, 1.1, 'old_bin', '旧值', 7, -7, NULL, NULL, 1) "
            f"({old_ts + 1}, 12, 1.2, 'old_bin_1', '旧值1', 7, -7, NULL, NULL, 1)"
        )
        tdSql.execute(f"flush database {DB_NAME}")
        tdSql.execute(
            f"insert into {DB_NAME}.{tb_name} values "
            f"({mid_ts}, 21, 2.1, 'mid_bin', '中值', 7, -7, NULL, NULL, 1) "
            f"({mid_ts + 1}, 22, 2.2, 'mid_bin_1', '中值1', 7, -7, NULL, NULL, 1)"
        )
        tdSql.execute(f"flush database {DB_NAME}")
        tdSql.execute(
            f"insert into {DB_NAME}.{tb_name} values "
            f"({new_ts}, NULL, 3.1, 'new_bin', '新值', 7, -7, 1, NULL, 0) "
            f"({new_ts + 1}, NULL, 3.2, 'new_bin_1', '新值1', 7, -7, 1, NULL, 0)"
        )
        tdSql.execute(f"flush database {DB_NAME}")

        sql = (
            f"select last(c_big) from {STB_NAME} "
            f"where grp5 = '{grp5}' "
            f"and ts >= {TIME_RANGE_START} and ts <= {TIME_RANGE_END} "
            "and (c_keep = 1 or (abs(c_scalar) = 7 and (c_noise1 is not null or c_noise2 is not null)))"
        )
        self._assert_query_result("deep_nested_and_regression", sql, ("c_big",), [22])
        self._assert_explain_analyze("deep_nested_and_regression", sql, ("c_big",), applicable=True)

    def _run_root_or_shape_regression(self):
        tb_name = "ct_reg_root_or"
        grp5 = "grp5_reg_root_or"
        old_ts = BLOCK_BASE_TS["old"]
        mid_ts = BLOCK_BASE_TS["mid"]
        new_ts = BLOCK_BASE_TS["new"]
        tdSql.execute(f"create table {DB_NAME}.{tb_name} using {STB_NAME} tags (900004, '{grp5}')")
        tdSql.execute(
            f"insert into {DB_NAME}.{tb_name} values "
            f"({old_ts}, 11, 1.1, 'root_old', '旧根', 7, -7, NULL, NULL, 1) "
            f"({mid_ts}, 22, 2.2, 'root_mid', '中根', 7, -7, NULL, NULL, 1)"
        )
        tdSql.execute(f"flush database {DB_NAME}")
        tdSql.execute(
            f"insert into {DB_NAME}.{tb_name} values "
            f"({new_ts}, NULL, 9.9, 'root_new', '新根', 9, -9, 1, NULL, 0)"
        )
        tdSql.execute(f"flush database {DB_NAME}")

        sql = (
            f"select last(c_big), last(c_dbl) from {STB_NAME} "
            f"where (grp5 = '{grp5}' and ts >= {TIME_RANGE_START} and ts <= {TIME_RANGE_END}) "
            f"or (grp5 = '{grp5}' and c_keep = 0)"
        )
        self._assert_query_result("root_or_shape_regression", sql, ("c_big", "c_dbl"), [22, 9.9])
        self._assert_hidden_not_null_plan(
            "root_or_shape_regression", sql, ("c_big", "c_dbl"), applicable=True
        )
        self._assert_explain_analyze(
            "root_or_shape_regression", sql, ("c_big", "c_dbl"), applicable=True
        )

    def _run_last_row_scan_regression(self):
        tb_name = "ct_reg_lastrow"
        tdSql.execute(f"create table {DB_NAME}.{tb_name} using {STB_NAME} tags (900002, 'grp5_reg_lastrow')")
        tdSql.execute(
            f"insert into {DB_NAME}.{tb_name} values "
            f"({BLOCK_BASE_TS['old']}, 100, 10.0, 'row_bin_0', '行值0', 7, -7, 1, 1, 1) "
            f"({BLOCK_BASE_TS['new']}, 200, 20.0, 'row_bin_1', '行值1', 7, -7, 1, 1, 1)"
        )

        sql = f"select last(c_big) from {DB_NAME}.{tb_name}"
        self._assert_query_result("last_row_scan_regression", sql, ("c_big",), [200])
        self._assert_hidden_not_null_plan(
            "last_row_scan_regression",
            sql,
            ("c_big",),
            applicable=False,
            must_have_last_row_scan=True,
        )
        self._assert_explain_analyze("last_row_scan_regression", sql, ("c_big",), applicable=False)

    def _run_inapplicable_cases(self):
        tb_name = "ct_inapp"
        grp5 = "grp5_inapp"
        tdSql.execute(f"create table {DB_NAME}.{tb_name} using {STB_NAME} tags (900003, '{grp5}')")
        tdSql.execute(
            f"insert into {DB_NAME}.{tb_name} values "
            f"({BLOCK_BASE_TS['old']}, 10, 1.0, 'inapp_old', '无优化旧值', 7, -7, 1, 1, 1) "
            f"({BLOCK_BASE_TS['new']}, 20, 2.0, 'inapp_new', '无优化新值', 7, -7, 1, 1, 1)"
        )
        tdSql.execute(
            f"create table if not exists {DB_NAME}.dim_last_sma_prefilter "
            "(ts timestamp, grp_num int, note binary(32))"
        )
        tdSql.execute(
            f"insert into {DB_NAME}.dim_last_sma_prefilter values ({BLOCK_BASE_TS['old']}, 900003, 'join_hit')"
        )

        for filter_mode in FILTER_MODES:
            case_prefix = f"inapp__{filter_mode}"
            filter_clause = self._build_filter_mode_clause(filter_mode, grp5)
            range_clause = f"ts >= {TIME_RANGE_START} and ts <= {TIME_RANGE_END}"
            where_clause = f"{filter_clause} and {range_clause}"

            cases = [
                (
                    f"{case_prefix}__no_last",
                    f"select max(c_big) from {STB_NAME} where {where_clause}",
                    (20,),
                    False,
                ),
                (
                    f"{case_prefix}__other_agg_func",
                    f"select last(c_big), sum(c_pred) from {STB_NAME} where {where_clause}",
                    (20, 14),
                    False,
                ),
                (
                    f"{case_prefix}__wrapped_last_is_allowed_baseline",
                    f"select abs(last(c_big)) from {STB_NAME} where {where_clause}",
                    (20,),
                    True,
                ),
                (
                    f"{case_prefix}__last_arg_plus",
                    f"select last(c_big + 1) from {STB_NAME} where {where_clause}",
                    (21,),
                    False,
                ),
                (
                    f"{case_prefix}__last_arg_abs",
                    f"select last(abs(c_big)) from {STB_NAME} where {where_clause}",
                    (20,),
                    False,
                ),
                (
                    f"{case_prefix}__last_arg_tbname",
                    f"select last(tbname) from {STB_NAME} where {where_clause}",
                    (tb_name,),
                    False,
                ),
            ]

            for case_id, sql, expected, applicable in cases:
                self._assert_expected_row(case_id, sql, expected)
                self._assert_hidden_not_null_plan(case_id, sql, ("c_big",), applicable=applicable)
                self._assert_explain_analyze(case_id, sql, ("c_big",), applicable=applicable)

            join_case_id = f"{case_prefix}__join"
            join_sql = (
                "select last(s.c_big) "
                f"from {STB_NAME} s, {DB_NAME}.dim_last_sma_prefilter d "
                f"where {self._build_filter_mode_clause(filter_mode, grp5, 's.grp5')} "
                f"and s.ts >= {TIME_RANGE_START} and s.ts <= {TIME_RANGE_END} "
                "and s.grp_num = d.grp_num and d.note = 'join_hit'"
            )
            self._assert_expected_row(join_case_id, join_sql, (20,))
            self._assert_hidden_not_null_plan(join_case_id, join_sql, ("c_big",), applicable=False)
            self._assert_explain_analyze(join_case_id, join_sql, ("c_big",), applicable=False)

        sql = f"select last(c_big) from {DB_NAME}.{tb_name}"
        self._assert_expected_row("inapp__simple_no_filter", sql, (20,))
        self._assert_hidden_not_null_plan("inapp__simple_no_filter", sql, ("c_big",), applicable=False)
        self._assert_explain_analyze("inapp__simple_no_filter", sql, ("c_big",), applicable=False)

        nested_sql = (
            "select last(c_big) from ("
            f"select * from {STB_NAME} where grp5 = '{grp5}' order by ts asc limit 1"
            ")"
        )
        self._assert_expected_row("inapp__nested_limit", nested_sql, (10,))
        self._assert_hidden_not_null_plan("inapp__nested_limit", nested_sql, ("c_big",), applicable=False)
        self._assert_explain_analyze("inapp__nested_limit", nested_sql, ("c_big",), applicable=False)

        scalar_subquery_sql = (
            f"select c_big from {STB_NAME} where grp5 = '{grp5}' and c_big = ("
            f"select last(c_big) from {STB_NAME} where grp5 = '{grp5}'"
            ")"
        )
        self._assert_expected_row("inapp__scalar_subquery", scalar_subquery_sql, (20,))
        self._assert_hidden_not_null_plan(
            "inapp__scalar_subquery", scalar_subquery_sql, ("c_big",), applicable=False
        )
        self._assert_explain_analyze("inapp__scalar_subquery", scalar_subquery_sql, ("c_big",), applicable=False)

        partition_sql = f"select last(c_big) from {STB_NAME} where grp5 = '{grp5}' partition by grp5"
        self._assert_expected_row("inapp__partition_by", partition_sql, (20,))
        self._assert_hidden_not_null_plan(
            "inapp__partition_by", partition_sql, ("c_big",), applicable=False, must_have_last_row_scan=True
        )
        self._assert_explain_analyze("inapp__partition_by", partition_sql, ("c_big",), applicable=False)

        subquery_partition_sql = (
            "select last(c_big) from ("
            f"select ts, c_big, grp5 from {STB_NAME} where grp5 = '{grp5}' and c_big > 0"
            ") partition by grp5"
        )
        self._assert_expected_row("inapp__subquery_partition_by", subquery_partition_sql, (20,))
        self._assert_hidden_not_null_plan(
            "inapp__subquery_partition_by", subquery_partition_sql, ("c_big",), applicable=False
        )
        self._assert_explain_analyze("inapp__subquery_partition_by", subquery_partition_sql, ("c_big",), applicable=False)

    def _run_block_sma_mode_regression(self):
        tb_name = "ct_block_sma_mode"
        grp5 = "grp5_block_sma_mode"
        tdSql.execute(f"create table {DB_NAME}.{tb_name} using {STB_NAME} tags (900005, '{grp5}')")

        block_specs = (
            ("old", 10000, 1, -1),
            ("mid", None, 2, -2),
            ("new", 30000, 3, -3),
        )
        for block_name, big_base, pred_value, scalar_value in block_specs:
            value_sqls: list[str] = []
            for row_idx in range(200):
                row = {
                    "ts": BLOCK_BASE_TS[block_name] + row_idx,
                    "c_big": None if big_base is None else big_base + row_idx,
                    "c_dbl": float((big_base or 20000) + row_idx) + 0.25,
                    "c_bin": f"mode_{block_name}_{row_idx}",
                    "c_nchar": f"模式_{block_name}_{row_idx}",
                    "c_pred": pred_value,
                    "c_scalar": scalar_value,
                    "c_noise1": row_idx,
                    "c_noise2": row_idx + 1,
                    "c_keep": 1,
                }
                value_sqls.append(
                    "("
                    + ", ".join(
                        _fmt_sql_value(row[col_name])
                        for col_name in ROW_COLUMNS
                    )
                    + ")"
                )

            tdSql.execute(f"insert into {DB_NAME}.{tb_name} values " + " ".join(value_sqls))
            tdSql.execute(f"flush database {DB_NAME}")

        cases = [
            (
                "block_sma_mode__tag_eq_applicable",
                f"select last(c_big) from {STB_NAME} "
                f"where grp5 = '{grp5}' and ts >= {TIME_RANGE_START} and ts <= {TIME_RANGE_END}",
                (30001,),
                True,
            ),
            (
                "block_sma_mode__tag_in_applicable",
                f"select last(c_big) from {STB_NAME} "
                f"where {self._build_filter_mode_clause('scalar', grp5)} "
                f"and ts >= {TIME_RANGE_START} and ts <= {TIME_RANGE_END}",
                (30001,),
                True,
            ),
            (
                "block_sma_mode__data_predicate_inapplicable",
                f"select last(c_big) from {STB_NAME} "
                f"where grp5 = '{grp5}' and c_pred = 7",
                None,
                True,
            ),
            (
                "block_sma_mode__scalar_predicate_inapplicable",
                f"select last(c_big) from {STB_NAME} "
                f"where grp5 = '{grp5}' and abs(c_scalar) = 7",
                None,
                True,
            ),
        ]

        for case_id, sql, expected, expect_hidden_not_null in cases:
            self._assert_block_sma_mode_case(
                case_id,
                sql,
                expected,
                expect_hidden_not_null,
            )

    def test_last_scan_conditions(self):
        """LAST conditional scans preserve filtered record selection.

        1. Check ranges, filters, mixed NULL inputs and associated outputs.
        2. Check composite keys and grouped tables before and after flush.

        Catalog:
            - Query

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Tony Zhang added conditional LAST scan regressions
        """
        self.do_last_scan_conditions()

    def test_last_scan_missing_sma(self):
        """LAST preserves values in blocks without SMA on the input column.

        Read a flushed table whose SMA list includes only the filter and key.

        Since: v3.5.0.0

        Labels: common,ci
        """
        self.do_last_scan_missing_sma()

    def test_last_scan_all_null_group(self):
        """Grouped LAST preserves a tag group with only NULL input values.

        Read a flushed supertable with default block statistics and no WHERE.

        Since: v3.5.0.0

        Labels: common,ci
        """
        self.do_last_scan_all_null_group()

    def _last_scan_wait_for_data_blocks(self, sql):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            tdSql.query(f"explain analyze verbose true {sql}")
            plan = "\n".join(row[0] for row in tdSql.queryResult)
            if re.search(r"file_load_blocks=[1-9]\d*\b", plan) and re.search(
                r"stt_load_blocks=0\s+mem_load_blocks=0.*composed_blocks=0\b",
                plan,
            ):
                return
            time.sleep(0.2)
        tdLog.exit(f"Expected a scan of clean DATA blocks for {sql}:\n{plan}")

    def do_last_scan_missing_sma(self):
        db = "last_scan_missing_sma"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} cachemodel 'none' keep 36500 "
            "vgroups 1 stt_trigger 1 minrows 100 maxrows 256"
        )
        try:
            tdSql.execute(
                f"create table {db}.t (ts timestamp, v int, f int) sma(ts, f)"
            )
            values = " ".join(
                f"({1704067200000 + i * 1000},{i + 1},{i % 2})"
                for i in range(512)
            )
            tdSql.execute(f"insert into {db}.t values {values}")
            tdSql.execute(
                f"create table {db}.staggered (ts timestamp, v int, w int, "
                "f int) sma(ts, w, f)"
            )
            values = " ".join(
                f"({1704067200000 + i * 1000},"
                f"{i + 1 if i < 256 else 'NULL'},{i + 1},{i % 2})"
                for i in range(512)
            )
            tdSql.execute(f"insert into {db}.staggered values {values}")
            tdSql.execute(f"flush database {db}")
            sql = f"select last(v) from {db}.t where f = 1"
            self._last_scan_wait_for_data_blocks(sql)
            self._last_scan_check(sql, [(512,)])

            # w has a result in the newest block, while v needs an older block.
            sql = f"select last(v), last(w) from {db}.staggered where f = 1"
            self._last_scan_wait_for_data_blocks(sql)
            self._last_scan_check(sql, [(256, 512)])
        finally:
            tdSql.execute(f"drop database if exists {db}")
        print("last scan without input SMA ................. [ passed ]")

    def do_last_scan_all_null_group(self):
        db = "last_scan_all_null_group"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} cachemodel 'none' keep 36500 "
            "vgroups 1 stt_trigger 1 minrows 100 maxrows 256"
        )
        try:
            tdSql.execute(f"create stable {db}.st (ts timestamp, v int) tags(g int)")
            tdSql.execute(f"create table {db}.t using {db}.st tags(42)")
            values = " ".join(
                f"({1704067200000 + i * 1000},NULL)" for i in range(512)
            )
            tdSql.execute(f"insert into {db}.t values {values}")
            tdSql.execute(f"flush database {db}")
            sql = f"select g, last(v) from {db}.st group by g"
            self._last_scan_wait_for_data_blocks(sql)
            self._last_scan_check(sql, [(42, None)])
        finally:
            tdSql.execute(f"drop database if exists {db}")
        print("last scan all-NULL tag group ................ [ passed ]")

    def _last_scan_check(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    def _last_scan_expected(self, rows, functions):
        if not rows:
            return []
        ordered = sorted(
            rows, key=lambda row: (row["ts"], row.get("k", 0)),
            reverse=True,
        )
        result = []
        for function, column in functions:
            candidates = ordered
            if function == "last":
                candidates = [row for row in ordered
                              if row[column] is not None]
            result.append(candidates[0][column] if candidates else None)
        return [tuple(result)]

    def _last_scan_query_shapes(self, table, where, rows):
        shapes = (
            (("last", "v"),),
            (("last_row", "r"),),
            (("last", "v"), ("last_row", "r")),
            (("last_row", "note"), ("last", "r"), ("last", "v")),
        )
        for functions in shapes:
            select = ", ".join(f"{fn}({col})" for fn, col in functions)
            self._last_scan_check(
                f"select {select} from {table} where {where}",
                self._last_scan_expected(rows, functions),
            )

    def do_last_scan_prepare(self):
        self.last_scan_db = "last_scan_conditions"
        self.last_scan_base = 1704067200000
        self.last_scan_rows = {}
        self.last_scan_pk_rows = []
        db = self.last_scan_db
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} cachemodel 'none' keep 36500 "
            "vgroups 1 minrows 10 maxrows 200"
        )
        tdSql.execute(
            f"create stable {db}.st (ts timestamp, v int, r int, "
            "pick int, note varchar(32)) tags (g int)"
        )
        for group, table in ((1, "ls_a"), (2, "ls_b")):
            tdSql.execute(
                f"create table {db}.{table} using {db}.st tags ({group})"
            )
            rows = []
            for index in range(240):
                rows.append({
                    "ts": self.last_scan_base + index * 1000,
                    "v": None if index >= 220 or index % 7 == 0
                    else group * 1000 + index,
                    "r": None if index >= 230 else group * 10000 + index,
                    "pick": None if index % 11 == 0 else index % 4,
                    "note": f"row_{group}_{index}",
                })
            values = " ".join(
                "(" + ",".join(_fmt_sql_value(row[col])
                               for col in ("ts", "v", "r", "pick", "note"))
                + ")" for row in rows
            )
            tdSql.execute(f"insert into {db}.{table} values {values}")
            self.last_scan_rows[table] = rows
        tdSql.execute(
            f"create table {db}.pk (ts timestamp, k int primary key, "
            "v int, r int, pick int, note varchar(32))"
        )
        for index in range(120):
            for key in range(4):
                self.last_scan_pk_rows.append({
                    "ts": self.last_scan_base + index * 1000,
                    "k": key,
                    "v": None if index >= 110 or
                    (key == 3 and index % 3 == 0) else index * 10 + key,
                    "r": None if index >= 115 else index * 100 + key,
                    "pick": None if (index + key) % 17 == 0 else key % 2,
                    "note": f"pk_{index}_{key}",
                })
        values = " ".join(
            "(" + ",".join(_fmt_sql_value(row[col]) for col in
                           ("ts", "k", "v", "r", "pick", "note"))
            + ")" for row in self.last_scan_pk_rows
        )
        tdSql.execute(f"insert into {db}.pk values {values}")
        print("last scan deterministic data ................ [ passed ]")

    def do_last_scan_ranges(self):
        base = self.last_scan_base
        table = f"{self.last_scan_db}.ls_a"
        all_rows = self.last_scan_rows["ls_a"]
        ranges = (
            (f"ts >= {base} and ts <= {base + 239000}",
             lambda ts: base <= ts <= base + 239000),
            (f"ts >= {base + 10000} and ts <= {base + 20000}",
             lambda ts: base + 10000 <= ts <= base + 20000),
            (f"ts > {base + 10000} and ts < {base + 20000}",
             lambda ts: base + 10000 < ts < base + 20000),
            (f"ts = {base + 10000}",
             lambda ts: ts == base + 10000),
            (f"ts > {base + 10000} and ts < {base + 11000}",
             lambda ts: base + 10000 < ts < base + 11000),
            (f"ts >= {base + 240000} and ts <= {base + 250000}",
             lambda ts: base + 240000 <= ts <= base + 250000),
        )
        for where, matches in ranges:
            rows = [row for row in all_rows if matches(row["ts"])]
            self._last_scan_query_shapes(table, where, rows)
        print("last scan closed ranges and holes ........... [ passed ]")

    def do_last_scan_filters(self):
        db = self.last_scan_db
        base = self.last_scan_base
        all_rows = self.last_scan_rows["ls_a"]
        predicates = (
            ("pick = 1", lambda row: row["pick"] == 1),
            ("pick is null", lambda row: row["pick"] is None),
            ("abs(pick) = 2", lambda row: row["pick"] == 2),
            ("(pick = 1 or pick is null) and v >= 1050",
             lambda row: (row["pick"] == 1 or row["pick"] is None)
             and row["v"] is not None and row["v"] >= 1050),
            ("(pick = 2 and r < 10100) or pick = 3",
             lambda row: (row["pick"] == 2 and row["r"] is not None
                          and row["r"] < 10100) or row["pick"] == 3),
            ("pick = 99", lambda row: row["pick"] == 99),
        )
        for predicate, matches in predicates:
            where = (f"ts >= {base} and ts <= {base + 239000} "
                     f"and ({predicate})")
            rows = [row for row in all_rows if matches(row)]
            self._last_scan_query_shapes(f"{db}.ls_a", where, rows)
        rows = [row for row in all_rows if row["pick"] == 1]
        latest = rows[-1]
        last_value = [row for row in rows if row["v"] is not None][-1]
        self._last_scan_check(
            f"select last_row(r), note from {db}.ls_a where pick = 1",
            [(latest["r"], latest["note"])],
        )
        self._last_scan_check(
            f"select last(v), note from {db}.ls_a where pick = 1",
            [(last_value["v"], last_value["note"])],
        )
        self._last_scan_check(
            f"select last(v), last_row(r) from {db}.ls_a "
            "where pick is null and v is null",
            [(None, None)],
        )
        print("last scan predicates and NULL association ... [ passed ]")

    def do_last_scan_primary_keys(self):
        table = f"{self.last_scan_db}.pk"
        base = self.last_scan_base
        all_rows = self.last_scan_pk_rows
        predicates = (
            (f"ts >= {base} and ts <= {base + 119000}",
             lambda row: True),
            (f"ts = {base + 42000}",
             lambda row: row["ts"] == base + 42000),
            (f"ts > {base + 42000} and ts <= {base + 44000}",
             lambda row: base + 42000 < row["ts"] <= base + 44000),
            (f"ts > {base + 42000} and ts < {base + 43000}",
             lambda row: base + 42000 < row["ts"] < base + 43000),
            ("k = 2", lambda row: row["k"] == 2),
            ("k < 3 and pick = 0",
             lambda row: row["k"] < 3 and row["pick"] == 0),
            ("(k = 1 or k = 3) and pick is not null",
             lambda row: row["k"] in (1, 3) and row["pick"] is not None),
        )
        for where, matches in predicates:
            rows = [row for row in all_rows if matches(row)]
            # Keep same-column mixed PK semantics outside this regression.
            for function, column in (("last", "v"), ("last_row", "r")):
                self._last_scan_check(
                    f"select {function}({column}) from {table} "
                    f"where {where}",
                    self._last_scan_expected(rows, ((function, column),)),
                )
        latest = [row for row in all_rows if row["k"] < 3][-1]
        self._last_scan_check(
            f"select last_row(r), k, note from {table} where k < 3",
            [(latest["r"], latest["k"], latest["note"])],
        )
        print("last scan composite key bounds and filter ... [ passed ]")

    def do_last_scan_groups(self):
        db = self.last_scan_db
        tag_expected = []
        table_expected = []
        for group, table in ((1, "ls_a"), (2, "ls_b")):
            rows = [row for row in self.last_scan_rows[table]
                    if row["pick"] == 1]
            result = self._last_scan_expected(
                rows, (("last", "v"), ("last_row", "r")),
            )[0]
            tag_expected.append((group, *result))
            table_expected.append(
                (table, rows[-1]["r"], rows[-1]["note"])
            )
        self._last_scan_check(
            f"select g, last(v), last_row(r) from {db}.st "
            "where pick = 1 partition by g order by g",
            tag_expected,
        )
        self._last_scan_check(
            f"select tbname, last_row(r), note from {db}.st "
            "where pick = 1 and tbname in ('ls_a', 'ls_b') "
            "partition by tbname order by tbname",
            table_expected,
        )
        rows = [row for row in self.last_scan_rows["ls_b"]
                if row["pick"] is None]
        self._last_scan_query_shapes(
            f"{db}.st", "g = 2 and pick is null", rows,
        )
        print("last scan tag and table groups .............. [ passed ]")

    def do_last_scan_conditions(self):
        self.do_last_scan_prepare()
        try:
            for phase in ("memory", "flushed"):
                if phase == "flushed":
                    tdSql.execute(f"flush database {self.last_scan_db}")
                tdLog.info(f"LAST conditional scan phase: {phase}")
                self.do_last_scan_ranges()
                self.do_last_scan_filters()
                self.do_last_scan_primary_keys()
                self.do_last_scan_groups()
        finally:
            tdSql.execute(f"drop database if exists {self.last_scan_db}")
        print("last conditional scans ...................... [ passed ]")
