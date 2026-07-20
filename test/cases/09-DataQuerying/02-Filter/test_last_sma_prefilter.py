import os
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

        for case_no, case in enumerate(cases, start=1):
            if case_no == 1 or case_no % 50 == 0:
                tdLog.info(f"running applicable case {case_no}/{len(cases)}: {case.case_id}")
            self._run_applicable_case(case, case_no)

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

    def _run_applicable_case(self, case: ApplicableCase, case_no: int):
        tb_name = f"ct_{case_no:04d}"
        grp_num = case_no
        grp5 = f"grp5_case_{case_no:04d}"
        rows = self._create_case_table_and_rows(tb_name, grp_num, grp5, case, case_no)
        sql = self._build_applicable_sql(case, grp5)
        expected = self._compute_expected(rows, case.last_cols)

        self._assert_query_result(case.case_id, sql, case.last_cols, expected)
        self._assert_hidden_not_null_plan(case.case_id, sql, case.last_cols, applicable=True)
        self._assert_explain_analyze(case.case_id, sql, case.last_cols, applicable=True)

    def _create_case_table_and_rows(
        self, tb_name: str, grp_num: int, grp5: str, case: ApplicableCase, case_no: int
    ) -> list[dict[str, object]]:
        tdSql.execute(f"create table {DB_NAME}.{tb_name} using {STB_NAME} tags ({grp_num}, '{grp5}')")
        rows: list[dict[str, object]] = []

        for block_name in BLOCK_NAMES:
            value_sqls: list[str] = []
            for row_idx in range(2):
                row = self._build_row(case, case_no, block_name, row_idx)
                rows.append(row)
                value_sqls.append(
                    "("
                    + ", ".join(
                        _fmt_sql_value(row[col_name])
                        for col_name in (
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
                    )
                    + ")"
                )
            tdSql.execute(f"insert into {DB_NAME}.{tb_name} values " + " ".join(value_sqls))
            tdSql.execute(f"flush database {DB_NAME}")

        return rows

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

    def _get_taosd_log_file(self, dnode_idx: int = 1) -> str:
        log_file = os.path.join(self.work_dir, f"dnode{dnode_idx}", "log", "taosdlog.0")
        assert os.path.exists(log_file), f"taosd log file not found: {log_file}"
        return log_file

    def _read_log_delta(self, log_file: str, offset: int) -> str:
        with open(log_file, "rb") as fp:
            fp.seek(offset)
            return fp.read().decode("utf-8", errors="ignore")

    def _run_query_with_log_capture(
        self,
        sql: str,
        expect_markers: tuple[str, ...] = (),
        retries: int = 20,
        interval: float = 0.2,
    ) -> str:
        log_file = self._get_taosd_log_file()
        offset = os.path.getsize(log_file)

        tdSql.query(sql, queryTimes=1)

        if not expect_markers:
            return self._read_log_delta(log_file, offset)

        log_text = ""
        for _ in range(retries):
            log_text = self._read_log_delta(log_file, offset)
            if all(marker in log_text for marker in expect_markers):
                return log_text
            time.sleep(interval)

        return log_text

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
        runtime_markers: tuple[str, ...] = (),
        forbidden_runtime_markers: tuple[str, ...] = (),
    ):
        log_text = self._run_query_with_log_capture(sql, expect_markers=runtime_markers)
        for marker in runtime_markers:
            assert marker in log_text, f"{case_id} missing runtime marker {marker}:\n{log_text}"
        for marker in forbidden_runtime_markers:
            assert marker not in log_text, (
                f"{case_id} should not contain runtime marker {marker}:\n{log_text}"
            )

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
            expect_err = "Not supported join since"
            tdSql.error(join_sql, expectErrInfo=expect_err, fullMatched=False)
            tdSql.error(f"explain verbose true {join_sql}", expectErrInfo=expect_err, fullMatched=False)
            tdSql.error(f"explain analyze verbose true {join_sql}", expectErrInfo=expect_err, fullMatched=False)

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
                        for col_name in (
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
                (),
                (),
            ),
            (
                "block_sma_mode__tag_in_applicable",
                f"select last(c_big) from {STB_NAME} "
                f"where {self._build_filter_mode_clause('scalar', grp5)} "
                f"and ts >= {TIME_RANGE_START} and ts <= {TIME_RANGE_END}",
                (30001,),
                True,
                (),
                (),
            ),
            (
                "block_sma_mode__data_predicate_inapplicable",
                f"select last(c_big) from {STB_NAME} "
                f"where grp5 = '{grp5}' and c_pred = 7",
                None,
                True,
                (
                    "last-sma-debug block SMA rejected block in filter path",
                    "data block filter out by block SMA",
                ),
                (),
            ),
            (
                "block_sma_mode__scalar_predicate_inapplicable",
                f"select last(c_big) from {STB_NAME} "
                f"where grp5 = '{grp5}' and abs(c_scalar) = 7",
                None,
                True,
                ("last-sma-debug block not-null group evaluated",),
                ("last-sma-debug block SMA rejected block in filter path",),
            ),
        ]

        for (
            case_id,
            sql,
            expected,
            expect_hidden_not_null,
            runtime_markers,
            forbidden_runtime_markers,
        ) in cases:
            self._assert_block_sma_mode_case(
                case_id,
                sql,
                expected,
                expect_hidden_not_null,
                runtime_markers,
                forbidden_runtime_markers,
            )
