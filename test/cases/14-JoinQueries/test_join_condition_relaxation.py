import os
import time
import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql


class TestJoinConditionRelaxation:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def prepare(self):
        tdSql.execute("drop database if exists join_condition_relaxation")
        tdSql.execute("create database join_condition_relaxation vgroups 2")
        tdSql.execute("use join_condition_relaxation")
        tdSql.execute(
            "create table l_expr("
            "ts timestamp, id int, ts2 timestamp, k int, x int, y int, name varchar(20))"
        )
        tdSql.execute(
            "create table r_expr("
            "ts timestamp, id int, ts2 timestamp, k int, x int, y int, name varchar(20))"
        )
        tdSql.execute(
            "insert into l_expr values"
            "('2025-01-01 00:00:00', 1, '2025-01-01 01:00:10', 1, 2, 3, 'Alpha')"
            "('2025-01-01 00:00:01', 2, '2025-01-01 01:00:11', 2, 3, 4, 'Beta')"
            "('2025-01-01 00:00:02', 3, '2025-01-01 01:00:11', 2, 4, 3, 'BETA')"
            "('2025-01-01 00:00:03', 4, null, null, null, 8, null)"
            "('2025-01-01 00:00:04', 5, '2025-01-01 01:00:14', 5, -6, 1, 'Gamma')"
        )
        tdSql.execute(
            "insert into r_expr values"
            "('2025-01-01 00:10:00', 11, '2025-01-01 01:00:09', 2, 5, 0, 'alpha')"
            "('2025-01-01 00:10:01', 12, '2025-01-01 01:00:10', 3, 7, 0, 'Beta')"
            "('2025-01-01 00:10:02', 13, '2025-01-01 01:00:10', 3, 7, 1, 'BETA')"
            "('2025-01-01 00:10:03', 14, null, null, null, 2, null)"
            "('2025-01-01 00:10:04', 15, '2025-01-01 01:00:13', 6, 5, 2, 'gamma')"
        )
        tdSql.execute(
            "create table l_range("
            "ts timestamp, id int, v int, lo int, hi int, grp int, name varchar(20))"
        )
        tdSql.execute(
            "create table r_range("
            "ts timestamp, id int, v int, lo int, hi int, grp int, name varchar(20))"
        )
        tdSql.execute(
            "insert into l_range values"
            "('2025-02-01 00:00:00', 1, -2, -3, 0, 1, 'Alpha')"
            "('2025-02-01 00:00:01', 2, 0, -1, 2, 1, 'Beta')"
            "('2025-02-01 00:00:02', 3, 2, 1, 4, 2, 'BETA')"
            "('2025-02-01 00:00:03', 4, 2, 2, 2, 2, null)"
            "('2025-02-01 00:00:04', 5, null, null, null, 3, 'Gamma')"
        )
        tdSql.execute(
            "insert into r_range values"
            "('2025-02-01 00:00:00', 11, -1, -2, 1, 1, 'alpha')"
            "('2025-02-01 00:00:02', 12, 1, 0, 2, 1, 'Beta')"
            "('2025-02-01 00:00:03', 13, 2, 1, 3, 2, 'BETA')"
            "('2025-02-01 00:00:05', 14, 4, 3, 5, 2, null)"
            "('2025-02-01 00:00:06', 15, null, null, null, 3, 'gamma')"
        )

    @staticmethod
    def query_rows(sql):
        tdSql.query(sql)
        return list(tdSql.queryResult)

    def check_query_equal(self, actual_sql, reference_sql):
        actual = self.query_rows(actual_sql)
        expected = self.query_rows(reference_sql)
        tdSql.checkEqual(actual, expected)

    def check_plan(self, sql, algorithm, require_sort=False):
        plan = "\n".join(
            "|".join(str(value) for value in row)
            for row in self.query_rows("explain " + sql)
        )
        assert f"algo={algorithm}" in plan, plan
        if require_sort:
            assert "Sort" in plan, plan

    def run_case_file(self, case_name):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        in_file = os.path.join(base_dir, "in", f"{case_name}.in")
        ans_file = os.path.join(base_dir, "ans", f"{case_name}.ans")
        if not os.path.isfile(in_file) or not os.path.isfile(ans_file):
            tdLog.exit(f"missing join snapshot: {in_file}, {ans_file}")
        tdCom.compare_testcase_result(in_file, ans_file, case_name)

    # --- impl ---

    def do_hash_arithmetic_keys(self):
        cases = [
            (
                "a.k + 1 = b.k",
                "(select id, k + 1 as ek from l_expr) a join r_expr b on a.ek = b.k",
            ),
            (
                "a.x + a.y = b.x",
                "(select id, x + y as ek from l_expr) a join r_expr b on a.ek = b.x",
            ),
            (
                "abs(a.x) = b.x",
                "(select id, abs(x) as ek from l_expr) a join r_expr b on a.ek = b.x",
            ),
            (
                "cast(a.k as bigint) = cast(b.k - 1 as bigint)",
                "(select id, cast(k as bigint) as ek from l_expr) a join "
                "(select id, cast(k - 1 as bigint) as ek from r_expr) b on a.ek = b.ek",
            ),
        ]
        for condition, reference_from in cases:
            actual = (
                "select /*+ join_algo(hash) */ a.id, b.id from l_expr a join r_expr b on "
                + condition
                + " order by 1, 2"
            )
            reference = "select a.id, b.id from " + reference_from + " order by 1, 2"
            self.check_query_equal(actual, reference)
            self.check_plan(actual.removesuffix(" order by 1, 2"), "Hash")
        print("L1 Hash arithmetic expression keys ........ [ passed ]")

    def do_hash_timestamp_and_string_keys(self):
        cases = [
            (
                "a.ts2 - 1s = b.ts2",
                "(select id, ts2 - 1s as ek from l_expr) a join r_expr b on a.ek = b.ts2",
            ),
            (
                "a.ts2 - 1s = b.ts2 + 1s",
                "(select id, ts2 - 1s as ek from l_expr) a join "
                "(select id, ts2 + 1s as ek from r_expr) b on a.ek = b.ek",
            ),
            (
                "lower(a.name) = lower(b.name)",
                "(select id, lower(name) as ek from l_expr) a join "
                "(select id, lower(name) as ek from r_expr) b on a.ek = b.ek",
            ),
        ]
        for condition, reference_from in cases:
            actual = "select /*+ join_algo(hash) */ a.id, b.id from l_expr a join r_expr b on " + condition + " order by 1, 2"
            reference = "select a.id, b.id from " + reference_from + " order by 1, 2"
            self.check_query_equal(actual, reference)
            self.check_plan(actual.removesuffix(" order by 1, 2"), "Hash")
        print("L1 Hash timestamp and string keys ......... [ passed ]")

    def do_hash_composite_and_residual_keys(self):
        actual = (
            "select a.id, b.id from l_expr a join r_expr b "
            "on a.k + 1 = b.k and lower(a.name) = lower(b.name) and a.y < b.y + 4 "
            "order by 1, 2"
        )
        reference = (
            "select a.id, b.id from "
            "(select id, k + 1 as k1, lower(name) as n, y from l_expr) a join "
            "(select id, k, lower(name) as n, y from r_expr) b "
            "on a.k1 = b.k and a.n = b.n and a.y < b.y + 4 order by 1, 2"
        )
        self.check_query_equal(actual, reference)
        self.check_plan(actual.removesuffix(" order by 1, 2"), "Hash")
        print("L1 Hash composite and residual keys ....... [ passed ]")

    def do_hash_join_types(self):
        join_types = [
            "left join",
            "right join",
            "full join",
            "left semi join",
            "left anti join",
            "right semi join",
            "right anti join",
        ]
        for join_type in join_types:
            if "semi" in join_type or "anti" in join_type:
                select_list = "a.id" if join_type.startswith("left") else "b.id"
                order_by = " order by 1"
            else:
                select_list = "a.id, b.id"
                order_by = " order by 1, 2"
            actual = (
                f"select {select_list} from l_expr a {join_type} r_expr b "
                f"on a.k + 1 = b.k{order_by}"
            )
            reference = (
                f"select {select_list} from (select id, k + 1 as ek from l_expr) a "
                f"{join_type} r_expr b on a.ek = b.k{order_by}"
            )
            self.check_query_equal(actual, reference)
        print("L1 Hash all regular Join types ............ [ passed ]")

    def do_l1_hash_join_type_edges(self):
        join_types = [
            "join",
            "left join",
            "right join",
            "full join",
            "left semi join",
            "left anti join",
            "right semi join",
            "right anti join",
        ]
        for join_type in join_types:
            if "semi" in join_type or "anti" in join_type:
                select_list = "a.id" if join_type.startswith("left") else "b.id"
                order_by = " order by 1"
            else:
                select_list = "a.id, b.id"
                order_by = " order by 1, 2"
            actual = (
                f"select /*+ join_algo(hash) */ {select_list} from l_expr a {join_type} r_expr b "
                f"on a.k + 1 = b.k and a.y < b.y + 4{order_by}"
            )
            reference = (
                f"select {select_list} from (select id, k + 1 as ek, y from l_expr) a "
                f"{join_type} r_expr b on a.ek = b.k and a.y < b.y + 4{order_by}"
            )
            self.check_query_equal(actual, reference)
            self.check_plan(actual.removesuffix(order_by), "Hash")

        empty_side_cases = [
            (
                "select a.id, b.id from (select * from l_expr where 1 = 0) a "
                "full join r_expr b on a.k + 1 = b.k order by 1, 2",
                "select a.id, b.id from (select id, k + 1 as ek from l_expr where 1 = 0) a "
                "full join r_expr b on a.ek = b.k order by 1, 2",
            ),
            (
                "select a.id, b.id from l_expr a full join "
                "(select * from r_expr where 1 = 0) b on a.k + 1 = b.k order by 1, 2",
                "select a.id, b.id from (select id, k + 1 as ek from l_expr) a full join "
                "(select * from r_expr where 1 = 0) b on a.ek = b.k order by 1, 2",
            ),
        ]
        for actual, reference in empty_side_cases:
            self.check_query_equal(actual, reference)
        print("L1 Hash Join-type edge matrix ............. [ passed ]")

    def do_l1_reference_oracle(self):
        join_types = [
            "join",
            "left join",
            "right join",
            "full join",
            "left semi join",
            "left anti join",
            "right semi join",
            "right anti join",
        ]
        for join_type in join_types:
            if "semi" in join_type or "anti" in join_type:
                select_list = "a.id" if join_type.startswith("left") else "b.id"
                order_by = " order by 1"
            else:
                select_list = "a.id, b.id"
                order_by = " order by 1, 2"
            actual = (
                f"select {select_list} from l_expr a {join_type} r_expr b "
                f"on a.x + a.y = b.x{order_by}"
            )
            reference = (
                f"select {select_list} from (select id, x + y as ek from l_expr) a "
                f"{join_type} r_expr b on a.ek = b.x{order_by}"
            )
            self.check_query_equal(actual, reference)

        cases = [
            (
                "select a.id, b.id from l_expr a join r_expr b "
                "on a.k + 1 = b.k and lower(a.name) = lower(b.name) order by 1, 2",
                "select a.id, b.id from (select id, k + 1 ek, lower(name) en from l_expr) a "
                "join (select id, k, lower(name) en from r_expr) b "
                "on a.ek = b.k and a.en = b.en order by 1, 2",
            ),
            (
                "select a.id, b.id from (select distinct id, k from l_expr) a join "
                "(select id, k from r_expr order by id desc limit 4) b "
                "on a.k + 1 = b.k order by 1, 2",
                "select a.id, b.id from (select distinct id, k + 1 ek from l_expr) a join "
                "(select id, k from r_expr order by id desc limit 4) b "
                "on a.ek = b.k order by 1, 2",
            ),
            (
                "select a.id, count(*), min(b.id), max(b.id) from l_expr a join r_expr b "
                "on a.k + 1 = b.k group by a.id order by 1",
                "select a.id, count(*), min(b.id), max(b.id) from "
                "(select id, k + 1 ek from l_expr) a join r_expr b "
                "on a.ek = b.k group by a.id order by 1",
            ),
            (
                "select a.id, b.id, a.k + 1, b.k from l_expr a join r_expr b "
                "on a.k + 1 = b.k order by 1, 2",
                "select a.id, b.id, a.ek, b.k from "
                "(select id, k + 1 ek from l_expr) a join r_expr b "
                "on a.ek = b.k order by 1, 2",
            ),
        ]
        for actual, reference in cases:
            self.check_query_equal(actual, reference)
        print("L1 explicit reference oracle ............. [ passed ]")

    def do_merge_timestamp_keys(self):
        cases = [
            (
                "a.ts2 - 1s = b.ts2",
                "(select id, ts2 - 1s as ek from l_expr) a join r_expr b on a.ek = b.ts2",
                "a.ek is not null and b.ts2 is not null",
            ),
            (
                "a.ts2 - 1s = b.ts2 + 1s",
                "(select id, ts2 - 1s as ek from l_expr) a join "
                "(select id, ts2 + 1s as ek from r_expr) b on a.ek = b.ek",
                "a.ek is not null and b.ek is not null",
            ),
        ]
        for condition, reference_from, reference_not_null in cases:
            actual = (
                "select /*+ join_algo(merge) */ a.id, b.id from l_expr a join r_expr b on "
                + condition
                + " where a.ts2 is not null and b.ts2 is not null order by 1, 2"
            )
            reference = "select a.id, b.id from " + reference_from + " where " + reference_not_null + " order by 1, 2"
            self.check_query_equal(actual, reference)
            self.check_plan(actual.removesuffix(" order by 1, 2"), "Merge", require_sort=True)
        print("L1 Merge timestamp expression keys ....... [ passed ]")

    def do_theta_hint_matches_hash_and_merge(self):
        self.run_case_file("test_join_condition_relaxation_theta_hint")
        print("Theta hint result snapshot matrix ........ [ passed ]")

    def do_hint_fallback_and_statement_time(self):
        for hint in ("join_algo(merge)", "merge_join", "join_algo(hash)", "hash_join"):
            for condition, algorithm in (
                ("lower(a.name) = lower(b.name)", "Hash"),
                ("a.k + 1 = b.k", "Hash"),
                ("a.x < b.x", "Theta(BlockNestedLoop)"),
            ):
                sql = f"select /*+ {hint} */ a.id, b.id from l_expr a join r_expr b on {condition}"
                reference = f"select /*+ join_algo(theta) */ a.id, b.id from l_expr a join r_expr b on {condition}"
                self.check_query_equal(sql + " order by 1, 2", reference + " order by 1, 2")
                self.check_plan(sql, algorithm)
        for hint in ("hash", "merge", "theta"):
            for join in ("left asof join", "right asof join"):
                reference = (
                    f"select a.id, b.id from l_range a {join} r_range b "
                    "on a.ts = b.ts order by 1, 2"
                )
                self.check_query_equal(
                    reference.replace("select ", f"select /*+ join_algo({hint}) */ ", 1), reference
                )
        for clock in ("now", "now()", "today", "today()"):
            condition = f"timediff({clock}, a.ts2) = timediff({clock}, b.ts2)"
            for hint in ("", "/*+ join_algo(hash) */", "/*+ join_algo(merge) */", "/*+ join_algo(theta) */"):
                for relation in (f"join r_expr b on {condition}", f", r_expr b where {condition}"):
                    sql = f"select {hint} a.id, b.id from l_expr a {relation} order by 1, 2"
                    reference = "select a.id, b.id from l_expr a join r_expr b on a.ts2 = b.ts2 order by 1, 2"
                    self.check_query_equal(sql, reference)
            tdSql.query(f"select count(*) from l_expr a join r_expr b on {clock} = {clock}")
            tdSql.checkData(0, 0, 25)
        tdSql.query("select now = now(), today = today(), today = timetruncate(now, 1d, 1)")
        tdSql.checkData(0, 0, True)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(0, 2, True)
        for precision in ("ms", "us", "ns"):
            db = f"join_clock_{precision}"
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} precision '{precision}'")
            tdSql.execute(f"create table {db}.t(ts timestamp, v int)")
            tdSql.execute(f"insert into {db}.t values (now, 1)")
            tdSql.query(
                f"select count(*) from {db}.t a join {db}.t b "
                "on now = now() and today = today() "
                "and today = timetruncate(now, 1d, 1)"
            )
            tdSql.checkData(0, 0, 1)
            tdSql.query(
                f"select count(*) from (select now() as n from {db}.t) a "
                f"join (select now() as n from {db}.t) b on a.n = b.n"
            )
            tdSql.checkData(0, 0, 1)
            tdSql.execute(f"drop database {db}")
        print("Join hint fallback and statement time .... [ passed ]")

    def do_unconditional_join(self):
        cross_joins = (
            "l_expr a, r_expr b",
            "l_expr a cross join r_expr b",
        )
        for tables in cross_joins:
            tdSql.query(f"select count(*) from {tables}")
            tdSql.checkData(0, 0, 25)
            tdSql.query(f"select count(*) from {tables} where a.id < 3")
            tdSql.checkData(0, 0, 10)
            self.check_plan(f"select a.id from {tables}", "Theta(BlockNestedLoop)")

        tdSql.query("select count(*) from l_expr a, r_expr b, l_expr c")
        tdSql.checkData(0, 0, 125)

        for hint in ("", "/*+ join_algo(theta) */"):
            for join in ("join", "inner join", "left join", "right join", "full join",
                         "left semi join", "left anti join", "right semi join", "right anti join"):
                tdSql.error(f"select {hint} a.id from l_expr a {join} r_expr b")

        tdSql.error("select a.id from l_expr a cross join r_expr b on a.id = b.id")
        tdSql.error("select a.id from l_expr a, r_expr b on a.id = b.id")
        print("Cross Join syntax and semantics .......... [ passed ]")

    def do_l2_integer_add_normalization(self):
        join_types = ["join", "left join", "right join", "full join"]
        for join_type in join_types:
            actual = (
                f"select a.id, b.id from l_expr a {join_type} r_expr b "
                "on a.x + b.k = b.x order by 1, 2"
            )
            reference = (
                f"select a.id, b.id from l_expr a {join_type} "
                "(select id, k, x, x - k as ek from r_expr) b "
                "on a.x = b.ek order by 1, 2"
            )
            self.check_query_equal(actual, reference)
        self.check_plan(
            "select a.id, b.id from l_expr a join r_expr b on a.x + b.k = b.x",
            "Hash",
        )
        self.check_plan(
            "select /*+ join_algo(hash) */ a.id, b.id from l_expr a join r_expr b "
            "on a.x + abs(b.k) = b.x",
            "Hash",
        )
        self.check_plan(
            "select /*+ join_algo(merge) */ a.id, b.id from l_expr a join r_expr b "
            "on a.ts2 + b.y = b.ts2",
            "Merge",
            require_sort=True,
        )
        print("L2 signed integer add normalization ...... [ passed ]")

    def do_l2_hash_join_type_edges(self):
        join_types = [
            "join",
            "left join",
            "right join",
            "full join",
            "left semi join",
            "left anti join",
            "right semi join",
            "right anti join",
        ]
        for join_type in join_types:
            if "semi" in join_type or "anti" in join_type:
                select_list = "a.id" if join_type.startswith("left") else "b.id"
                order_by = " order by 1"
            else:
                select_list = "a.id, b.id"
                order_by = " order by 1, 2"
            actual = (
                f"select /*+ join_algo(hash) */ {select_list} from l_expr a {join_type} r_expr b "
                f"on a.x + b.k = b.x and a.y < b.y + 4{order_by}"
            )
            reference = (
                f"select {select_list} from l_expr a {join_type} "
                f"(select id, k, x, y, x - k as ek from r_expr) b "
                f"on a.x = b.ek and a.y < b.y + 4{order_by}"
            )
            self.check_query_equal(actual, reference)
            self.check_plan(actual.removesuffix(order_by), "Hash")
        print("L2 Hash Join-type edge matrix ............. [ passed ]")

    def do_l2_reference_oracle(self):
        join_types = [
            "join",
            "left join",
            "right join",
            "full join",
            "left semi join",
            "left anti join",
            "right semi join",
            "right anti join",
        ]
        for join_type in join_types:
            if "semi" in join_type or "anti" in join_type:
                select_list = "a.id" if join_type.startswith("left") else "b.id"
                order_by = " order by 1"
            else:
                select_list = "a.id, b.id"
                order_by = " order by 1, 2"
            actual = (
                f"select {select_list} from l_expr a {join_type} r_expr b "
                f"on a.x + b.y = b.k{order_by}"
            )
            reference = (
                f"select {select_list} from l_expr a {join_type} "
                f"(select id, k, y, k - y as ek from r_expr) b "
                f"on a.x = b.ek{order_by}"
            )
            self.check_query_equal(actual, reference)

        cases = [
            (
                "select a.id, b.id from l_expr a join r_expr b "
                "on cast(a.x as bigint) + cast(b.k as bigint) = cast(b.x as bigint) order by 1, 2",
                "select a.id, b.id from l_expr a join "
                "(select id, cast(x as bigint) - cast(k as bigint) ek from r_expr) b "
                "on cast(a.x as bigint) = b.ek order by 1, 2",
            ),
            (
                "select a.id, b.id from l_expr a left join r_expr b "
                "on a.x + b.k = b.x and a.id < b.id where a.id between 2 and 5 order by 1, 2",
                "select a.id, b.id from l_expr a left join "
                "(select id, x - k ek from r_expr) b on a.x = b.ek and a.id < b.id "
                "where a.id between 2 and 5 order by 1, 2",
            ),
            (
                "select a.id, b.id from (select id, x from l_expr order by id desc limit 3) a "
                "left join r_expr b on a.x + b.k = b.x order by 1, 2",
                "select a.id, b.id from (select id, x from l_expr order by id desc limit 3) a "
                "left join (select id, x - k ek from r_expr) b on a.x = b.ek order by 1, 2",
            ),
            (
                "select a.id, b.id from (select distinct id, x from l_expr where id >= 2) a full join "
                "(select distinct id, k, x from r_expr where id <= 14) b "
                "on a.x + b.k = b.x order by 1, 2",
                "select a.id, b.id from (select distinct id, x from l_expr where id >= 2) a full join "
                "(select distinct id, x - k ek from r_expr where id <= 14) b "
                "on a.x = b.ek order by 1, 2",
            ),
            (
                "select /*+ join_algo(hash) */ a.id, b.id from l_expr a join r_expr b "
                "on a.ts2 + b.y = b.ts2 and a.x + b.k = b.x order by 1, 2",
                "select a.id, b.id from l_expr a join "
                "(select id, ts2 - y ets, x - k ex from r_expr) b "
                "on a.ts2 = b.ets and a.x = b.ex order by 1, 2",
            ),
        ]
        for actual, reference in cases:
            self.check_query_equal(actual, reference)

        tdSql.query(
            "select a.x, b.k, b.x, a.x + b.k from l_expr a join r_expr b "
            "on a.x + b.k = b.x order by a.id, b.id"
        )
        tdSql.checkRows(3)
        expected = [(3, 2, 5, 5), (4, 3, 7, 7), (4, 3, 7, 7)]
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

        time_join_types = ["join", "left join", "right join", "full join"]
        for join_type in time_join_types:
            actual = (
                f"select /*+ join_algo(merge) */ a.id, b.id from l_expr a {join_type} r_expr b "
                "on b.ts2 = b.y + a.ts2 order by 1, 2"
            )
            reference = (
                f"select a.id, b.id from l_expr a {join_type} "
                "(select id, ts2 - y ek from r_expr) b on a.ts2 = b.ek order by 1, 2"
            )
            self.check_query_equal(actual, reference)
        print("L2 normalized L1 reference oracle ......... [ passed ]")

    def do_l3_reference_oracle(self):
        left = [(-2, 1), (0, 2), (2, 3), (2, 4), (None, 5)]
        right = [(-1, 11), (1, 12), (2, 13), (4, 14), (None, 15)]

        def matches(op):
            rows = []
            for left_value, left_id in left:
                for right_value, right_id in right:
                    if left_value is None or right_value is None:
                        continue
                    if op(left_value, right_value):
                        rows.append((left_id, right_id))
            return rows

        predicates = {
            "<": lambda left_value, right_value: left_value < right_value,
            "<=": lambda left_value, right_value: left_value <= right_value,
            ">": lambda left_value, right_value: left_value > right_value,
            ">=": lambda left_value, right_value: left_value >= right_value,
        }
        for operator, predicate in predicates.items():
            expected = matches(predicate)
            tdSql.query(
                f"select a.id, b.id from l_range a join r_range b "
                f"on a.v {operator} b.v order by 1, 2"
            )
            tdSql.checkEqual(list(tdSql.queryResult), expected)

        expected = matches(predicates["<"])
        left_matched = {left_id for left_id, _ in expected}
        right_matched = {right_id for _, right_id in expected}
        join_expectations = {
            "left join": sorted(expected + [(left_id, None) for _, left_id in left if left_id not in left_matched]),
            "right join": sorted(expected + [(None, right_id) for _, right_id in right if right_id not in right_matched],
                                 key=lambda row: (-1 if row[0] is None else row[0], row[1])),
            "full join": sorted(
                expected
                + [(left_id, None) for _, left_id in left if left_id not in left_matched]
                + [(None, right_id) for _, right_id in right if right_id not in right_matched],
                key=lambda row: (-1 if row[0] is None else row[0], -1 if row[1] is None else row[1]),
            ),
        }
        for join_type, expected_rows in join_expectations.items():
            tdSql.query(
                f"select a.id, b.id from l_range a {join_type} r_range b "
                "on a.v < b.v order by 1, 2"
            )
            tdSql.checkEqual(list(tdSql.queryResult), expected_rows)

        existence_queries = [
            ("left semi join", "a.id", sorted(left_matched)),
            ("left anti join", "a.id", sorted(left_id for _, left_id in left if left_id not in left_matched)),
            ("right semi join", "b.id", sorted(right_matched)),
            ("right anti join", "b.id", sorted(right_id for _, right_id in right if right_id not in right_matched)),
        ]
        for join_type, select_list, expected_ids in existence_queries:
            tdSql.query(
                f"select {select_list} from l_range a {join_type} r_range b "
                "on a.v < b.v order by 1"
            )
            tdSql.checkEqual(list(tdSql.queryResult), [(value,) for value in expected_ids])
        print("L3 independent Cartesian oracle .......... [ passed ]")

    def do_l3_structured_oracle(self):
        left = [(-2, 1, 1), (0, 2, 1), (2, 3, 2), (2, 4, 2), (None, 5, 3)]
        right = [(-1, 11, 1), (1, 12, 1), (2, 13, 2), (4, 14, 2), (None, 15, 3)]

        cases = [
            (
                "a.v + 1 < b.v",
                lambda lv, lid, lg, rv, rid, rg: lv is not None and rv is not None and lv + 1 < rv,
            ),
            (
                "cast(a.v as bigint) <= cast(b.v as bigint)",
                lambda lv, lid, lg, rv, rid, rg: lv is not None and rv is not None and lv <= rv,
            ),
            (
                "a.v < b.v and a.grp = b.grp",
                lambda lv, lid, lg, rv, rid, rg: lv is not None and rv is not None and lv < rv and lg == rg,
            ),
            (
                "a.v <= b.v and a.id < b.id",
                lambda lv, lid, lg, rv, rid, rg: lv is not None and rv is not None and lv <= rv and lid < rid,
            ),
            (
                "a.v < b.v and b.id <= 12",
                lambda lv, lid, lg, rv, rid, rg: lv is not None and rv is not None and lv < rv and rid <= 12,
            ),
        ]
        for condition, predicate in cases:
            expected = [
                (left_id, right_id)
                for left_value, left_id, left_group in left
                for right_value, right_id, right_group in right
                if predicate(left_value, left_id, left_group, right_value, right_id, right_group)
            ]
            tdSql.query(
                f"select a.id, b.id from l_range a join r_range b on {condition} order by 1, 2"
            )
            tdSql.checkEqual(list(tdSql.queryResult), expected)

        tdSql.query(
            "select a.id, b.id from l_range a join r_range b on a.v < b.v "
            "where a.grp = 1 and b.grp <= 2 order by 1, 2"
        )
        expected = [
            (left_id, right_id)
            for left_value, left_id, left_group in left
            for right_value, right_id, right_group in right
            if left_value is not None and right_value is not None and left_value < right_value
            and left_group == 1 and right_group <= 2
        ]
        tdSql.checkEqual(list(tdSql.queryResult), expected)

        tdSql.query(
            "select a.id, b.id from l_range a full join r_range b on a.v < b.v "
            "where a.id is null or b.id is null order by 1, 2"
        )
        tdSql.checkEqual(list(tdSql.queryResult), [(None, 15), (5, None)])

        tdSql.query(
            "select a.id, b.id from (select * from l_range where id <= 3) a "
            "join r_range b on a.v < b.v order by 1, 2"
        )
        expected = [
            (left_id, right_id)
            for left_value, left_id, _ in left if left_id <= 3
            for right_value, right_id, _ in right
            if left_value is not None and right_value is not None and left_value < right_value
        ]
        tdSql.checkEqual(list(tdSql.queryResult), expected)

        self.check_plan(
            "select a.id, b.id from l_range a join r_range b on a.v < b.v",
            "Theta(BlockNestedLoop)",
        )
        print("L3 structured Cartesian oracle ........... [ passed ]")

    def do_l4_reference_oracle(self):
        left = [(-2, 1, 1), (0, 2, 1), (2, 3, 2), (2, 4, 2), (None, 5, 3)]
        right = [(-1, 11, 1, 1), (1, 12, 1, 2), (2, 13, 2, 3), (4, 14, 2, 5), (None, 15, 3, None)]
        cases = [
            ("a.v <> b.v", lambda lv, li, lg, rv, ri, rg, rh: lv is not None and rv is not None and lv != rv),
            ("a.v < b.v or a.grp = b.grp", lambda lv, li, lg, rv, ri, rg, rh: lg == rg or (lv is not None and rv is not None and lv < rv)),
            ("not (a.v < b.v)", lambda lv, li, lg, rv, ri, rg, rh: lv is not None and rv is not None and not lv < rv),
            ("a.v = 2", lambda lv, li, lg, rv, ri, rg, rh: lv == 2),
            ("b.v > 0", lambda lv, li, lg, rv, ri, rg, rh: rv is not None and rv > 0),
            ("1 = 1", lambda lv, li, lg, rv, ri, rg, rh: True),
            ("1 = 0", lambda lv, li, lg, rv, ri, rg, rh: False),
            ("a.v * b.v < b.hi", lambda lv, li, lg, rv, ri, rg, rh: lv is not None and rv is not None and rh is not None and lv * rv < rh),
            ("a.grp <> b.grp or a.v = b.v", lambda lv, li, lg, rv, ri, rg, rh: lg != rg or (lv is not None and rv is not None and lv == rv)),
            ("(a.v + b.v) % 2 = 0", lambda lv, li, lg, rv, ri, rg, rh: lv is not None and rv is not None and (lv + rv) % 2 == 0),
        ]
        for condition, predicate in cases:
            matches = [
                (left_id, right_id)
                for left_value, left_id, left_group in left
                for right_value, right_id, right_group, right_hi in right
                if predicate(left_value, left_id, left_group, right_value, right_id, right_group, right_hi)
            ]
            left_matched = {left_id for left_id, _ in matches}
            right_matched = {right_id for _, right_id in matches}
            expected_by_join = {
                "join": sorted(matches),
                "left join": sorted(matches + [(left_id, None) for _, left_id, _ in left if left_id not in left_matched]),
                "right join": sorted(matches + [(None, right_id) for _, right_id, _, _ in right if right_id not in right_matched],
                                     key=lambda row: (-1 if row[0] is None else row[0], row[1])),
                "full join": sorted(matches + [(left_id, None) for _, left_id, _ in left if left_id not in left_matched]
                                    + [(None, right_id) for _, right_id, _, _ in right if right_id not in right_matched],
                                    key=lambda row: (-1 if row[0] is None else row[0], -1 if row[1] is None else row[1])),
            }
            for join_type, expected in expected_by_join.items():
                tdSql.query(f"select a.id, b.id from l_range a {join_type} r_range b on {condition} order by 1, 2")
                tdSql.checkEqual(list(tdSql.queryResult), expected)
            existence = [
                ("left semi join", "a.id", sorted(left_matched)),
                ("left anti join", "a.id", sorted(left_id for _, left_id, _ in left if left_id not in left_matched)),
                ("right semi join", "b.id", sorted(right_matched)),
                ("right anti join", "b.id", sorted(right_id for _, right_id, _, _ in right if right_id not in right_matched)),
            ]
            for join_type, select_list, expected in existence:
                tdSql.query(f"select {select_list} from l_range a {join_type} r_range b on {condition} order by 1")
                tdSql.checkEqual(list(tdSql.queryResult), [(value,) for value in expected])
            self.check_plan(
                f"select a.id, b.id from l_range a join r_range b on {condition}",
                "Theta(BlockNestedLoop)",
            )
        print("L4 independent Cartesian oracle .......... [ passed ]")

    def do_l4_implicit_join(self):
        left = [(-2, 1, 1), (0, 2, 1), (2, 3, 2), (2, 4, 2), (None, 5, 3)]
        right = [(-1, 11, 1, 1), (1, 12, 1, 2), (2, 13, 2, 3), (4, 14, 2, 5), (None, 15, 3, None)]
        expected = sorted(
            (left_id, right_id)
            for left_value, left_id, _ in left
            for right_value, right_id, _, _ in right
            if ((left_value is not None and left_value == right_value and left_value > 0) or
                (right_value is not None and right_value < 2))
        )
        # These matches come only from the AND branch, not from b.v < 2.
        tdSql.checkEqual([row for row in expected if row[1] == 13], [(3, 13), (4, 13)])
        sql = (
            "select a.id, b.id from l_range a, r_range b "
            "where (a.v = b.v and a.v > 0) or b.v < 2 order by 1, 2"
        )
        tdSql.query(sql)
        tdSql.checkEqual(list(tdSql.queryResult), expected)
        self.check_plan(sql, "Theta(BlockNestedLoop)")

        tdSql.query(
            "select a.id, b.id from l_range a, r_range b "
            "where a.v < b.v and a.grp = b.grp order by 1, 2"
        )
        expected = sorted(
            (left_id, right_id)
            for left_value, left_id, left_group in left
            for right_value, right_id, right_group, _ in right
            if left_value is not None and right_value is not None and
            left_value < right_value and left_group == right_group
        )
        tdSql.checkEqual(list(tdSql.queryResult), expected)

        tdSql.query("select a.id, b.id from l_range a, r_range b")
        tdSql.checkRows(25)
        print("L4 implicit comma Join contract .......... [ passed ]")

    def do_result_snapshot_matrix(self):
        self.run_case_file("test_join_condition_relaxation_l1")
        print("L1 result snapshot matrix ................ [ passed ]")

    def do_l2_result_snapshot_matrix(self):
        self.run_case_file("test_join_condition_relaxation_l2")
        print("L2 result snapshot matrix ................ [ passed ]")

    def do_l3_result_snapshot_matrix(self):
        self.run_case_file("test_join_condition_relaxation_l3")
        print("L3 result snapshot matrix ................ [ passed ]")

    def do_l4_result_snapshot_matrix(self):
        self.run_case_file("test_join_condition_relaxation_l4")
        print("L4 result snapshot matrix ................ [ passed ]")

    def prepare_three_dnode_data(self):
        tdSql.query("select id from information_schema.ins_dnodes where status = 'ready' order by id")
        ready_dnodes = [row[0] for row in tdSql.queryResult]
        if len(ready_dnodes) < 3:
            pytest.skip("requires three ready dnodes; run this test with -N 3")

        target_dnodes = sorted(ready_dnodes)[:3]
        dnode_list = ",".join(str(dnode_id) for dnode_id in target_dnodes)
        tdSql.execute("drop database if exists join_condition_relaxation_3dn")
        tdSql.execute(
            f"create database join_condition_relaxation_3dn vgroups 6 replica 1 dnodes '{dnode_list}'"
        )
        # DNODES limits eligible nodes; load balancing need not use every node.
        tdSql.query(
            "select vgroup_id, v1_dnode from information_schema.ins_vgroups "
            "where db_name = 'join_condition_relaxation_3dn' order by vgroup_id"
        )
        placements = list(tdSql.queryResult)
        expected_placements = dict(placements)
        for target_dnode in target_dnodes:
            if target_dnode in expected_placements.values():
                continue
            for vgroup_id, current_dnode in expected_placements.items():
                if list(expected_placements.values()).count(current_dnode) > 1:
                    tdSql.execute(f"redistribute vgroup {vgroup_id} dnode {target_dnode}")
                    expected_placements[vgroup_id] = target_dnode
                    break
        deadline = time.monotonic() + 60
        while True:
            tdSql.query(
                "select vgroup_id, v1_dnode from information_schema.ins_vgroups "
                "where db_name = 'join_condition_relaxation_3dn'"
            )
            if dict(tdSql.queryResult) == expected_placements:
                break
            if time.monotonic() >= deadline:
                pytest.fail(f"vgroup placement did not converge: {tdSql.queryResult}")
            time.sleep(0.2)
        tdSql.execute("use join_condition_relaxation_3dn")
        tdSql.execute("create stable l_dist(ts timestamp, id int, v int) tags(t int)")
        tdSql.execute("create stable r_dist(ts timestamp, id int, v int, k int) tags(t int)")
        for index in range(30):
            tdSql.execute(f"create table l_dist_{index} using l_dist tags({index})")
            tdSql.execute(f"create table r_dist_{index} using r_dist tags({index})")

        tdSql.query(
            "select t.table_name, v.v1_dnode "
            "from information_schema.ins_tables t join information_schema.ins_vgroups v "
            "on t.vgroup_id = v.vgroup_id and t.db_name = v.db_name "
            "where t.db_name = 'join_condition_relaxation_3dn' and t.type = 'CHILD_TABLE'"
        )
        tables_by_dnode = {"l": {}, "r": {}}
        for table_name, dnode_id in tdSql.queryResult:
            side = "l" if table_name.startswith("l_dist_") else "r"
            tables_by_dnode[side].setdefault(dnode_id, table_name)

        for side in ("l", "r"):
            if not set(tables_by_dnode[side]) >= set(target_dnodes):
                pytest.fail(f"missing test tables on selected dnodes {target_dnodes}: {tables_by_dnode}")

        left_values = [(1, 1), (2, 3), (3, 5)]
        right_values = [(11, 2), (12, 4), (13, 6)]
        for offset, dnode_id in enumerate(target_dnodes):
            left_id, left_value = left_values[offset]
            right_id, right_value = right_values[offset]
            tdSql.execute(
                f"insert into {tables_by_dnode['l'][dnode_id]} values"
                f"('2025-03-01 00:00:0{offset}', {left_id}, {left_value})"
            )
            tdSql.execute(
                f"insert into {tables_by_dnode['r'][dnode_id]} values"
                f"('2025-03-01 00:01:0{offset}', {right_id}, {right_value}, 1)"
            )

        tdSql.execute(
            f"insert into {tables_by_dnode['l'][target_dnodes[0]]} values"
            "('2025-03-01 00:00:10', 4, null)"
        )
        tdSql.execute(
            f"insert into {tables_by_dnode['r'][target_dnodes[1]]} values"
            "('2025-03-01 00:01:10', 14, null, 1)"
        )

        tdSql.query(
            "select distinct v.v1_dnode from information_schema.ins_tables t "
            "join information_schema.ins_vgroups v "
            "on t.vgroup_id = v.vgroup_id and t.db_name = v.db_name "
            "where t.db_name = 'join_condition_relaxation_3dn' "
            "and t.table_name in ('"
            + "','".join(
                [tables_by_dnode[side][dnode_id] for side in ("l", "r") for dnode_id in target_dnodes]
            )
            + "') order by 1"
        )
        tdSql.checkEqual(list(tdSql.queryResult), [(dnode_id,) for dnode_id in target_dnodes])

    def do_l2_three_dnode_join(self):
        sql = "select a.id, b.id from l_dist a join r_dist b on a.v + b.k = b.v"
        self.check_plan(sql, "Hash")
        tdSql.query(sql + " order by 1, 2")
        tdSql.checkEqual(list(tdSql.queryResult), [(1, 11), (2, 12), (3, 13)])
        print("L2 three-dnode distributed Join .......... [ passed ]")

    def do_l3_three_dnode_join(self):
        sql = "select a.id, b.id from l_dist a join r_dist b on a.v < b.v"
        self.check_plan(sql, "Theta(BlockNestedLoop)")
        tdSql.query(sql + " order by 1, 2")
        tdSql.checkEqual(
            list(tdSql.queryResult),
            [(1, 11), (1, 12), (1, 13), (2, 12), (2, 13), (3, 13)],
        )
        print("L3 three-dnode distributed Join .......... [ passed ]")

    def do_l4_three_dnode_join(self):
        cases = [
            (
                "a.v <> b.v or a.id = 1",
                [(1, 11), (1, 12), (1, 13), (1, 14), (2, 11), (2, 12), (2, 13),
                 (3, 11), (3, 12), (3, 13)],
            ),
            (
                "1 = 1",
                [(left_id, right_id) for left_id in (1, 2, 3, 4) for right_id in (11, 12, 13, 14)],
            ),
        ]
        for condition, expected in cases:
            sql = f"select a.id, b.id from l_dist a join r_dist b on {condition}"
            self.check_plan(sql, "Theta(BlockNestedLoop)")
            tdSql.query(sql + " order by 1, 2")
            tdSql.checkEqual(list(tdSql.queryResult), expected)
        print("L4 three-dnode distributed Join .......... [ passed ]")

    def do_three_dnode_result_snapshot_matrix(self):
        self.run_case_file("test_join_condition_relaxation_l2_3dn")
        print("L2 three-dnode result snapshot matrix .... [ passed ]")
        self.run_case_file("test_join_condition_relaxation_l3_3dn")
        print("L3 three-dnode result snapshot matrix .... [ passed ]")

    # --- main ---

    def test_join_condition_relaxation_l4(self):
        """Validate L4 general Theta Join semantics

        1. Verify arbitrary boolean ON predicates with an independent Cartesian oracle
        2. Verify all Join types, NULL three-valued logic, clauses and nested inputs
        3. Compare at least 100 L4 SQL statements with reviewed expected results

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 Added L4 general Theta Join coverage

        """
        self.prepare()
        self.do_l4_reference_oracle()
        self.do_l4_implicit_join()
        self.do_l4_result_snapshot_matrix()

    def test_join_condition_relaxation_l3(self):
        """Validate L3 range Join semantics and snapshot coverage

        1. Verify range comparisons and all regular Join types with an independent Cartesian oracle
        2. Verify structured expressions, residual ON predicates, WHERE filters and subqueries
        3. Verify L3 behavior remains correct after enabling the L4 fallback
        4. Compare the complete L3 SQL matrix with the reviewed answer file

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 Added independent L3 oracle and Theta Join coverage
            - 2026-08-12 Enabled reviewed L3 snapshot matrix

        """
        self.prepare()
        self.do_l3_reference_oracle()
        self.do_l3_structured_oracle()
        self.do_l4_reference_oracle()
        self.do_l3_result_snapshot_matrix()

    def test_join_condition_relaxation_three_dnodes(self):
        """Validate L2, L3 and L4 Join across three dnodes

        1. Place both Join inputs across dnodes 1, 2 and 3
        2. Verify L2 normalized equality Join across all distributed inputs
        3. Verify L3 range Join across all distributed inputs
        4. Verify L4 general predicates across all distributed inputs

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-08-12 Added three-dnode L2, L3 and L4 distributed Join coverage

        """
        self.prepare_three_dnode_data()
        self.do_l2_three_dnode_join()
        self.do_l3_three_dnode_join()
        self.do_l4_three_dnode_join()
        self.do_three_dnode_result_snapshot_matrix()

    def test_join_condition_relaxation(self):
        """Validate L1 and L2 Join condition relaxation for Hash and Merge Join

        1. Cover arithmetic, cast, scalar function, timestamp and string expression keys
        2. Cover composite keys, residual ON predicates, NULLs and duplicate keys
        3. Cover inner, outer, semi and anti Hash Join semantics
        4. Cover forced Merge Join with timestamp expressions and inserted Sort nodes
        5. Cover the L2 signed-integer and timestamp-offset normalization whitelist
        6. Cover nested expressions, nested joins, subqueries, clauses and predicate placement
        7. Support statement time constants and TIMEDIFF in Join conditions
        8. Verify lower-level Hash and Merge behavior independently of Theta fallback

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-10 Added hint fallback, unconditional Join and statement time coverage
            - 2026-08-11 Added L1 Join condition relaxation acceptance coverage
            - 2026-08-11 Added initial L2 signed-integer addition normalization coverage
            - 2026-08-11 Expanded L2 Hash, Merge, nested and predicate coverage
            - 2026-08-12 Expanded L2 mirrors, empty inputs, subqueries and Join-type edges
            - 2026-08-12 Expanded L1 snapshot and independent reference coverage past 100 queries
            - 2026-08-12 Added the 102-query L3 range Join acceptance matrix

        """
        self.prepare()
        self.do_hash_arithmetic_keys()
        self.do_hash_timestamp_and_string_keys()
        self.do_hash_composite_and_residual_keys()
        self.do_hash_join_types()
        self.do_l1_hash_join_type_edges()
        self.do_l1_reference_oracle()
        self.do_merge_timestamp_keys()
        self.do_hint_fallback_and_statement_time()
        self.do_unconditional_join()
        self.do_l2_integer_add_normalization()
        self.do_l2_hash_join_type_edges()
        self.do_l2_reference_oracle()
        self.do_result_snapshot_matrix()
        self.do_l2_result_snapshot_matrix()

    def test_theta_hint_matches_hash_and_merge(self):
        """Validate forced Theta Join against existing Hash and Merge results.

        Reuses 180 L1/L2 Hash and Merge Join SQL statements without changing
        the original cases, and compares all results with a reviewed snapshot
        file.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-14 Added JOIN_ALGO(THETA) result parity coverage
        """
        self.prepare()
        self.do_theta_hint_matches_hash_and_merge()

    def test_theta_outer_join_final_filter_with_columns(self):
        """Regression coverage for THETA outer joins with post-join filters.

        Keep the original snapshot SQL unchanged; this adds projected join-key
        and payload columns so a false unmatched-row emission is observable.

        Since: v3.4.3.0
        """
        self.prepare()

        self.check_query_equal(
            "select /*+ join_algo(theta) */ a.id, b.id, a.k, b.k, a.x, b.x "
            "from l_expr a full join r_expr b on a.k + 1 = b.k "
            "where a.id is null or b.id is null order by 1, 2",
            "select a.id, b.id, a.k, b.k, a.x, b.x "
            "from l_expr a full join r_expr b on a.k + 1 = b.k "
            "where a.id is null or b.id is null order by 1, 2",
        )
        self.check_query_equal(
            "select /*+ join_algo(theta) */ a.id, b.id, a.k, b.k, a.x, b.x "
            "from l_expr a left join r_expr b on a.k + 1 = b.k "
            "where a.k is null or b.k is null order by 1, 2",
            "select a.id, b.id, a.k, b.k, a.x, b.x "
            "from l_expr a left join r_expr b on a.k + 1 = b.k "
            "where a.k is null or b.k is null order by 1, 2",
        )
        self.check_query_equal(
            "select /*+ join_algo(theta) */ a.id, b.id, a.k, b.k, a.x, b.x "
            "from l_expr a full join r_expr b on a.x + b.k = b.x "
            "where a.x is null or b.x is null order by 1, 2",
            "select a.id, b.id, a.k, b.k, a.x, b.x "
            "from l_expr a full join r_expr b on a.x + b.k = b.x "
            "where a.x is null or b.x is null order by 1, 2",
        )
        self.check_query_equal(
            "select /*+ join_algo(theta) */ a.k + 1, b.k, a.id, b.id, a.x, b.x "
            "from l_expr a join r_expr b on a.k + 1 = b.k "
            "where a.id >= 2 and b.id <= 13 order by 1, 2, 3, 4",
            "select a.k + 1, b.k, a.id, b.id, a.x, b.x "
            "from l_expr a join r_expr b on a.k + 1 = b.k "
            "where a.id >= 2 and b.id <= 13 order by 1, 2, 3, 4",
        )

        # Explicitly retain SEMI/ANTI behavior after moving matched marking.
        self.check_query_equal(
            "select /*+ join_algo(theta) */ a.id, a.k, a.x from l_expr a "
            "left semi join r_expr b on a.k + 1 = b.k order by 1",
            "select a.id, a.k, a.x from l_expr a "
            "left semi join r_expr b on a.k + 1 = b.k order by 1",
        )
        self.check_query_equal(
            "select /*+ join_algo(theta) */ a.id, a.k, a.x from l_expr a "
            "left anti join r_expr b on a.k + 1 = b.k order by 1",
            "select a.id, a.k, a.x from l_expr a "
            "left anti join r_expr b on a.k + 1 = b.k order by 1",
        )
