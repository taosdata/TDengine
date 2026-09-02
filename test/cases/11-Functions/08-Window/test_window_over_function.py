import os

from new_test_framework.utils import tdCom, tdLog, tdSql


class TestWindowOverFunction:
    updatecfgDict = {
        "debugFlag": 131,
        "asyncLog": 1,
        "qDebugFlag": 131,
        "cDebugFlag": 131,
        "rpcDebugFlag": 131,
    }
    clientCfgDict = {
        "debugFlag": 131,
        "asyncLog": 1,
        "qDebugFlag": 131,
        "cDebugFlag": 131,
        "rpcDebugFlag": 131,
    }
    updatecfgDict["clientCfg"] = clientCfgDict

    caseName = "test_window_over_function"
    dbname = "win_over_func"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    fileIdx = 0

    matrix_window_functions = [
        {"code": "cnt", "expr": "count(v)", "requires_order": False},
        {"code": "cntn", "expr": "count(nullable_v)", "requires_order": False},
        {"code": "sum", "expr": "sum(v)", "requires_order": False},
        {"code": "min", "expr": "min(v)", "requires_order": False},
        {"code": "max", "expr": "max(v)", "requires_order": False},
        {"code": "avg", "expr": "avg(v)", "requires_order": False},
        {"code": "pct", "expr": "percentile(v, 50)", "requires_order": False},
        {"code": "first", "expr": "first(v)", "requires_order": False},
        {"code": "last", "expr": "last(v)", "requires_order": False},
        {"code": "lastrow", "expr": "last_row(v)", "requires_order": False},
        {"code": "rown", "expr": "row_number()", "requires_order": True},
        {"code": "rank", "expr": "rank()", "requires_order": True},
        {"code": "drank", "expr": "dense_rank()", "requires_order": True},
        {"code": "prank", "expr": "percent_rank()", "requires_order": True},
        {"code": "cdist", "expr": "cume_dist()", "requires_order": True},
        {"code": "lag", "expr": "lag(v)", "requires_order": True},
        {"code": "lead", "expr": "lead(v)", "requires_order": True},
        {"code": "fval", "expr": "first_value(v)", "requires_order": True},
        {"code": "lval", "expr": "last_value(v)", "requires_order": True},
        {"code": "nth", "expr": "nth_value(v, 2)", "requires_order": True},
    ]

    matrix_partitions = [
        {"code": "p0", "sql": ""},
        {"code": "p1", "sql": "partition by dev"},
        {"code": "p2", "sql": "partition by site, dev"},
    ]

    matrix_orders = [
        {"code": "o0", "sql": "", "range_type": "none"},
        {"code": "o1", "sql": "order by ts", "range_type": "time"},
        {"code": "o2", "sql": "order by ts desc", "range_type": "time"},
        {"code": "o3", "sql": "order by v", "range_type": "numeric"},
        {"code": "o4", "sql": "order by v, ts", "range_type": "multi"},
    ]

    matrix_frames = [
        {"code": "f0", "kind": "default", "sql": ""},
        {"code": "f1", "kind": "rows", "sql": "rows between current row and current row"},
        {"code": "f2", "kind": "rows", "sql": "rows between 1 preceding and current row"},
        {"code": "f3", "kind": "rows", "sql": "rows between current row and 1 following"},
        {"code": "f4", "kind": "rows", "sql": "rows between 1 preceding and 1 following"},
        {"code": "f5", "kind": "range_no_offset", "sql": "range between current row and current row"},
        {"code": "f6", "kind": "range_no_offset", "sql": "range between unbounded preceding and current row"},
        {"code": "f7", "kind": "range_offset", "sql": None},
    ]

    aggregate_funcs = [
        ("F027_count_v", "count(v)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F027_count_nullable", "count(nullable_v)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F027_sum", "sum(v)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F027_min", "min(v)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F027_max", "max(v)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F027_avg", "avg(v)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F029_avg_expr", "avg(v + 1)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F029_sum_cast", "sum(cast(v as double))", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between unbounded preceding and current row"),
        ("F027_percentile", "percentile(v, 50)", "rows_t", "where dev = 'd1'", "partition by dev"),
        ("F027_percentile_rows", "percentile(v, 50)", "rows_t", "where dev = 'd1'", "partition by dev order by ts rows between 1 preceding and current row"),
    ]

    order_sensitive_funcs = [
        "row_number()",
        "rank()",
        "dense_rank()",
        "percent_rank()",
        "cume_dist()",
        "lag(v)",
        "lead(v)",
        "first_value(v)",
        "last_value(v)",
        "nth_value(v, 1)",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_window_over_function(self):
        """OVER/WINDOW function matrix test.

        1. Prepare one deterministic database for normal tables, stable tables, range data,
           ranking data, named windows, and BI-style nested queries.
        2. Generate all positive and negative SQL from templates and syntax matrices.
        3. Compare the full taos CLI output, including query results and DB error lines,
           with the expected CSV file.

        Since: v3.4.1.0

        Labels: common,ci

        Feishu: None

        Jira: None

        History:
            - 2026-06-15 Generated from over-window-function TS
        """

        self.prepareData()
        self.execMatrixCase()
        self.rmoveSqlTmpFiles()

    def prepareData(self):
        tdLog.info("prepare data for OVER/WINDOW function test")
        tdSql.execute_ignore_error("create snode on dnode 1")
        tdSql.execute(f"drop topic if exists topic_window_illegal")
        tdSql.execute(f"drop database if exists {self.dbname}")
        tdSql.execute(f"create database {self.dbname}")
        tdSql.execute(f"use {self.dbname}")

        sqls = [
            """
            create table rows_t(
                ts timestamp,
                dev varchar(16),
                site varchar(16),
                v int,
                nullable_v int,
                vb bigint,
                vd double,
                flag bool,
                name varchar(20),
                note nchar(20),
                ts2 timestamp
            )
            """,
            """
            insert into rows_t values
                ('2024-01-01 00:00:00.000', 'd1', 's1', 10, 10, 100, 1.5, true,  'alpha',   'n1', '2024-02-01 00:00:00.000')
                ('2024-01-01 00:00:01.000', 'd1', 's1', 20, null, 200, 2.5, false, 'bravo',   'n2', '2024-02-01 00:00:01.000')
                ('2024-01-01 00:00:02.000', 'd1', 's1', 30, 30, 300, 3.5, true,  'charlie', 'n3', '2024-02-01 00:00:02.000')
                ('2024-01-01 00:00:03.000', 'd1', 's1', 40, null, 400, 4.5, false, 'delta',   'n4', '2024-02-01 00:00:03.000')
                ('2024-01-01 00:00:04.000', 'd1', 's1', 50, 50, 500, 5.5, true,  'echo',    'n5', '2024-02-01 00:00:04.000')
                ('2024-01-01 00:01:00.000', 'd2', 's1', 1,  1, 10, 0.1, true,  'one', 'm1', '2024-03-01 00:00:00.000')
                ('2024-01-01 00:01:01.000', 'd2', 's1', 2,  2, 20, 0.2, false, 'two', 'm2', '2024-03-01 00:00:01.000')
                ('2024-01-01 00:02:00.000', 'd3', 's2', 7,  7, 70, 0.7, true,  'solo', 'p1', '2024-04-01 00:00:00.000')
            """,
            "create stable stb(ts timestamp, v int, score double) tags(device varchar(16), site varchar(16))",
            "create table stb_d1 using stb tags('dev-a', 'north')",
            "create table stb_d2 using stb tags('dev-b', 'north')",
            "create table stb_d3 using stb tags('dev-c', 'south')",
            """
            insert into stb_d1 values
                ('2024-01-01 00:00:00.000', 10, 1.0)
                ('2024-01-01 00:00:01.000', 20, 2.0)
                ('2024-01-01 00:00:02.000', 30, 3.0)
            """,
            """
            insert into stb_d2 values
                ('2024-01-01 00:00:00.000', 1, 1.0)
                ('2024-01-01 00:00:01.000', 2, 2.0)
            """,
            "insert into stb_d3 values('2024-01-01 00:00:00.000', 7, 7.0)",
            "create table range_t(ts timestamp, grp varchar(16), v int)",
            """
            insert into range_t values
                ('2024-01-01 00:00:00.000', 'g1', 10)
                ('2024-01-01 00:00:05.000', 'g1', 20)
                ('2024-01-01 00:00:15.000', 'g1', 30)
                ('2024-01-01 00:00:30.000', 'g1', 40)
                ('2024-01-01 00:01:00.000', 'g2', 1)
                ('2024-01-01 00:01:08.000', 'g2', 2)
            """,
            "create table numeric_t(ts timestamp, grp varchar(16), k int, v int)",
            """
            insert into numeric_t values
                ('2024-01-01 00:00:00.000', 'g1', -5, 5)
                ('2024-01-01 00:00:01.000', 'g1', 0, 10)
                ('2024-01-01 00:00:02.000', 'g1', 10, 20)
                ('2024-01-01 00:00:03.000', 'g1', 10, 30)
                ('2024-01-01 00:00:04.000', 'g1', 25, 40)
            """,
            "create table numeric_decimal_t(ts timestamp, grp varchar(16), k double, v int)",
            """
            insert into numeric_decimal_t values
                ('2024-01-01 00:00:00.000', 'g1', -5.5, 5)
                ('2024-01-01 00:00:01.000', 'g1', 0.0, 10)
                ('2024-01-01 00:00:02.000', 'g1', 10.5, 20)
                ('2024-01-01 00:00:03.000', 'g1', 10.5, 30)
                ('2024-01-01 00:00:04.000', 'g1', 25.25, 40)
            """,
            "create table null_order_t(ts timestamp, k int, v int)",
            """
            insert into null_order_t values
                ('2024-01-01 00:00:00.000', null, 10)
                ('2024-01-01 00:00:01.000', 1, 20)
                ('2024-01-01 00:00:02.000', null, 30)
                ('2024-01-01 00:00:03.000', 2, 40)
            """,
            "create table multi_order_t(ts timestamp, k1 int, k2 varchar(8), v int)",
            """
            insert into multi_order_t values
                ('2024-01-01 00:00:00.000', 1, 'a', 10)
                ('2024-01-01 00:00:01.000', 1, 'a', 20)
                ('2024-01-01 00:00:02.000', 1, 'b', 30)
                ('2024-01-01 00:00:03.000', 2, 'a', 40)
            """,
            "create table rank_t(ts timestamp, grp varchar(8), score double, v int)",
            """
            insert into rank_t values
                ('2024-01-01 00:00:00.000', 'g1', 10.0, 10)
                ('2024-01-01 00:00:01.000', 'g1', 20.0, 20)
                ('2024-01-01 00:00:02.000', 'g1', 20.0, 30)
                ('2024-01-01 00:00:03.000', 'g1', 30.0, 40)
                ('2024-01-01 00:01:00.000', 'g2', 10.0, 1)
                ('2024-01-01 00:01:01.000', 'g2', 10.0, 2)
                ('2024-01-01 00:01:02.000', 'g2', 20.0, 3)
                ('2024-01-01 00:02:00.000', 'g3', 99.0, 99)
            """,
            "create table rank_double_t(ts timestamp, grp varchar(8), score double)",
            """
            insert into rank_double_t values
                ('2024-01-01 00:00:00.000', 'g1', -0.0)
                ('2024-01-01 00:00:01.000', 'g1', 0.0)
                ('2024-01-01 00:00:02.000', 'g1', 1.0)
            """,
            "create table geom_t(ts timestamp, grp varchar(8), g geometry(128))",
            """
            insert into geom_t values
                ('2024-01-01 00:00:00.000', 'g1', 'POINT(1 1)')
                ('2024-01-01 00:00:01.000', 'g1', 'POINT(2 2)')
            """,
            "create table named_t(ts timestamp, grp varchar(8), v int)",
            """
            insert into named_t values
                ('2024-01-01 00:00:00.000', 'n1', 10)
                ('2024-01-01 00:00:01.000', 'n1', 20)
                ('2024-01-01 00:00:02.000', 'n1', 30)
                ('2024-01-01 00:00:03.000', 'n1', 40)
            """,
            "create table bi_t(ts timestamp, value_col int, group_col int, flag_col int)",
            """
            insert into bi_t values
                ('2025-01-01 00:00:00.000', 1, 10, 100)
                ('2025-01-01 00:00:01.000', 2, 10, 100)
                ('2025-01-01 00:00:02.000', 3, 20, 200)
                ('2025-01-01 00:00:03.000', 4, 20, 200)
            """,
        ]
        tdSql.executes(sqls)

    def checkResultWithResultFile(self, sqlFile, resFile):
        tdLog.info(f"check result with sql: {sqlFile}")
        tdCom.compare_testcase_result(
            sqlFile, resFile, self.caseName + "." + str(self.fileIdx)
        )
        tdLog.info("check result with result file succeed")

    def openSqlTmpFile(self):
        tmp_file = os.path.join(
            self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql"
        )
        os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
        self.generated_queries_file = open(tmp_file, "w", encoding="utf-8")
        self.generated_queries_file.write(f"use {self.dbname};\n\n")

    def rmoveSqlTmpFiles(self):
        for idx in range(0, self.fileIdx + 1):
            tmp_file = os.path.join(
                self.currentDir, f"{self.caseName}_generated_queries{idx}.sql"
            )
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

    def _write_sql(self, sql):
        self.generated_queries_file.write(" ".join(sql.strip().split()).rstrip(";") + ";\n")
        self.generated_queries_file.flush()

    def _cid_select(self, cid, columns, table, where="", order=""):
        where_sql = f" {where}" if where else ""
        order_sql = f" {order}" if order else ""
        return f"select '{cid}' as cid, {columns} from {table}{where_sql}{order_sql}"

    def _matrix_frame_sql(self, frame, order):
        if frame["kind"] != "range_offset":
            return frame["sql"]
        if order["range_type"] == "time":
            return "range between 1s preceding and current row"
        return "range between 10 preceding and current row"

    def _matrix_window_sql(self, partition, order, frame):
        clauses = [partition["sql"], order["sql"], self._matrix_frame_sql(frame, order)]
        return " ".join(clause for clause in clauses if clause)

    def _matrix_expects_error(self, func, order, frame):
        if func["requires_order"] and not order["sql"]:
            return True
        if frame["kind"].startswith("range") and not order["sql"]:
            return True
        if frame["kind"] == "range_offset" and order["range_type"] not in ("time", "numeric"):
            return True
        return False

    def _matrix_select(self, cid, func, window_sql):
        return f"""
            select '{cid}' as cid,
                   count(*) as total_rows,
                   count(w) as non_null_rows,
                   min(cast(w as double)) as min_w,
                   max(cast(w as double)) as max_w,
                   sum(cast(w as double)) as sum_w
            from (
                select {func["expr"]} over({window_sql}) as w
                from rows_t
                where dev in ('d1', 'd2', 'd3')
            ) q
            """

    def _generate_window_matrix_sqls(self):
        case_idx = 0
        for func in self.matrix_window_functions:
            for partition in self.matrix_partitions:
                for order in self.matrix_orders:
                    for frame in self.matrix_frames:
                        window_sql = self._matrix_window_sql(partition, order, frame)
                        if self._matrix_expects_error(func, order, frame):
                            case_idx += 1
                            cid = (
                                f"M{case_idx:04d}_{func['code']}_{partition['code']}_"
                                f"{order['code']}_{frame['code']}_err"
                            )
                            self._write_sql(
                                f"select '{cid}' as cid, {func['expr']} over({window_sql}) as w "
                                "from rows_t where dev in ('d1', 'd2', 'd3')"
                            )
                        else:
                            case_idx += 1
                            cid = (
                                f"M{case_idx:04d}_{func['code']}_{partition['code']}_"
                                f"{order['code']}_{frame['code']}"
                            )
                            self._write_sql(self._matrix_select(cid, func, window_sql))

    def _generate_positive_sqls(self):
        basic_cases = [
            (
                "F001_multi_over",
                "dev, v, sum(v) over(partition by dev order by ts rows between unbounded preceding and current row) as s, "
                "avg(v) over(partition by dev order by ts rows between 1 preceding and current row) as ma",
                "rows_t",
                "where dev in ('d1', 'd2')",
                "order by dev, ts",
            ),
            (
                "F003_no_partition",
                "dev, v, sum(v) over(order by ts, dev rows between unbounded preceding and current row) as s",
                "rows_t",
                "",
                "order by ts, dev",
            ),
            (
                "F009_whole_partition",
                "dev, v, sum(v) over(partition by dev) as s",
                "rows_t",
                "",
                "order by dev, ts",
            ),
            (
                "F005_multi_partition",
                "site, dev, v, count(*) over(partition by site, dev order by ts) as c",
                "rows_t",
                "",
                "order by site, dev, ts",
            ),
            (
                "F006_order_direction",
                "v, sum(v) over(partition by dev order by ts) as dft, "
                "sum(v) over(partition by dev order by ts asc) as asc_s, "
                "sum(v) over(partition by dev order by ts desc) as desc_s",
                "rows_t",
                "where dev = 'd1'",
                "order by ts",
            ),
            (
                "F004_tbname_partition",
                "tbname, v, sum(v) over(partition by tbname order by ts rows between unbounded preceding and current row) as s",
                "(select tbname, ts, v from stb) q",
                "",
                "order by tbname, ts",
            ),
            (
                "F004_tag_partition",
                "device, v, sum(v) over(partition by device order by ts rows between unbounded preceding and current row) as s",
                "stb",
                "",
                "order by device, ts",
            ),
        ]
        for case in basic_cases:
            self._write_sql(self._cid_select(*case))

        row_frame_cases = [
            (
                "F012_rows_current",
                "v, count(v) over(partition by dev order by ts rows between current row and current row) as c",
            ),
            (
                "F013_rows_preceding",
                "v, sum(v) over(partition by dev order by ts rows between 1 preceding and current row) as s",
            ),
            (
                "F014_rows_following",
                "v, sum(v) over(partition by dev order by ts rows between current row and 2 following) as s",
            ),
            (
                "F015_rows_neighbor",
                "v, avg(v) over(partition by dev order by ts rows between 1 preceding and 1 following) as a",
            ),
            (
                "F016_rows_unbounded_prefix",
                "v, sum(v) over(partition by dev order by ts rows between unbounded preceding and current row) as s",
            ),
            (
                "F016_rows_unbounded_suffix",
                "v, sum(v) over(partition by dev order by ts rows between current row and unbounded following) as s",
            ),
            (
                "F017_rows_shorthand",
                "v, sum(v) over(partition by dev order by ts rows 10 preceding) as s",
            ),
            (
                "F018_rows_empty",
                "v, count(v) over(partition by dev order by ts rows between 1 preceding and 1 preceding) as c, "
                "first(v) over(partition by dev order by ts rows between 1 preceding and 1 preceding) as f",
            ),
            (
                "F006_rows_desc",
                "v, sum(v) over(partition by dev order by ts desc rows between unbounded preceding and current row) as s",
            ),
        ]
        for cid, columns in row_frame_cases:
            order = "order by ts desc" if cid == "F006_rows_desc" else "order by ts"
            self._write_sql(self._cid_select(cid, columns, "rows_t", "where dev = 'd1'", order))

        range_cases = [
            (
                "F019_range_time_preceding",
                "grp, v, sum(v) over(partition by grp order by ts range between 10s preceding and current row) as s",
                "range_t",
                "",
                "order by grp, ts",
            ),
            (
                "F020_range_time_both",
                "grp, v, sum(v) over(partition by grp order by ts range between 1m preceding and 1m following) as s",
                "range_t",
                "",
                "order by grp, ts",
            ),
            (
                "F021_range_numeric",
                "k, v, count(v) over(partition by grp order by k range between 10 preceding and current row) as c",
                "numeric_t",
                "",
                "order by k, ts",
            ),
            (
                "F021_range_numeric_decimal",
                "k, v, count(v) over(partition by grp order by k range between 10 preceding and current row) as c",
                "numeric_decimal_t",
                "",
                "order by k, ts",
            ),
            (
                "F022_range_peer",
                "k, v, sum(v) over(partition by grp order by k range between current row and current row) as s, "
                "count(v) over(partition by grp order by k rows between current row and current row) as rc",
                "numeric_t",
                "",
                "order by k, ts",
            ),
            (
                "F006_range_desc",
                "k, v, count(v) over(partition by grp order by k desc range between 10 preceding and current row) as c",
                "numeric_t",
                "",
                "order by k desc, ts",
            ),
            (
                "F008_null_default",
                "k, v, sum(v) over(order by k) as s",
                "null_order_t",
                "",
                "order by k, ts",
            ),
            (
                "F008_nulls_last",
                "k, v, sum(v) over(order by k asc nulls last) as s",
                "null_order_t",
                "",
                "order by k asc nulls last, ts",
            ),
            (
                "F008_nulls_first_desc",
                "k, v, sum(v) over(order by k desc nulls first) as s",
                "null_order_t",
                "",
                "order by k desc nulls first, ts",
            ),
            (
                "F008_null_range_preceding",
                "k, v, sum(v) over(order by k range between unbounded preceding and 1 preceding) as s",
                "null_order_t",
                "",
                "order by k, ts",
            ),
            (
                "F008_null_desc_range_following",
                "k, v, sum(v) over(order by k desc range between 1 following and unbounded following) as s",
                "null_order_t",
                "",
                "order by k desc, ts",
            ),
            (
                "F024_range_multi_order",
                "k1, k2, v, count(v) over(order by k1, k2 range between current row and current row) as c, "
                "sum(v) over(order by k1, k2) as s",
                "multi_order_t",
                "",
                "order by k1, k2, ts",
            ),
            (
                "F010_default_vs_explicit_range",
                "k, v, sum(v) over(order by k) as dft, "
                "sum(v) over(order by k range between unbounded preceding and current row) as expl",
                "numeric_t",
                "",
                "order by k, ts",
            ),
        ]
        for case in range_cases:
            self._write_sql(self._cid_select(*case))

        for cid, func, table, where, win in self.aggregate_funcs:
            columns = f"v, {func} over({win}) as w"
            self._write_sql(self._cid_select(cid, columns, table, where, "order by ts"))

        selection_cases = [
            (
                "F028_first_last",
                "v, first(v) over(partition by dev order by ts rows between unbounded preceding and current row) as f, "
                "last(v) over(partition by dev order by ts rows between unbounded preceding and current row) as l, "
                "last_row(v) over(partition by dev order by ts rows between unbounded preceding and current row) as lr",
            ),
            (
                "F028_selection_empty",
                "v, first(v) over(partition by dev order by ts rows between 1 preceding and 1 preceding) as f, "
                "last(v) over(partition by dev order by ts rows between 1 preceding and 1 preceding) as l",
            ),
        ]
        for cid, columns in selection_cases:
            self._write_sql(self._cid_select(cid, columns, "rows_t", "where dev = 'd1'", "order by ts"))

        ranking_columns = (
            "grp, score, row_number() over(partition by grp order by score, ts) as rn, "
            "rank() over(partition by grp order by score) as r, "
            "dense_rank() over(partition by grp order by score) as dr, "
            "percent_rank() over(partition by grp order by score) as pr, "
            "cume_dist() over(partition by grp order by score) as cd"
        )
        self._write_sql(
            self._cid_select(
                "F030_ranking_distribution", ranking_columns, "rank_t", "", "order by grp, score, ts"
            )
        )
        frame_ranking_columns = (
            "grp, score, rank() over(partition by grp order by score rows between current row and current row) as r, "
            "dense_rank() over(partition by grp order by score rows between current row and current row) as dr, "
            "percent_rank() over(partition by grp order by score rows between current row and current row) as pr, "
            "cume_dist() over(partition by grp order by score rows between current row and current row) as cd"
        )
        self._write_sql(
            self._cid_select(
                "F034_ranking_frame_ignored", frame_ranking_columns, "rank_t", "", "order by grp, score, ts"
            )
        )
        self._write_sql(
            self._cid_select(
                "F035_ranking_expr_order",
                "score, rank() over(partition by grp order by score + 0) as r, "
                "dense_rank() over(partition by grp order by score + 0) as dr, "
                "cume_dist() over(partition by grp order by score + 0) as cd",
                "rank_double_t",
                "",
                "order by score, ts",
            )
        )

        offset_columns = (
            "v, lag(v) over(partition by dev order by ts) as lag1, "
            "lead(v) over(partition by dev order by ts) as lead1, "
            "lag(v, 0) over(partition by dev order by ts) as lag0, "
            "lead(v, 0) over(partition by dev order by ts) as lead0, "
            "lag(v, 2, -1) over(partition by dev order by ts) as lag2, "
            "lead(v, 2, -1) over(partition by dev order by ts) as lead2, "
            "lag(v, 4294967296, -1) over(partition by dev order by ts) as lag_big, "
            "lead(v, 9223372036854775807, -1) over(partition by dev order by ts) as lead_big"
        )
        self._write_sql(self._cid_select("F036_lag_lead", offset_columns, "rows_t", "where dev = 'd1'", "order by ts"))
        self._write_sql(
            self._cid_select(
                "F037_value_functions",
                "v, first_value(v) over(partition by dev order by ts rows between unbounded preceding and current row) as fv, "
                "last_value(v) over(partition by dev order by ts) as lv, "
                "nth_value(v, 2) over(partition by dev order by ts rows between unbounded preceding and current row) as nth2, "
                "nth_value(v, 10) over(partition by dev order by ts rows between unbounded preceding and current row) as nth10, "
                "nth_value(v, 9223372036854775807) over(partition by dev order by ts rows between 1 preceding and current row) as nth_big",
                "rows_t",
                "where dev = 'd1'",
                "order by ts",
            )
        )
        self._write_sql(
            self._cid_select(
                "F036_lag_lead_text",
                "name, lag(name, 1, 'missing') over(partition by dev order by ts) as prev_name, "
                "lead(name, 10, 'tail') over(partition by dev order by ts) as next_name",
                "rows_t",
                "where dev = 'd1'",
                "order by ts",
            )
        )
        self._write_sql(
            self._cid_select(
                "F036_lag_ts_default",
                "lag(ts2, 1, '2023-01-01 00:00:00.000') over(partition by dev order by ts) as prev_ts",
                "rows_t",
                "where dev = 'd1'",
                "order by ts",
            )
        )
        self._write_sql(
            """
            select 'F036_lag_geometry' as cid, st_astext(g0) as gtxt from (
                select lag(g, 1, 'POINT(9 9)') over(partition by grp order by ts) as g0, ts
                from geom_t
            ) q order by ts
            """
        )

        named_cases = [
            """
            select 'F039_named_reuse' as cid, v, avg(v) over win as a, max(v) over win as m
            from named_t
            window win as (partition by grp order by ts rows between 1 preceding and current row)
            order by ts
            """,
            """
            select 'F040_named_multi' as cid, v, sum(v) over win1 as prefix_s, sum(v) over win2 as suffix_s
            from named_t
            window win1 as (partition by grp order by ts rows between unbounded preceding and current row),
                   win2 as (partition by grp order by ts rows between current row and unbounded following)
            order by ts
            """,
        ]
        for sql in named_cases:
            self._write_sql(sql)

        composition_cases = [
            """
            select 'F002_order_alias' as cid, v, avg(v) over(order by ts rows between unbounded preceding and current row) as ma
            from rows_t where dev = 'd1' order by ma desc
            """,
            """
            select 'F002_order_window_expr' as cid, v from rows_t where dev = 'd1'
            order by rank() over(order by v desc), v
            """,
            """
            select 'F044_subquery_projection' as cid, dev, v, ma from (
                select dev, v, avg(v) over(partition by dev order by ts rows between 1 preceding and current row) as ma
                from rows_t where dev in ('d1', 'd2')
            ) q order by dev, v
            """,
            """
            select 'F045_outer_filter' as cid, dev, cur, prev_v from (
                select dev, v as cur, lag(v) over(partition by dev order by ts) as prev_v
                from rows_t where dev in ('d1', 'd2')
            ) q where prev_v is not null and cur > prev_v order by dev, cur
            """,
            """
            select 'F046_outer_group' as cid, is_rise, count(*) as c from (
                select case when prev_v is not null and cur > prev_v then 1 else 0 end as is_rise
                from (
                    select v as cur, lag(v) over(partition by dev order by ts) as prev_v
                    from rows_t where dev in ('d1', 'd2')
                ) w
            ) q group by is_rise order by is_rise
            """,
            """
            select 'F047_bi_nested' as cid, t2.__fcol_5, t2.__fcol_6, t2.__fcol_10,
                   min(t2.__fcol_10) over(partition by t2.__fcol_6) as __fcol_17
            from (
                select t1.__fcol_5, t1.__fcol_6,
                       max(t1.__fcol_5) over(partition by t1.__fcol_6) as __fcol_10
                from (
                    select t0.value_col as __fcol_5,
                           t0.group_col as __fcol_6,
                           t0.flag_col as __fcol_7
                    from bi_t t0
                ) t1
            ) t2
            order by t2.__fcol_5
            """,
            """
            select 'F049_offset_0' as cid, v from rows_t where dev = 'd1' order by ts offset 0
            """,
            """
            select 'F049_offset_2' as cid, v from rows_t where dev = 'd1' order by ts offset 2
            """,
            """
            select 'F049_offset_overflow' as cid, v from rows_t where dev = 'd1' order by ts offset 99
            """,
            """
            select 'F050_offset_after_window' as cid, v,
                   avg(v) over(partition by dev order by ts rows between 1 preceding and current row) as ma
            from rows_t where dev = 'd1' order by ts offset 2
            """,
            """
            select 'F055_empty_input' as cid, sum(v) over(order by ts) as s, row_number() over(order by ts) as rn
            from rows_t where dev = 'missing' order by ts
            """,
            """
            select 'F056_single_row_partition' as cid, dev, v, sum(v) over(partition by dev) as s,
                   row_number() over(partition by dev order by ts) as rn,
                   rank() over(partition by dev order by v) as r,
                   dense_rank() over(partition by dev order by v) as dr,
                   percent_rank() over(partition by dev order by v) as pr,
                   cume_dist() over(partition by dev order by v) as cd,
                   lag(v) over(partition by dev order by ts) as lag1,
                   lead(v, 1, -1) over(partition by dev order by ts) as lead1
            from rows_t where dev = 'd3' order by ts
            """,
        ]
        for sql in composition_cases:
            self._write_sql(sql)

        deterministic_sql = """
            select 'F057_deterministic_repeat' as cid, dev, v,
                   sum(v) over(partition by dev order by ts rows between unbounded preceding and current row) as s,
                   rank() over(partition by dev order by v) as r
            from rows_t where dev in ('d1', 'd2') order by dev, ts
        """
        for _ in range(3):
            self._write_sql(deterministic_sql)

    def _generate_negative_sqls(self):
        for func in self.order_sensitive_funcs:
            self._write_sql(
                f"select 'E011_missing_order_{func.split('(')[0]}' as cid, {func} over(partition by dev) as w from rows_t"
            )

        error_sqls = [
            "select 'E041_undefined_window' as cid, avg(v) over missing as w from rows_t",
            "select 'E041_duplicate_window' as cid, avg(v) over win as w from rows_t window win as (order by ts), win as (order by v)",
            "select 'E042_inner_window_scope' as cid, avg_v from (select avg(v) over win as avg_v from rows_t) t window win as (order by avg_v)",
            "select 'E042_outer_window_scope' as cid, avg(v) over win as w from (select v from rows_t window win as (order by ts)) t",
            "select 'E043_extend_named_window' as cid, avg(v) over(win order by ts) as w from rows_t window win as (partition by dev)",
            "select 'E043_inherit_named_window' as cid, avg(v) over win2 as w from rows_t window win1 as (order by ts), win2 as (win1)",
            "select 'E018_invalid_rows_order' as cid, sum(v) over(order by ts rows between 1 following and 1 preceding) as w from rows_t",
            "select 'E018_negative_rows' as cid, sum(v) over(order by ts rows between -1 preceding and current row) as w from rows_t",
            "select 'E018_decimal_rows' as cid, sum(v) over(order by ts rows between 1.5 preceding and current row) as w from rows_t",
            "select 'E018_invalid_unbounded' as cid, sum(v) over(order by ts rows between current row and unbounded preceding) as w from rows_t",
            "select 'E025_range_offset_multi_order' as cid, sum(v) over(order by k1, k2 range between 1 preceding and current row) as w from multi_order_t",
            "select 'E025_range_text_order' as cid, sum(v) over(order by name range between 1 preceding and current row) as w from rows_t",
            "select 'E025_range_bool_order' as cid, sum(v) over(order by flag range between 1 preceding and current row) as w from rows_t",
            "select 'E026_bad_time_unit' as cid, sum(v) over(order by ts range between 1x preceding and current row) as w from rows_t",
            "select 'E036_lag_negative' as cid, lag(v, -1) over(partition by dev order by ts) as w from rows_t",
            "select 'E036_lag_decimal' as cid, lag(v, 1.5) over(partition by dev order by ts) as w from rows_t",
            "select 'E036_lag_bad_default' as cid, lag(v, 1, 'bad-int') over(partition by dev order by ts) as w from rows_t",
            "select 'E036_lead_negative' as cid, lead(v, -1) over(partition by dev order by ts) as w from rows_t",
            "select 'E038_nth_zero' as cid, nth_value(v, 0) over(partition by dev order by ts) as w from rows_t",
            "select 'E038_nth_negative' as cid, nth_value(v, -1) over(partition by dev order by ts) as w from rows_t",
            "select 'E038_nth_decimal' as cid, nth_value(v, 1.5) over(partition by dev order by ts) as w from rows_t",
            "select 'E038_nth_missing_arg' as cid, nth_value(v) over(partition by dev order by ts) as w from rows_t",
            "select 'E038_nth_extra_arg' as cid, nth_value(v, 1, 2) over(partition by dev order by ts) as w from rows_t",
            "select 'E051_where_position' as cid, v from rows_t where row_number() over(order by ts) > 1",
            "select 'E051_group_position' as cid, row_number() over(order by ts) as rn, count(*) as c from rows_t group by row_number() over(order by ts)",
            "select 'E051_having_position' as cid, dev, count(*) as c from rows_t group by dev having rank() over(order by dev) > 1",
            "select 'E052_scalar_arg' as cid, abs(row_number() over(order by ts)) as w from rows_t",
            "select 'E052_nested_window' as cid, avg(v) over(partition by row_number() over(order by ts) order by ts) as w from rows_t",
            "alter table stb_d1 set tag device = cast(row_number() over(order by ts) as varchar)",
            "create topic topic_window_illegal as select row_number() over(order by ts) from rows_t",
            "select 'E053_typo_function' as cid, densk_rank() over(order by ts) as w from rows_t",
            "select 'E053_unknown_function' as cid, missing_window_func(v) over(order by ts) as w from rows_t",
            "select 'E054_lag_no_arg' as cid, lag() over(order by ts) as w from rows_t",
            "select 'E054_lead_too_many_args' as cid, lead(v, 1, 2, 3) over(order by ts) as w from rows_t",
        ]
        for sql in error_sqls:
            self._write_sql(sql)

    def execMatrixCase(self):
        tdLog.info("execMatrixCase begin")
        self.openSqlTmpFile()
        self._generate_window_matrix_sqls()
        self._generate_positive_sqls()
        self._generate_negative_sqls()
        self.generated_queries_file.close()

        tmp_file = os.path.join(
            self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql"
        )
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        return True
