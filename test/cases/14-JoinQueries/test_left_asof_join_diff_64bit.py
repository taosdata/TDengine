import datetime

from new_test_framework.utils import tdLog, tdSql, etool


class TestLeftAsofJoinDiff64bit:
    DB_NAME = "ts7045210011"
    FRONT_TABLE = "front_machine"
    BACK_TABLE = "back_machine"

    BASE_TIME = datetime.datetime(2026, 7, 1, 0, 0, 1)
    TOTAL_ROWS = 70002
    BATCH_SIZE = 2000

    FIRST_PICI_ROWS = 50000
    RANGE_START_OFFSET = 65535
    RANGE_END_OFFSET = 70001

    PICI_FIRST = "DD26070111M056"
    PICI_SECOND = "DD26070113M001"
    PICI_THIRD = "DD26070308M001"
    PICI_FOURTH = "DD26070400M002"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def _ts_ms(self, offset_seconds):
        return int((self.BASE_TIME + datetime.timedelta(seconds=offset_seconds)).timestamp() * 1000)

    def _ts_str(self, offset_seconds):
        return (self.BASE_TIME + datetime.timedelta(seconds=offset_seconds)).strftime("%Y-%m-%d %H:%M:%S.000")

    def _drop_db(self):
        tdSql.execute(f"drop database if exists {self.DB_NAME}")

    def _prepare_schema(self):
        self._drop_db()
        tdSql.execute(f"create database {self.DB_NAME} keep 36500")
        tdSql.execute(f"use {self.DB_NAME}")
        tdSql.execute(
            f"""
            create table {self.FRONT_TABLE} (
                ts timestamp,
                duny1_gt_zs bigint,
                duny2_gt_zs bigint,
                dutn1_gt_zs bigint,
                dutn2_gt_zs bigint,
                duxn1_gt_zs bigint,
                duxn2_gt_zs bigint
            )
            """
        )
        tdSql.execute(
            f"""
            create table {self.BACK_TABLE} (
                ts timestamp,
                pici_hao binary(32),
                chanxian_zhuangtai int
            )
            """
        )

    def _insert_front_rows(self):
        tdLog.info("=== insert front_machine rows for 64-bit diff regression")
        for start in range(0, self.TOTAL_ROWS, self.BATCH_SIZE):
            values = []
            end = min(start + self.BATCH_SIZE, self.TOTAL_ROWS)
            for row_idx in range(start, end):
                ts_ms = self._ts_ms(row_idx)
                values.append(
                    f"({ts_ms}, {row_idx}, {row_idx + 10}, {row_idx + 20}, {row_idx + 30}, {row_idx + 40}, {row_idx + 50})"
                )
            tdSql.execute(f"insert into {self.FRONT_TABLE} values " + ",".join(values))

    def _insert_back_rows(self):
        tdLog.info("=== insert back_machine rows for pici switches")
        second_offset = self.FIRST_PICI_ROWS
        third_offset = self.RANGE_START_OFFSET
        fourth_offset = self.RANGE_END_OFFSET - 1
        tdSql.execute(
            f"""
            insert into {self.BACK_TABLE} values
                ({self._ts_ms(0)}, '{self.PICI_FIRST}', 1),
                ({self._ts_ms(second_offset)}, '{self.PICI_SECOND}', 1),
                ({self._ts_ms(third_offset)}, '{self.PICI_THIRD}', 1),
                ({self._ts_ms(fourth_offset)}, '{self.PICI_FOURTH}', 1)
            """
        )

    def _prepare_data(self):
        self._prepare_schema()
        self._insert_front_rows()
        self._insert_back_rows()
        print("prepare left asof join diff data ............ [ passed ]")

    def _join_from_clause(self):
        return f"""
            from {self.FRONT_TABLE} a
            left asof join {self.BACK_TABLE} b
            on a.ts >= b.ts
            where b.ts > '2026-07-01'
              and b.ts < '2026-07-09'
              and a.ts > '2026-07-01'
              and a.ts < '2026-07-09'
              and b.chanxian_zhuangtai = 1
        """

    def _diff_query_core(self, with_filter=False):
        extra_filter = f" and b.pici_hao = '{self.PICI_FIRST}'" if with_filter else ""
        return f"""
            select
                a.ts,
                b.pici_hao as pici_hao,
                abs(diff(duny1_gt_zs, 0)) as diff_duny1_gt_zs,
                abs(diff(duny2_gt_zs, 2)) as diff_duny2_gt_zs,
                abs(diff(dutn1_gt_zs, 2)) as diff_dutn1_gt_zs,
                abs(diff(dutn2_gt_zs, 2)) as diff_dutn2_gt_zs,
                abs(diff(duxn1_gt_zs, 2)) as diff_duxn1_gt_zs,
                abs(diff(duxn2_gt_zs, 2)) as diff_duxn2_gt_zs
            {self._join_from_clause()}
            {extra_filter}
        """

    def _raw_query_core(self):
        return f"""
            select
                a.ts,
                b.pici_hao as pici_hao,
                duny1_gt_zs as diff_duny1_gt_zs,
                duny2_gt_zs as diff_duny2_gt_zs,
                dutn1_gt_zs as diff_dutn1_gt_zs,
                dutn2_gt_zs as diff_dutn2_gt_zs,
                duxn1_gt_zs as diff_duxn1_gt_zs,
                duxn2_gt_zs as diff_duxn2_gt_zs
            {self._join_from_clause()}
        """

    def _outer_diff_query_core(self):
        return f"""
            select
                ts,
                pici_hao,
                abs(diff(duny1_gt_zs, 0)) as diff_duny1_gt_zs,
                abs(diff(duny2_gt_zs, 2)) as diff_duny2_gt_zs,
                abs(diff(dutn1_gt_zs, 2)) as diff_dutn1_gt_zs,
                abs(diff(dutn2_gt_zs, 2)) as diff_dutn2_gt_zs,
                abs(diff(duxn1_gt_zs, 2)) as diff_duxn1_gt_zs,
                abs(diff(duxn2_gt_zs, 2)) as diff_duxn2_gt_zs
            from (
                select
                    a.ts,
                    b.pici_hao as pici_hao,
                    duny1_gt_zs,
                    duny2_gt_zs,
                    dutn1_gt_zs,
                    dutn2_gt_zs,
                    duxn1_gt_zs,
                    duxn2_gt_zs
                {self._join_from_clause()}
            ) t
        """

    def _assert_count(self, sql, expected):
        count = int(tdSql.getFirstValue(f"select count(*) from ({sql}) t"))
        if count != expected:
            raise AssertionError(f"expect count {expected}, actual {count}")

    def _assert_range_boundary(self, sql, expected_count, first_pici, last_pici):
        range_start = self._ts_str(self.RANGE_START_OFFSET)
        range_end = self._ts_str(self.RANGE_END_OFFSET)
        range_sql = f"""
            select *
            from ({sql}) t
            where ts >= '{range_start}'
              and ts <= '{range_end}'
        """
        range_count = int(tdSql.getFirstValue(f"select count(*) from ({range_sql}) s"))
        if range_count != expected_count:
            raise AssertionError(f"expect range count {expected_count}, actual {range_count}")

        tdSql.query(
            f"""
            select pici_hao, timediff(ts, '{range_start}', 1s)
            from ({range_sql}) s
            order by ts asc
            limit 1
            """
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, first_pici)
        tdSql.checkData(0, 1, 0)

        tdSql.query(
            f"""
            select pici_hao, timediff(ts, '{range_end}', 1s)
            from ({range_sql}) s
            order by ts desc
            limit 1
            """
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_pici)
        tdSql.checkData(0, 1, 0)

    def do_filtered_diff_case(self):
        sql = self._diff_query_core(with_filter=True)
        self._assert_count(sql, self.FIRST_PICI_ROWS - 1)
        tdSql.query(f"select * from ({sql}) t order by ts desc limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, self.PICI_FIRST)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 7, 1)
        print("filtered left asof join diff ............... [ passed ]")

    def do_unfiltered_diff_case(self):
        sql = self._diff_query_core(with_filter=False)
        self._assert_count(sql, self.TOTAL_ROWS - 1)
        self._assert_range_boundary(
            sql,
            self.RANGE_END_OFFSET - self.RANGE_START_OFFSET + 1,
            self.PICI_THIRD,
            self.PICI_FOURTH,
        )
        tdSql.query(f"select * from ({sql}) t order by ts desc limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 7, 1)
        print("unfiltered left asof join diff ............. [ passed ]")

    def do_unfiltered_raw_case(self):
        sql = self._raw_query_core()
        self._assert_count(sql, self.TOTAL_ROWS)
        self._assert_range_boundary(
            sql,
            self.RANGE_END_OFFSET - self.RANGE_START_OFFSET + 1,
            self.PICI_THIRD,
            self.PICI_FOURTH,
        )
        tdSql.query(f"select * from ({sql}) t order by ts desc limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, self.TOTAL_ROWS - 1)
        tdSql.checkData(0, 7, self.TOTAL_ROWS - 1 + 50)
        print("unfiltered left asof join raw columns ...... [ passed ]")

    def do_outer_diff_case(self):
        sql = self._outer_diff_query_core()
        self._assert_count(sql, self.TOTAL_ROWS - 1)
        self._assert_range_boundary(
            sql,
            self.RANGE_END_OFFSET - self.RANGE_START_OFFSET + 1,
            self.PICI_THIRD,
            self.PICI_FOURTH,
        )
        tdSql.query(f"select * from ({sql}) t order by ts desc limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 7, 1)
        print("outer subquery diff ........................ [ passed ]")

    # --- main ---
    def test_left_asof_join_diff_64bit(self):
        """Left asof join diff regression with large result rows

        1. Build front/back tables whose left asof join output exceeds 65535 diff rows
        2. Verify the filtered query stays correct below the old uint16 threshold
        3. Verify the unfiltered diff query keeps the rows immediately after the old 65535 boundary
        4. Verify the no-diff control query still returns the full joined rows
        5. Verify the outer-subquery-and-then-diff shape returns the same boundary-crossing range

        Catalog:
            - JoinQuery

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-07-10 Codex add left asof join diff large-row regression

        """
        try:
            self._prepare_data()
            self.do_filtered_diff_case()
            self.do_unfiltered_diff_case()
            self.do_unfiltered_raw_case()
            self.do_outer_diff_case()
        finally:
            self._drop_db()
