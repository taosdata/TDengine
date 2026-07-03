from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamSubqueryCountScalar:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_count_window_scalar_subquery_reuses_current_window(self):
        """Count window stream scalar subquery should use one window at a time

        1. Create a count_window stream over one child table.
        2. Use _twstart/_twend and %%tbname inside a nested scalar subquery.
        3. Insert three rows so one trigger batch contains two count windows.
        4. Verify the scalar subquery is evaluated per current window and the
           stream writes two result rows.

        Catalog:
            - Streams:SubQuery

        Since: v3.4.1.13

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-07-01 OpenAI Created
        """

        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        tdSql.prepare(dbname="scalar_repro", vgroups=1)
        tdSql.execute(
            "create stable scalar_repro.st "
            "(ts timestamp, soc int, trade_no varchar(32)) "
            "tags (pile_code varchar(32), gun_code varchar(32))"
        )
        tdSql.execute("create table scalar_repro.t1 using scalar_repro.st tags ('P1', 'G1')")

        tdSql.execute(
            "create stream scalar_repro.scalar_clean count_window(2,1) "
            "from scalar_repro.st partition by tbname,pile_code,gun_code "
            "stream_options(pre_filter(trade_no like 'T_CLEAN_%')) "
            "into scalar_repro.scalar_clean_out "
            "output_subtable(concat('scalar_',%%1)) "
            "as select _twend as ts, last(soc) as soc, first(soc) as prev_soc, "
            "last(trade_no) as trade_no "
            "from %%tbname "
            "where ts >= _twstart and ts <= _twend "
            "and trade_no like 'T_CLEAN_%' "
            "and ts >= ("
            "  select min(ts) from %%tbname "
            "  where soc != 0 and ts <= _twstart "
            "  and trade_no = ("
            "    select last(trade_no) from %%tbname "
            "    where ts >= _twstart and ts <= _twend "
            "    and trade_no like 'T_CLEAN_%'"
            "  )"
            ")"
        )
        tdStream.checkStreamStatus("scalar_clean")

        tdSql.execute(
            "insert into scalar_repro.t1 values "
            "('2026-01-01 00:00:00.000', 10, 'T_CLEAN_A') "
            "('2026-01-01 00:00:01.000', 11, 'T_CLEAN_A') "
            "('2026-01-01 00:00:02.000', 12, 'T_CLEAN_A')"
        )

        tdSql.checkResultsByFunc(
            "select ts, soc, prev_soc, trade_no "
            "from scalar_repro.scalar_clean_out order by ts",
            lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2026-01-01 00:00:01.000")
            and tdSql.compareData(0, 1, 11)
            and tdSql.compareData(0, 2, 10)
            and tdSql.compareData(0, 3, "T_CLEAN_A")
            and tdSql.compareData(1, 0, "2026-01-01 00:00:02.000")
            and tdSql.compareData(1, 1, 12)
            and tdSql.compareData(1, 2, 11)
            and tdSql.compareData(1, 3, "T_CLEAN_A"),
            retry=20,
        )

        tdStream.dropAllStreamsAndDbs()
