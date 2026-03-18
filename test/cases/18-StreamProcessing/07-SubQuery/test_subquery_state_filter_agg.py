from new_test_framework.utils import tdLog, tdSql, tdStream, clusterComCheck


class TestStreamSubqueryStateFilterAgg:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _ensure_snode(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode(1)

    def test_stream_subquery_state_filter_agg(self):
        """Subquery: state window aggregate filter

        Reproduce bug: outer WHERE on aggregated column in stream subquery
        does not take effect. Also covers the empty output block case:
        first finalized window is filtered out, and later window should
        still be output.

        Since: v3.3.8.x

        Labels: common, ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6842208309

        """

        self._ensure_snode()

        dbname = "stream_filter_agg_bug"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.prepare(dbname=dbname, vgroups=1)
        clusterComCheck.checkDbReady(dbname)
        tdSql.execute(f"use {dbname}")

        tdSql.execute(
            "create stable stb (ts timestamp, c1 int, c2 int) tags (gid int)"
        )

        tdSql.execute(
            """
            create stream s1 state_window(c1)
            from stb partition by tbname
            into s1_res
            as
            select ts, c1, accum_c2
            from (
                select _twstart ts, last(c1) c1, last(c2) - first(c2) accum_c2
                from %%tbname
                where ts >= _twstart and ts <= _twend
            )
            where accum_c2 > 40
            """
        )

        tdStream.checkStreamStatus("s1")

        tdSql.execute("create table ctb_1 using stb tags (1)")
        tdSql.execute(
            """
            insert into ctb_1 values
                ('2026-01-01 00:00:01', 1, 10),
                ('2026-01-01 00:00:02', 1, 20),
                ('2026-01-01 00:00:03', 1, 30),
                ('2026-01-01 00:00:11', 2, 10)
            """
        )

        tdSql.checkResultsByFunc(
            sql=f"select table_name from information_schema.ins_tables where db_name='{dbname}' and stable_name='s1_res'",
            func=lambda: tdSql.getRows() == 1,
        )
        tdSql.checkResultsByFunc(
            sql=f"select count(*) from {dbname}.s1_res",
            func=lambda: tdSql.compareData(0, 0, 0),
        )

        tdSql.execute(
            """
            insert into ctb_1 values
                ('2026-01-01 00:00:12', 2, 20),
                ('2026-01-01 00:00:13', 2, 30),
                ('2026-01-01 00:00:17', 2, 70),
                ('2026-01-01 00:00:21', 3, 10)
            """
        )

        tdSql.checkResultsByFunc(
            sql="select c1, accum_c2 from s1_res order by ts, c1",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 2)
            and tdSql.compareData(0, 1, 60),
        )
