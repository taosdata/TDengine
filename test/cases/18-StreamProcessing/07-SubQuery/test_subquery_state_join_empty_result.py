import time

from new_test_framework.utils import tdLog, tdSql, tdStream, clusterComCheck


class TestStreamSubqueryStateJoinEmptyResult:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _ensure_snode(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode(1)

    def test_stream_subquery_state_join_empty_result(self):
        """Subquery: state window + join + outer filter should output empty result

        Reproduce bug:
        stream query outputs rows even when join-side filter removes all rows
        (expected empty result).

        Since: v3.3.8.x

        Labels: common, ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6799007996
        """

        self._ensure_snode()

        dbname = "stream_state_join_empty_bug"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.prepare(dbname=dbname, vgroups=1)
        clusterComCheck.checkDbReady(dbname)
        tdSql.execute(f"use {dbname}")

        tdSql.execute(
            """
            create stable src_evt (
                ts timestamp,
                group_id varchar(32),
                value_x int,
                amount double
            ) tags (
                tag_key varchar(32)
            )
            """
        )

        tdSql.execute(
            """
            create stable src_ref (
                ts timestamp,
                ref_v double
            ) tags (
                tag_key varchar(32)
            )
            """
        )

        tdSql.execute(
            """
            create stream st_result state_window(group_id)
            from src_evt
                partition by tbname, tag_key
            into st_result
                output_subtable(concat("st_result_", %%1))
            as
            select
                ts,
                group_id,
                score
            from (
                select
                    a.ts,
                    b.group_id,
                    case when b.delta_v is not null then b.amount / a.base_ref end score
                from (
                    select
                        _twstart ts,
                        last(ref_v) base_ref
                    from src_ref
                    where tag_key = %%2 and _c0 >= _twstart and _c0 < _twend
                ) a
                join (
                    select
                        ts,
                        (v_last - v_first) delta_v,
                        group_id,
                        amount
                    from (
                        select
                            _twstart ts,
                            last(group_id) group_id,
                            last(amount) amount,
                            last(value_x) v_last,
                            first(value_x) v_first
                        from %%tbname
                        where _c0 >= _twstart and _c0 < _twend and value_x <= 95 and value_x > 0
                    )
                    where v_last >= 95
                ) b
                on a.ts = b.ts
            )
            where score is not null
            """
        )

        tdStream.checkStreamStatus("st_result")

        tdSql.execute(
            """
            insert into src_ref_dev1 using src_ref tags ('dev1') (ts, ref_v) values ('2026-01-01 00:05:00', 400.0)
            """
        )

        tdSql.execute(
            """
            insert into src_evt_dev1 using src_evt tags ('dev1') (ts, group_id, amount, value_x) values
                ('2026-01-01 00:00:00', 'g_001', 20.0, 30),
                ('2026-01-01 00:10:00', 'g_001', 50.0, 80),
                ('2026-01-01 00:20:00', 'g_001', 80.0, 90),
                ('2026-01-01 00:30:00', 'g_002', 30.0, 40)
            """
        )

        tdSql.checkResultsByFunc(
            sql=f"select table_name from information_schema.ins_tables where db_name='{dbname}' and stable_name='st_result'",
            func=lambda: tdSql.getRows() == 1,
        )
        tdSql.checkResultsByFunc(
            sql=f"select count(*) from {dbname}.st_result",
            func=lambda: tdSql.compareData(0, 0, 0),
        )
