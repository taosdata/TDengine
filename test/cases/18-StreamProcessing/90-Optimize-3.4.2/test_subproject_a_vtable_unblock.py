from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSubProjectAVTableUnblock:
    """DS A invariant A5: virtual-table + pre_filter + %%trows is no longer
    rejected (was TSDB_CODE_STREAM_INVALID_QUERY in v3.4.1).

    Pyramid level: L2 (e2e). The previous behavior raised at parse time, so
    the strongest negative-regression signal is to simply create such a
    stream and assert the trigger task reaches Running.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_subproject_a_vtable_unblock(self):
        """Virtual-table stream with pre_filter and %%trows is accepted.

        Before this PR, the parser refused any stream whose source was a
        virtual table while pre_filter and %%trows coexisted. This test
        constructs that exact configuration and asserts the stream is
        created successfully and the trigger task reaches Running, which is
        a contract-level reverse check on DS A invariant A5.

        Catalog:
            - Streams:Optimize:3.4.2

        Since: v3.4.2.0

        Labels: common,ci

        Jira: TS-7126
        """

        tdStream.dropAllStreamsAndDbs()
        try:
            tdStream.dropSnode()
        except Exception:
            pass
        tdStream.createSnode()

        tdSql.executes([
            "create database qdb vgroups 1;",
            "create stable qdb.meters (ts timestamp, cint int, cfloat float) tags(tg int);",
            "create table qdb.t1 using qdb.meters tags(1);",
            "create table qdb.t2 using qdb.meters tags(2);",
            "create table qdb.trig (ts timestamp, c1 int);",
            "insert into qdb.t1 values "
            "('2025-01-01 00:00:00', 1, 1.0),"
            "('2025-01-01 00:00:01', 2, 2.0),"
            "('2025-01-01 00:00:02', 3, 3.0);",
            "insert into qdb.t2 values "
            "('2025-01-01 00:00:00', 10, 10.0),"
            "('2025-01-01 00:00:01', 20, 20.0),"
            "('2025-01-01 00:00:02', 30, 30.0);",
            "create vtable qdb.v1 (ts timestamp, "
            "c1 int from qdb.t1.cint, c2 int from qdb.t2.cint, "
            "tg int from qdb.trig.c1);",
            "create database rdb vgroups 1;",
        ])

        # The combination below was rejected before this PR. Now it must
        # parse, plan, and reach Running on the trigger task.
        tdSql.execute(
            "create stream qdb.s_vt interval(1s) sliding(1s) "
            "from qdb.v1 "
            "stream_options(pre_filter(c1 > 0)|ignore_disorder) "
            "into rdb.r_vt as select _twstart ts, sum(c1) v from %%trows;"
        )

        tdStream.checkStreamStatus("s_vt")

        # Negative parity: confirm the parse-time error path is no longer
        # exercised; if a regression re-introduces the gate, create stream
        # itself would have failed above (so reaching here already proves
        # A5). Run a sanity insert to ensure the stream task does not crash
        # post-startup with the new pruning + dual-mode wiring.
        tdSql.execute(
            "insert into qdb.t1 values "
            "('2025-01-01 00:00:10', 100, 100.0);"
        )
        tdSql.execute(
            "insert into qdb.t2 values "
            "('2025-01-01 00:00:10', 200, 200.0);"
        )
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_stream_tasks "
                "where stream_name = 's_vt' and status = 'Running'",
            func=lambda: tdSql.getRows() >= 1,
        )
