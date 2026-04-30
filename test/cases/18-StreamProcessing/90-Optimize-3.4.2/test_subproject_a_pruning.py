from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSubProjectAPruning:
    """End-to-end smoke test that DS subproject A's column-pruning rules
    yield a deployable plan for the four primary trigger families.

    Pyramid level: L2 (e2e). Plan-level correctness of column pruning is
    asserted exhaustively in parStreamTest.cpp; this test only ensures
    the pruned plan reaches Running for INTERVAL / STATE / EVENT / SESSION
    triggers. Together with the unit tests it covers DS A invariants
    A1, A2, and A3.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_subproject_a_pruning(self):
        """Subproject A column pruning end-to-end smoke.

        Builds four streams (interval, state-window, event-window, session)
        whose calc clauses reference different column subsets, then asserts
        every trigger task reaches Running. This proves the pruned plans
        are deployable on the dnode/snode for all four trigger families.

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
            "create stable qdb.meters (ts timestamp, cint int, cfloat float, cstr binary(16)) tags(tg int);",
            "create table qdb.t1 using qdb.meters tags(1);",
            "insert into qdb.t1 values "
            "('2025-01-01 00:00:00', 1, 1.5, 'a'),"
            "('2025-01-01 00:00:01', 2, 2.5, 'a'),"
            "('2025-01-01 00:00:02', 3, 3.5, 'b');",
            "create database rdb vgroups 1;",
        ])

        # A2: interval window - calc reads only avg(cint); cfloat/cstr must
        # be pruned out of the scan.
        tdSql.execute(
            "create stream qdb.s_interval interval(2s) sliding(2s) "
            "from qdb.t1 stream_options(ignore_disorder) "
            "into rdb.r_interval as select _twstart ts, avg(cint) v from %%trows;"
        )

        # A1+A3: state window keyed on cstr; pre_filter (cint > 0) lives
        # outside %%trows so calc must NOT scan cstr; trigger must NOT scan
        # cfloat.
        tdSql.execute(
            "create stream qdb.s_state state_window(cstr) "
            "from qdb.t1 "
            "stream_options(pre_filter(cint > 0)|ignore_disorder) "
            "into rdb.r_state as "
            "select _twstart ts, avg(cint) v from qdb.t1 "
            "where ts >= _twstart and ts < _twend;"
        )

        # A2 with %%trows + pre_filter: calc DOES need pre_filter columns.
        tdSql.execute(
            "create stream qdb.s_event event_window(start with cint >= 2 end with cint >= 4) "
            "from qdb.t1 "
            "stream_options(pre_filter(cint > 0)|ignore_disorder) "
            "into rdb.r_event as select _twstart ts, count(*) c, avg(cint) v from %%trows;"
        )

        # A1: session window - trigger only needs ts; calc references cfloat
        # which trigger never scans.
        tdSql.execute(
            "create stream qdb.s_session session(ts, 1s) "
            "from qdb.t1 stream_options(ignore_disorder) "
            "into rdb.r_session as select _twstart ts, sum(cfloat) v from %%trows;"
        )

        for sname in ("s_interval", "s_state", "s_event", "s_session"):
            tdSql.checkResultsByFunc(
                sql=(
                    "select count(*) from information_schema.ins_stream_tasks "
                    f"where stream_name = '{sname}' "
                    "and type = 'Trigger' and status = 'Running'"
                ),
                func=lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) >= 1,
            )
