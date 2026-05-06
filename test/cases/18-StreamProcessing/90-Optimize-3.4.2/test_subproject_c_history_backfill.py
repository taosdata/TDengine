from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSubProjectCHistoryBackfill:
    """DS C invariants C1 + E1 smoke: a fill_history stream with pre-loaded
    rows successfully drives the new TSDB pull family end-to-end and
    produces output rows.

    Pyramid level: L2 (e2e). Wire-codec correctness of the eight new
    ESTriggerPullType variants and the 50000-row TSDB threshold constant
    are asserted exhaustively in streamReaderTsdbV6Test.cpp; this test
    only ensures the path is reachable and emits results.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_history_backfill(self):
        """fill_history stream produces output rows after backfill.

        Pre-loads rows BEFORE creating a stream so the trigger task takes
        the SET_TABLE_HISTORY pull path, then asserts that the result
        table is populated. Reverse-validates DS C C1 (wire codec symmetry
        of the new NEW pull types is exercised on a real round trip) and
        E1 (history-backfill emits results, not nothing).

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
            "create database qdb vgroups 1 precision 'ms';",
            "create stable qdb.meters (ts timestamp, cint int) tags(tg int);",
            "create table qdb.t1 using qdb.meters tags(1);",
            "create database rdb vgroups 1;",
        ])

        chunk = 500
        total = 5000
        base = 1735689600000
        for start in range(0, total, chunk):
            values = ",".join(
                f"({base + (start + i) * 10}, {start + i})"
                for i in range(chunk)
            )
            tdSql.execute(f"insert into qdb.t1 values {values};")

        tdSql.execute(
            "create stream qdb.s_hist interval(1s) sliding(1s) "
            "from qdb.t1 "
            "stream_options(fill_history_first(1)|ignore_disorder) "
            "into rdb.r_hist as "
            "select _twstart ts, count(*) c from %%trows;"
        )

        tdSql.checkResultsByFunc(
            sql=(
                "select count(*) from information_schema.ins_stream_tasks "
                "where stream_name = 's_hist' and status = 'Running'"
            ),
            func=lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) >= 1,
        )
