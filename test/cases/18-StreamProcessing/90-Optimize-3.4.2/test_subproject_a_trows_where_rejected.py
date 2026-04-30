from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSubProjectATrowsWhereRejected:
    """DS A invariant A4: writing %%trows together with WHERE in the calc
    must be rejected at parse time. Pyramid level: L2 negative test.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_trows_where_rejected(self):
        """%%trows combined with WHERE must error at create time.

        Asserts that the parser refuses any calc clause that mixes %%trows
        with a WHERE predicate. This complements the A4 unit-test path by
        verifying the rejection is observable end-to-end at SQL ingress.

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
            "create stable qdb.meters (ts timestamp, cint int) tags(tg int);",
            "create table qdb.t1 using qdb.meters tags(1);",
            "insert into qdb.t1 values ('2025-01-01 00:00:00', 1);",
            "create database rdb vgroups 1;",
        ])

        # Mixing %%trows with a calc-side WHERE must fail at parse time;
        # any other failure mode (success / runtime crash) is a regression.
        tdSql.error(
            "create stream qdb.s_bad interval(1s) sliding(1s) "
            "from qdb.meters partition by tbname "
            "stream_options(ignore_disorder) "
            "into rdb.r_bad as "
            "select _twstart ts, avg(cint) v from %%trows where cint > 0;"
        )
