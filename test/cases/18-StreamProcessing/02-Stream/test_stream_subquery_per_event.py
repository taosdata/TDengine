import time

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamSubqueryPerEvent:
    """Per-event re-evaluation of stream WHERE subqueries (customer ticket).

    The customer reproducer originally relied on a scalar subquery in the
    stream body to provide a per-event lower bound on a secondary source:

        WHERE ts >= (SELECT last_row(ts) FROM inicio_descarga)

    Two shapes are pinned here so any regression in either path is caught:

      1. test_where_subquery
         The original SQL shape with the scalar subquery in WHERE.
         This exercises the REMOTE_VALUE path (qFetchRemoteNode cache
         bypass in stream mode, sclInitParam REMOTE_VALUE handling,
         setTaskScalarExtraInfo on every fetch worker thread, etc.).
         Before the fix, the subquery was constant-folded at CREATE
         STREAM time and every event reported SUM=3.

      2. test_twstart_workaround
         The customer-suggested workaround: make inicio_descarga the
         trigger and reference _twstart in the body. This exercises the
         placeholder substitution path and does not touch the subquery
         code; it serves as a control that the count_window engine
         itself behaves as expected.

    Both shapes share identical pre-seeded cumple_descarga data (rows
    at 00:00:00, 00:00:01, 00:00:02 with cumple=1, total=1) and both
    must yield the same per-event progression as inicio.ts advances:

        event 1 (inicio @ 00:00:00) -> SUM=(3, 3)
        event 2 (inicio @ 00:00:01) -> SUM=(2, 2)
        event 3 (inicio @ 00:00:02) -> SUM=(1, 1)
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode()

    def test_where_subquery(self):
        """WHERE scalar subquery is re-evaluated per trigger event.

        1. Build linea_descarga (trigger), cumple_descarga (secondary
           source), inicio_descarga (referenced by WHERE subquery).
        2. Pre-seed cumple_descarga with three rows so SUM differs by
           the lower bound the subquery returns.
        3. Insert one row into inicio_descarga BEFORE creating the
           stream so the subquery is resolvable at plan time.
        4. Create a count_window(1,1,pressure) stream whose body filters
           cumple_descarga by ts >= (select last_row(ts) from inicio_descarga).
        5. Drive three trigger events one at a time, advancing
           inicio_descarga before each one - the customer pattern.
        6. Verify cumulative output after every event:
             after event 1: 1 row,  SUM=(3, 3)
             after event 2: 2 rows, (3,3) then (2,2)
             after event 3: 3 rows, (3,3) (2,2) (1,1)
           A regression to constant SUM=3 means the subquery has been
           constant-folded again and is no longer per-event.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-11 Created to pin customer reproducer behavior
            - 2026-05-13 Updated to dynamic per-event semantics after fix
        """

        streams = [self.WhereSubqueryDynamic()]
        tdStream.checkAll(streams)

    def test_twstart_workaround(self):
        """inicio_descarga as trigger + _twstart in body (control test).

        This documents the workaround the customer used before the
        engine fix and serves as a regression guard for the trigger /
        _twstart path, independent of the scalar-subquery code.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-11 Created to demonstrate workaround for ticket
        """

        streams = [self.SubqueryWorkaround()]
        tdStream.checkAll(streams)

    class WhereSubqueryDynamic(StreamCheckItem):
        def __init__(self):
            self.db = "test_subq_where"

        def create(self):
            tdLog.info(f"=== create db {self.db} and source tables ===")
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(
                "create table linea_descarga  (ts timestamp, pressure int)"
            )
            tdSql.execute(
                "create table cumple_descarga (ts timestamp, cumple int, total int)"
            )
            tdSql.execute(
                "create table inicio_descarga (ts timestamp, dummy int)"
            )

            # Pre-seed cumple_descarga so SUM differs by lower bound.
            tdSql.execute(
                "insert into cumple_descarga values "
                "('2026-05-01 00:00:00', 1, 1),"
                "('2026-05-01 00:00:01', 1, 1),"
                "('2026-05-01 00:00:02', 1, 1)"
            )

            # Subquery must resolve at CREATE STREAM time -> seed one row.
            tdSql.execute(
                "insert into inicio_descarga values ('2026-05-01 00:00:00', 1)"
            )

            expected_ts = "2026-05-01 00:00:00"
            deadline = time.time() + 10
            while True:
                tdSql.query("select last_row(ts) from inicio_descarga")
                if (
                    tdSql.queryResult
                    and tdSql.queryResult[0]
                    and str(tdSql.queryResult[0][0]).startswith(expected_ts)
                ):
                    break
                if time.time() >= deadline:
                    raise AssertionError(
                        "Timed out waiting for inicio_descarga seed row to become "
                        "visible to stream subquery resolution"
                    )
                time.sleep(0.5)

            tdLog.info("=== create stream analisis_68 ===")
            tdSql.execute(
                f"create stream analisis_68 count_window(1, 1, pressure) "
                f"from linea_descarga "
                f"into resultado_descarga as "
                f"select _twstart as ts, "
                f"       sum(cumple) as acumulado_cumple, "
                f"       sum(total)  as acumulado_total "
                f"from cumple_descarga "
                f"where ts >= (select last_row(ts) from inicio_descarga)"
            )

        def insert1(self):
            tdLog.info("=== event 1: trigger at 00:00:00 (inicio last_row=00:00:00) ===")
            tdSql.execute(
                "insert into linea_descarga values ('2026-05-01 00:00:00', 1)"
            )

        def check1(self):
            tdLog.info("=== check after event 1: 1 row, SUM=(3, 3) ===")
            tdSql.checkResultsByFunc(
                sql=f"select acumulado_cumple, acumulado_total "
                    f"from {self.db}.resultado_descarga order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, 3)
                and tdSql.compareData(0, 1, 3),
            )

        def insert2(self):
            tdLog.info(
                "=== advance inicio to 00:00:01, then trigger at 00:00:01 ==="
            )
            tdSql.execute(
                "insert into inicio_descarga values ('2026-05-01 00:00:01', 1)"
            )
            tdSql.execute(
                "insert into linea_descarga values ('2026-05-01 00:00:01', 1)"
            )

        def check2(self):
            tdLog.info("=== check after event 2: 2 rows, (3,3) then (2,2) ===")
            tdSql.checkResultsByFunc(
                sql=f"select acumulado_cumple, acumulado_total "
                    f"from {self.db}.resultado_descarga order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, 3)
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(1, 0, 2)
                and tdSql.compareData(1, 1, 2),
            )

        def insert3(self):
            tdLog.info(
                "=== advance inicio to 00:00:02, then trigger at 00:00:02 ==="
            )
            tdSql.execute(
                "insert into inicio_descarga values ('2026-05-01 00:00:02', 1)"
            )
            tdSql.execute(
                "insert into linea_descarga values ('2026-05-01 00:00:02', 1)"
            )

        def check3(self):
            tdLog.info("=== check after event 3: 3 rows, (3,3) (2,2) (1,1) ===")
            # Per-event re-evaluation: each event sees inicio_descarga's
            # last_row(ts) at trigger time (00:00:00, 00:00:01, 00:00:02),
            # so the matching cumple_descarga rows shrink with each event.
            tdSql.checkResultsByFunc(
                sql=f"select acumulado_cumple, acumulado_total "
                    f"from {self.db}.resultado_descarga order by ts",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, 3)
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(1, 0, 2)
                and tdSql.compareData(1, 1, 2)
                and tdSql.compareData(2, 0, 1)
                and tdSql.compareData(2, 1, 1),
            )

        def insert4(self):
            # Empty inicio_descarga, then trigger event 4. The subquery
            # now returns ZERO rows. Without the qFetchRemoteNode stream
            # branch clearing the per-subquery slot before refetch,
            # handleRemoteValueRes would fall into its "EOF after data"
            # branch and silently retain event 3's lower bound (00:00:02),
            # so event 4 would emit acumulado_cumple=1 just like event 3.
            tdLog.info(
                "=== empty inicio_descarga, then trigger event 4 ==="
            )
            tdSql.execute("delete from inicio_descarga")
            tdSql.execute(
                "insert into linea_descarga values ('2026-05-01 00:00:03', 1)"
            )

        def check4(self):
            tdLog.info(
                "=== check after event 4: 4th row must NOT be stale (1,1) ==="
            )
            # The fix means event 4 sees an empty subquery -> placeholder
            # is NULL -> WHERE evaluates to NULL -> aggregate over empty
            # input. The exact emitted shape (NULL row vs no row) is not
            # the contract we pin here; what we MUST guarantee is that
            # event 4 does not silently reuse event 3's value of 1.
            #
            # Use the existing polling helper instead of an open-coded
            # sleep loop so CI only waits as long as needed for the stream
            # output query to reach an acceptable stable shape.
            sql = (
                f"select acumulado_cumple, acumulado_total "
                f"from {self.db}.resultado_descarga order by ts"
            )
            tdSql.checkResultsByFunc(
                sql=sql,
                func=lambda: tdSql.getRows() == 3
                or (
                    tdSql.getRows() == 4
                    and not (
                        tdSql.getData(3, 0) == 1
                        and tdSql.getData(3, 1) == 1
                    )
                ),
            )
            tdSql.query(sql)
            rows = tdSql.getRows()
            assert rows in (3, 4), (
                f"unexpected row count {rows} after event 4"
            )
            if rows == 4:
                v0 = tdSql.getData(3, 0)
                v1 = tdSql.getData(3, 1)
                assert not (v0 == 1 and v1 == 1), (
                    "event 4 reused event 3's stale subquery value (1,1); "
                    "qFetchRemoteNode stream branch is not clearing the "
                    "subResNodes slot before refetch"
                )

    class SubqueryWorkaround(StreamCheckItem):
        def __init__(self):
            self.db = "test_subq_workaround"

        def create(self):
            tdLog.info(f"=== create db {self.db} and source tables ===")
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(
                "create table linea_descarga  (ts timestamp, pressure int)"
            )
            tdSql.execute(
                "create table cumple_descarga (ts timestamp, cumple int, total int)"
            )
            tdSql.execute(
                "create table inicio_descarga (ts timestamp, dummy int)"
            )

            tdSql.execute(
                "insert into cumple_descarga values "
                "('2026-05-01 00:00:00', 1, 1),"
                "('2026-05-01 00:00:01', 1, 1),"
                "('2026-05-01 00:00:02', 1, 1)"
            )

            tdLog.info("=== create workaround stream analisis_wa ===")
            # inicio_descarga is the trigger; each row forms its own
            # count_window(1) window; _twstart binds the per-window
            # lower bound dynamically into the cumple_descarga filter.
            tdSql.execute(
                f"create stream analisis_wa count_window(1, 1, dummy) "
                f"from inicio_descarga "
                f"into resultado_descarga as "
                f"select _twstart as ts, "
                f"       sum(cumple) as acumulado_cumple, "
                f"       sum(total)  as acumulado_total "
                f"from cumple_descarga "
                f"where ts >= _twstart"
            )

        def insert1(self):
            tdLog.info("=== inicio at 00:00:00 (matches all 3 cumple rows) ===")
            tdSql.execute(
                "insert into inicio_descarga values ('2026-05-01 00:00:00', 1)"
            )

        def check1(self):
            tdLog.info("=== check after inicio 1: 1 row, SUM=(3, 3) ===")
            tdSql.checkResultsByFunc(
                sql=f"select acumulado_cumple, acumulado_total "
                    f"from {self.db}.resultado_descarga order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, 3)
                and tdSql.compareData(0, 1, 3),
            )

        def insert2(self):
            tdLog.info(
                "=== inicio at 00:00:01 (matches cumple rows at 01, 02) ==="
            )
            tdSql.execute(
                "insert into inicio_descarga values ('2026-05-01 00:00:01', 1)"
            )

        def check2(self):
            tdLog.info(
                "=== check after inicio 2: 2 rows, second SUM=(2, 2) ==="
            )
            tdSql.checkResultsByFunc(
                sql=f"select acumulado_cumple, acumulado_total "
                    f"from {self.db}.resultado_descarga order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, 3)
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(1, 0, 2)
                and tdSql.compareData(1, 1, 2),
            )

        def insert3(self):
            tdLog.info(
                "=== inicio at 00:00:02 (matches only cumple row at 02) ==="
            )
            tdSql.execute(
                "insert into inicio_descarga values ('2026-05-01 00:00:02', 1)"
            )

        def check3(self):
            tdLog.info(
                "=== check after inicio 3: 3 rows, third SUM=(1, 1) ==="
            )
            tdSql.checkResultsByFunc(
                sql=f"select acumulado_cumple, acumulado_total "
                    f"from {self.db}.resultado_descarga order by ts",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, 3)
                and tdSql.compareData(0, 1, 3)
                and tdSql.compareData(1, 0, 2)
                and tdSql.compareData(1, 1, 2)
                and tdSql.compareData(2, 0, 1)
                and tdSql.compareData(2, 1, 1),
            )
