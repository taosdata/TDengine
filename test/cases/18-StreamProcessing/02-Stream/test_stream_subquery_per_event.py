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

    In stream mode every trigger event MUST refetch the subquery; the
    older code cached the result on the first event and silently
    replayed it forever.  This file pins the per-event semantics for all
    three remote-subquery flavours that flow through sclInitParam:

      1. test_where_subquery (REMOTE_VALUE)
         The original SQL shape with a scalar subquery in WHERE.
         Exercises qFetchRemoteNode, sclInitParam REMOTE_VALUE,
         setTaskScalarExtraInfo on every fetch worker thread, and
         (event 4) the slot-clear that makes empty later events take
         the first-call NULL branch instead of replaying the prior
         value, plus (event 5) setValueFromResBlock resetting
         pRes->isNull so the next non-NULL fetch isn't masked.

      2. test_twstart_workaround
         The customer-suggested workaround using _twstart.  Control
         test for the trigger / count_window engine, independent of
         the subquery code.

      3. test_in_list_subquery (REMOTE_VALUE_LIST)
         WHERE x IN (subquery) re-evaluation.  Pins the LIST cache
         invalidation: pHashFilter must be freed and
         VALUELIST_FLAG_VAL_UNSET re-armed every event.

      4. test_row_subquery (REMOTE_ROW)
         WHERE x > ANY (subquery) re-evaluation.  Pins the ROW cache
         invalidation: pRemote->valSet must be cleared every event.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        try:
            tdStream.createSnode()
        except Exception as e:
            if "Only one snode" not in str(e):
                raise

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
            tdSql.execute(f"drop database if exists {self.db}")
            # drop is async; wait for the dnode to fully release vgroups
            for _ in range(60):
                try:
                    tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
                    break
                except Exception as e:
                    if 'dropping' in str(e):
                        time.sleep(1)
                        continue
                    raise
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
            deadline = time.time() + 30
            while True:
                tdSql.query("select last_row(ts) from inicio_descarga")
                if (
                    tdSql.queryResult
                    and tdSql.queryResult[0]
                    and tdSql.queryResult[0][0] is not None
                ):
                    break
                if time.time() >= deadline:
                    raise AssertionError(
                        "Timed out waiting for inicio_descarga seed row to become "
                        f"visible (got {tdSql.queryResult!r})"
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
            rows_after_e4 = tdSql.getRows()
            assert rows_after_e4 in (3, 4), (
                f"unexpected row count {rows_after_e4} after event 4"
            )
            if rows_after_e4 == 4:
                v0 = tdSql.getData(3, 0)
                v1 = tdSql.getData(3, 1)
                assert not (v0 == 1 and v1 == 1), (
                    "event 4 reused event 3's stale subquery value (1,1); "
                    "qFetchRemoteNode stream branch is not clearing the "
                    "subResNodes slot before refetch"
                )

            # Event 5: re-populate inicio_descarga and trigger again.
            # This covers Finding 1: setValueFromResBlock must reset
            # pRes->isNull = false so the value placed by event 5 isn't
            # masked by the isNull=true left over from event 4's empty
            # fetch. With the bug, event 5's WHERE evaluates against a
            # NULL lower bound and matches no cumple rows -> aggregate
            # NULL. With the fix, the lower bound is 00:00:01 again
            # and exactly two cumple rows match -> SUM=(2, 2).
            tdLog.info(
                "=== event 5: re-insert inicio @ 00:00:01, trigger linea ==="
            )
            tdSql.execute(
                f"insert into {self.db}.inicio_descarga "
                f"values ('2026-05-01 00:00:01', 1)"
            )
            tdSql.execute(
                f"insert into {self.db}.linea_descarga "
                f"values ('2026-05-01 00:00:04', 1)"
            )
            # Wait for event 5 to land via the polling helper instead of
            # an open-coded sleep loop.
            tdSql.checkResultsByFunc(
                sql=sql,
                func=lambda: tdSql.getRows() > rows_after_e4
                and tdSql.getData(tdSql.getRows() - 1, 0) == 2
                and tdSql.getData(tdSql.getRows() - 1, 1) == 2,
            )
            tdSql.query(sql)
            rows_after_e5 = tdSql.getRows()
            assert rows_after_e5 > rows_after_e4, (
                f"event 5 produced no new row (was {rows_after_e4}, "
                f"now {rows_after_e5}); stream stalled after empty event"
            )
            last_row = rows_after_e5 - 1
            v0 = tdSql.getData(last_row, 0)
            v1 = tdSql.getData(last_row, 1)
            assert v0 == 2 and v1 == 2, (
                f"event 5 produced ({v0},{v1}); expected (2,2). "
                f"setValueFromResBlock did not reset pRes->isNull, so "
                f"event 4's NULL state masked the new subquery value."
            )

    class SubqueryWorkaround(StreamCheckItem):
        def __init__(self):
            self.db = "test_subq_workaround"

        def create(self):
            tdLog.info(f"=== create db {self.db} and source tables ===")
            tdSql.execute(f"drop database if exists {self.db}")
            # drop is async; wait for the dnode to fully release vgroups
            for _ in range(60):
                try:
                    tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
                    break
                except Exception as e:
                    if 'dropping' in str(e):
                        time.sleep(1)
                        continue
                    raise
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

    # ------------------------------------------------------------------
    # IN-list subquery (REMOTE_VALUE_LIST)
    # ------------------------------------------------------------------

    def test_in_list_subquery(self):
        """REMOTE_VALUE_LIST must be refreshed per stream event.

        Bug: in stream mode, the LIST cache check in sclInitParam()
        short-circuited once VALUELIST_FLAG_VAL_UNSET was cleared on
        the first event. Every subsequent trigger event reused the same
        pHashFilter, so the IN-list never reflected later changes to
        the source table.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-14 Created to pin LIST cache invalidation
        """
        streams = [self.InListPerEvent()]
        tdStream.checkAll(streams)

    class InListPerEvent(StreamCheckItem):
        def __init__(self):
            self.db = "test_subq_inlist"

        def create(self):
            tdLog.info(f"=== create db {self.db} ===")
            tdSql.execute(f"drop database if exists {self.db}")
            # drop is async; wait for the dnode to fully release vgroups
            for _ in range(60):
                try:
                    tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
                    break
                except Exception as e:
                    if 'dropping' in str(e):
                        time.sleep(1)
                        continue
                    raise
            tdSql.execute(f"use {self.db}")

            tdSql.execute("create table linea     (ts timestamp, p int)")
            tdSql.execute("create table data      (ts timestamp, f1 int, v int)")
            tdSql.execute("create table whitelist (ts timestamp, id int)")

            tdSql.execute(
                "insert into data values "
                "('2026-05-01 00:00:00', 1, 10),"
                "('2026-05-01 00:00:01', 2, 20),"
                "('2026-05-01 00:00:02', 3, 30)"
            )
            # Seed whitelist so the IN-subquery resolves at CREATE STREAM.
            tdSql.execute(
                "insert into whitelist values ('2026-05-01 00:00:00', 1)"
            )

            time.sleep(10)
            tdLog.info("=== create stream sum_in_whitelist ===")
            tdSql.execute(
                f"create stream sum_in_whitelist count_window(1, 1, p) "
                f"from linea "
                f"into r as "
                f"select _twstart as ts, sum(v) as total "
                f"from data "
                f"where f1 in (select id from whitelist)"
            )

        def insert1(self):
            tdLog.info("=== event 1: whitelist={1} -> match f1=1 -> SUM=10 ===")
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:00', 1)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f"select total from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, 10),
            )

        def insert2(self):
            tdLog.info("=== add id=2 to whitelist, trigger event 2 ===")
            tdSql.execute(
                "insert into whitelist values ('2026-05-01 00:00:01', 2)"
            )
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:01', 1)"
            )

        def check2(self):
            # Event 2 must see whitelist={1,2}: SUM=10+20=30.
            # Bug-without-fix would cache {1} and emit 10 again.
            tdSql.checkResultsByFunc(
                sql=f"select total from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, 10)
                and tdSql.compareData(1, 0, 30),
            )

        def insert3(self):
            tdLog.info("=== whitelist -> {2,3}, trigger event 3 ===")
            tdSql.execute(
                "delete from whitelist where ts = '2026-05-01 00:00:00'"
            )
            tdSql.execute(
                "insert into whitelist values ('2026-05-01 00:00:02', 3)"
            )
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:02', 1)"
            )

        def check3(self):
            # Event 3: whitelist={2,3}, SUM=20+30=50.
            tdSql.checkResultsByFunc(
                sql=f"select total from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, 10)
                and tdSql.compareData(1, 0, 30)
                and tdSql.compareData(2, 0, 50),
            )

    # ------------------------------------------------------------------
    # Row-comparison subquery (REMOTE_ROW)
    # ------------------------------------------------------------------

    def test_row_subquery(self):
        """REMOTE_ROW must be refreshed per stream event.

        `> ANY (subquery)` is rewritten by the planner to `> MIN(...)`,
        which materialises into a REMOTE_ROW node. In stream mode the
        ROW cache check in sclInitParam() short-circuited once
        pRemote->valSet was set on the first event, so the threshold
        was frozen forever.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-14 Created to pin ROW cache invalidation
        """
        streams = [self.RowPerEvent()]
        tdStream.checkAll(streams)

    class RowPerEvent(StreamCheckItem):
        def __init__(self):
            self.db = "test_subq_row"

        def create(self):
            tdLog.info(f"=== create db {self.db} ===")
            tdSql.execute(f"drop database if exists {self.db}")
            # drop is async; wait for the dnode to fully release vgroups
            for _ in range(60):
                try:
                    tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
                    break
                except Exception as e:
                    if 'dropping' in str(e):
                        time.sleep(1)
                        continue
                    raise
            tdSql.execute(f"use {self.db}")

            tdSql.execute("create table linea     (ts timestamp, p int)")
            tdSql.execute("create table data      (ts timestamp, v int)")
            tdSql.execute("create table threshold (ts timestamp, t int)")

            tdSql.execute(
                "insert into data values "
                "('2026-05-01 00:00:00', 10),"
                "('2026-05-01 00:00:01', 20),"
                "('2026-05-01 00:00:02', 30),"
                "('2026-05-01 00:00:03', 40)"
            )
            # Seed threshold so the row-subquery resolves at CREATE STREAM.
            tdSql.execute(
                "insert into threshold values ('2026-05-01 00:00:00', 35)"
            )

            time.sleep(10)
            tdLog.info("=== create stream sum_gt_any_threshold ===")
            tdSql.execute(
                f"create stream sum_gt_any_threshold count_window(1, 1, p) "
                f"from linea "
                f"into r as "
                f"select _twstart as ts, sum(v) as total "
                f"from data "
                f"where v > any (select t from threshold)"
            )

        def insert1(self):
            tdLog.info("=== event 1: threshold={35} -> v>35 -> SUM=40 ===")
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:00', 1)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f"select total from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, 40),
            )

        def insert2(self):
            tdLog.info("=== add t=15 (new min), trigger event 2 ===")
            tdSql.execute(
                "insert into threshold values ('2026-05-01 00:00:01', 15)"
            )
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:01', 1)"
            )

        def check2(self):
            # Event 2 must see new min 15: rows v in {20,30,40}, SUM=90.
            # Bug-without-fix would cache 35 and emit 40 again.
            tdSql.checkResultsByFunc(
                sql=f"select total from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, 40)
                and tdSql.compareData(1, 0, 90),
            )

        def insert3(self):
            tdLog.info("=== threshold -> {5}, trigger event 3 ===")
            tdSql.execute("delete from threshold")
            tdSql.execute(
                "insert into threshold values ('2026-05-01 00:00:02', 5)"
            )
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:02', 1)"
            )

        def check3(self):
            # Event 3: threshold={5}, all 4 rows match, SUM=100.
            tdSql.checkResultsByFunc(
                sql=f"select total from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, 40)
                and tdSql.compareData(1, 0, 90)
                and tdSql.compareData(2, 0, 100),
            )
