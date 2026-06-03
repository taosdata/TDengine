import time

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


def _drop_and_create_db(db, create_sql=None):
    """Drop database and recreate with retry for async 'dropping' state.

    Args:
        db: database name
        create_sql: full CREATE DATABASE statement; if None uses default
    """
    if create_sql is None:
        create_sql = f"create database {db} vgroups 1 buffer 8"
    tdSql.execute(f"drop database if exists {db}")
    for _ in range(60):
        try:
            tdSql.execute(create_sql)
            break
        except Exception as e:
            if 'dropping' in str(e).lower():
                time.sleep(1)
                continue
            raise
    tdSql.execute(f"use {db}")


class TestStreamSubqueryPerEvent:
    """Per-event re-evaluation of stream WHERE subqueries (customer ticket).

    The customer reproducer originally relied on a scalar subquery in the
    stream body to provide a per-event lower bound on a secondary source:

        WHERE ts >= (SELECT last_row(ts) FROM inicio_descarga)

    In stream mode every trigger event MUST refetch the subquery; the
    older code cached the result on the first event and silently
    replayed it forever.  This file pins the per-event semantics for the
    original reproducer, the workaround control path, and all four
    remote-subquery flavours that flow through sclInitParam:

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

      5. test_exists_subquery (REMOTE_ZERO_ROWS)
         EXISTS (subquery) re-evaluation.  Pins the ZERO_ROWS cache
         invalidation: setZeroRowsResValue rewrites node->type to
         QUERY_NODE_VALUE, and stream mode must restore it to
         QUERY_NODE_REMOTE_ZERO_ROWS so later events refetch.
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
            _drop_and_create_db(self.db)

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
            self._rows_after_e3 = 3

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
            # Validate the event-4 suppression against the known-good
            # row count captured after event 3, rather than taking a new
            # post-event-4 baseline.  This avoids missing a stale row
            # that is already present when we look, and also catches a
            # delayed event-4 row before event 5 is driven.
            sql = (
                f"select acumulado_cumple, acumulado_total "
                f"from {self.db}.resultado_descarga order by ts"
            )
            expected_rows = self._rows_after_e3
            deadline = time.time() + 5
            while time.time() < deadline:
                tdSql.query(sql)
                rows = tdSql.getRows()
                if rows != expected_rows:
                    raise AssertionError(
                        f"event 4 unexpectedly produced output before event 5: "
                        f"expected {expected_rows} rows, got {rows}"
                    )
                time.sleep(0.1)
            self._rows_pre_e5 = expected_rows
            tdLog.info(
                f"=== check4 verified no event-4 output: rows={self._rows_pre_e5} "
                f"(stale-(1,1) check remains deferred to check5) ==="
            )

        def insert5(self):
            # Re-populate inicio_descarga and trigger again.  This checks
            # the NULL-to-non-NULL transition after event 4's empty fetch:
            # setValueFromResBlock must reset pRes->isNull = false so
            # event 5's newly fetched value is not masked by the
            # isNull=true left over from event 4.  With the bug, event 5's
            # WHERE evaluates against a NULL lower bound and matches no
            # cumple rows -> aggregate NULL.  With the fix, the lower
            # bound is 00:00:01 again and exactly two cumple rows match
            # -> SUM=(2, 2).
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

        def check5(self):
            sql = (
                f"select acumulado_cumple, acumulado_total "
                f"from {self.db}.resultado_descarga order by ts"
            )
            rows_pre_e5 = self._rows_pre_e5
            # Wait for event 5's (2,2) row to arrive; this also flushes
            # any pending event-4 output, so we can scan the new tail
            # for the stale (1,1) marker.
            tdSql.checkResultsByFunc(
                sql=sql,
                func=lambda: tdSql.getRows() > rows_pre_e5
                and tdSql.getData(tdSql.getRows() - 1, 0) == 2
                and tdSql.getData(tdSql.getRows() - 1, 1) == 2,
            )
            tdSql.query(sql)
            rows_after_e5 = tdSql.getRows()
            assert rows_after_e5 > rows_pre_e5, (
                f"event 5 produced no new row (was {rows_pre_e5}, "
                f"now {rows_after_e5}); stream stalled after empty event"
            )
            # Verify event 4 did not silently reuse event 3's value (1,1).
            # Any row appended between rows_pre_e5 and the final event-5
            # row (last_row) belongs to event 4.
            last_row = rows_after_e5 - 1
            for i in range(rows_pre_e5, last_row):
                v0 = tdSql.getData(i, 0)
                v1 = tdSql.getData(i, 1)
                assert not (v0 == 1 and v1 == 1), (
                    f"event 4 reused event 3's stale subquery value "
                    f"(1,1) at row {i}; qFetchRemoteNode stream branch "
                    f"is not clearing the subResNodes slot before refetch"
                )
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
            _drop_and_create_db(self.db)

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

        def _wait_seed_visible(self, count_sql, label, timeout=15):
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    tdSql.query(count_sql)
                    if tdSql.getData(0, 0) == 1:
                        return
                except Exception:
                    pass
                time.sleep(0.5)
            raise RuntimeError(
                f"{label} seed row did not become queryable before CREATE STREAM"
            )

        def create(self):
            tdLog.info(f"=== create db {self.db} ===")
            _drop_and_create_db(self.db)

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

            self._wait_seed_visible(
                "select count(*) from whitelist "
                "where ts = '2026-05-01 00:00:00' and id = 1",
                "whitelist",
            )
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
            tdLog.info("=== whitelist -> {}, trigger event 3 ===")
            tdSql.execute(
                "delete from whitelist where ts = '2026-05-01 00:00:00'"
            )
            tdSql.execute(
                "delete from whitelist where ts = '2026-05-01 00:00:01'"
            )
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:02', 1)"
            )

        def check3(self):
            # Event 3 empties the whitelist so no rows match IN and the
            # stream body emits no output.  Poll for 15 seconds to both
            # allow the stream time to process event 3 and verify that no
            # stale IN-list row appears.  The 15-second window ensures
            # event 3 is evaluated while the whitelist is still empty,
            # before insert4 re-populates it.
            deadline = time.monotonic() + 15.0
            while True:
                tdSql.query(f"select total from {self.db}.r order by ts")
                assert tdSql.getRows() == 2, (
                    f"event 3 stale IN-list: got {tdSql.getRows()} rows, "
                    f"expected 2 (cached IN-list was not invalidated)"
                )
                if time.monotonic() >= deadline:
                    break
                time.sleep(0.5)

        def insert4(self):
            # Re-add id=1 to whitelist and trigger event 4.  Whitelist is
            # now {1} so the correct SUM is 10 (only f1=1, v=10 matches).
            # A stale IN-list from event 3 would have used {1,2} and
            # produced SUM=30, which is distinct from 10 and raises the
            # row count above rows_after_e2+1, catching the regression.
            tdLog.info("=== add id=1 to whitelist, trigger event 4 ===")
            tdSql.execute(
                "insert into whitelist values ('2026-05-01 00:00:03', 1)"
            )
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:03', 1)"
            )

        def check4(self):
            # Event 4: whitelist={1}, so only f1=1 (v=10) matches -> SUM=10.
            # check3 already verified event 3 produced no stale row, so we
            # just wait for the correct event-4 output.
            tdSql.checkResultsByFunc(
                sql=f"select total from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, 10)
                and tdSql.compareData(1, 0, 30)
                and tdSql.compareData(2, 0, 10),
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

        def _wait_seed_visible(self, count_sql, label, timeout=15):
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    tdSql.query(count_sql)
                    if tdSql.getData(0, 0) == 1:
                        return
                except Exception:
                    pass
                time.sleep(0.5)
            raise RuntimeError(
                f"{label} seed row did not become queryable before CREATE STREAM"
            )

        def create(self):
            tdLog.info(f"=== create db {self.db} ===")
            _drop_and_create_db(self.db)

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

            self._wait_seed_visible(
                "select count(*) from threshold "
                "where ts = '2026-05-01 00:00:00' and t = 35",
                "threshold",
            )
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

    def test_exists_subquery(self):
        """REMOTE_ZERO_ROWS (EXISTS) must be refreshed per stream event.

        `EXISTS (subquery)` is rewritten by the planner to a
        REMOTE_ZERO_ROWS node holding a 0/1 row count. handleRemoteZeroRowsRes
        forces the AST node type to QUERY_NODE_VALUE after fetching, so the
        scalar walker stops re-dispatching the case and replays the cached
        row count for every later event. The fix restores the
        QUERY_NODE_REMOTE_ZERO_ROWS type in stream mode so the next trigger
        re-fetches.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-14 Created to pin REMOTE_ZERO_ROWS per-event refetch
        """
        streams = [self.ExistsPerEvent()]
        tdStream.checkAll(streams)

    class ExistsPerEvent(StreamCheckItem):
        def __init__(self):
            self.db = "test_subq_exists"

        def _wait_seed_visible(self, count_sql, label, timeout=15):
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    tdSql.query(count_sql)
                    if tdSql.getData(0, 0) >= 1:
                        return
                except Exception:
                    pass
                time.sleep(0.5)
            raise RuntimeError(
                f"{label} seed row did not become queryable before CREATE STREAM"
            )

        def create(self):
            tdLog.info(f"=== create db {self.db} ===")
            _drop_and_create_db(self.db)

            tdSql.execute("create table linea  (ts timestamp, p int)")
            tdSql.execute("create table data   (ts timestamp, v int)")
            tdSql.execute("create table gate   (ts timestamp, on_off int)")

            tdSql.execute(
                "insert into data values "
                "('2026-05-01 00:00:00', 10),"
                "('2026-05-01 00:00:01', 20),"
                "('2026-05-01 00:00:02', 30)"
            )
            # Seed gate so EXISTS resolves to TRUE at CREATE STREAM.
            tdSql.execute(
                "insert into gate values ('2026-05-01 00:00:00', 1)"
            )
            self._wait_seed_visible(
                "select count(*) from gate", "gate",
            )

            tdLog.info("=== create stream sum_when_gate_open ===")
            # Use EXISTS inside the projection so every trigger event emits a
            # row regardless of gate state. flag is 1 when gate has rows,
            # 0 otherwise.  Without the REMOTE_ZERO_ROWS type-restore fix,
            # the AST node is rewritten to QUERY_NODE_VALUE on event 1 and
            # the walker stops re-dispatching the case, so event 2 keeps
            # replaying flag=1 even after the gate is emptied.
            tdSql.execute(
                f"create stream sum_when_gate_open count_window(1, 1, p) "
                f"from linea "
                f"into r as "
                f"select _twstart as ts, "
                f"max(case when exists (select * from gate) "
                f"then 1 else 0 end) as flag "
                f"from data"
            )

        def insert1(self):
            tdLog.info("=== event 1: gate non-empty -> flag=1 ===")
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:00', 1)"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f"select flag from {self.db}.r order by ts",
                func=lambda: tdSql.getRows() == 1
                and tdSql.compareData(0, 0, 1),
            )

        def insert2(self):
            tdLog.info("=== event 2: empty gate -> flag must be 0 ===")
            tdSql.execute("delete from gate")
            tdSql.execute(
                "insert into linea values ('2026-05-01 00:00:01', 1)"
            )

        def check2(self):
            sql = f"select flag from {self.db}.r order by ts"
            deadline = time.time() + 60
            while time.time() < deadline:
                tdSql.query(sql)
                if tdSql.getRows() >= 2:
                    break
                time.sleep(2)
            tdSql.query(sql)
            rows = tdSql.getRows()
            for i in range(rows):
                tdLog.info(f"check2 row{i} flag={tdSql.getData(i, 0)!r}")
            assert rows == 2, (
                f"event 2 produced no new row (rows={rows}); stream stalled "
                f"or output was suppressed"
            )
            v = tdSql.getData(1, 0)
            assert v == 0, (
                f"event 2 emitted flag={v} (expected 0); REMOTE_ZERO_ROWS "
                f"node replayed event 1's cached row count instead of "
                f"refetching after the gate was emptied"
            )
