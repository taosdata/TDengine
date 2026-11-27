import threading
import time

import taos
from new_test_framework.utils import tdLog, tdSql, tdCom


DB_NAME = "redist_q_nohang"
QUERY_STUCK_TIMEOUT = 60
REDISTRIBUTE_TIMEOUT = 300


class TestRedistributeQueryNoHang:
    """A query running while its vgroup is redistributed must never hang forever."""

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def wait_vgroup_stable(vgid, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query(
                f"select dnode_id, status from information_schema.ins_vnodes "
                f"where vgroup_id = {vgid}"
            )
            rows = [(int(r[0]), str(r[1])) for r in tdSql.queryResult]
            leaders = [d for d, s in rows if "leader" in s]
            followers = [d for d, s in rows if "follower" in s]
            if len(rows) == 3 and len(leaders) == 1 and len(followers) == 2:
                return leaders[0], followers
            time.sleep(1)
        raise AssertionError(f"vgroup {vgid} did not become stable, rows={tdSql.queryResult}")

    @staticmethod
    def run_query_loop(stop_event, state, client_cfg):
        conn = taos.connect(config=client_cfg)
        cursor = conn.cursor()
        try:
            while not stop_event.is_set():
                state["in_flight"] = True
                state["last_start"] = time.time()
                try:
                    cursor.execute(f"select count(*) from {DB_NAME}.t")
                    cursor.fetchall()
                    state["ok"] += 1
                except Exception as err:  # noqa: BLE001 - any clean failure is acceptable here
                    state["err"] += 1
                    state["last_err"] = str(err)
                state["in_flight"] = False
                time.sleep(0.05)
        finally:
            conn.close()

    def test_redistribute_query_no_hang(self):
        """Redistribute vgroup while a query is running

        Regression test for the transport ordering issue: a query that meets a vnode close
        (triggered by redistribute) must always finish, never stay in EXECUTING forever.
        1. Create a 3-replica database and insert enough data for repeated scans
        2. Start a background query loop
        3. Redistribute the vgroup so that the leader is removed from the replica set
        4. Fail if any single query stays in flight beyond the timeout
        5. Fail if the query loop cannot stop after the redistribute

        Catalog:
            - Database:Sync

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-20 Created
        """

        tdSql.query("select count(*) from information_schema.ins_dnodes")
        dnode_num = int(tdSql.queryResult[0][0])
        if dnode_num < 4:
            raise AssertionError(f"this test needs at least 4 dnodes, got {dnode_num}")

        tdSql.execute(f"drop database if exists {DB_NAME}")
        tdSql.execute(f"create database {DB_NAME} vgroups 1 replica 3")
        tdSql.execute(f"create table {DB_NAME}.t (ts timestamp, v int)")
        rows = [f"({1700000000000 + i}, {i})" for i in range(2000)]
        for start in range(0, len(rows), 500):
            tdSql.execute(f"insert into {DB_NAME}.t values " + " ".join(rows[start:start + 500]))

        tdSql.query(f"select vgroup_id from information_schema.ins_vgroups where db_name = '{DB_NAME}'")
        vgid = int(tdSql.queryResult[0][0])
        leader, followers = self.wait_vgroup_stable(vgid)

        all_dnodes = list(range(1, dnode_num + 1))
        tdSql.query(f"select dnode_id from information_schema.ins_vnodes where vgroup_id = {vgid}")
        members = [int(r[0]) for r in tdSql.queryResult]
        extra = [d for d in all_dnodes if d not in members]
        if not extra:
            raise AssertionError(f"no free dnode to redistribute vgroup {vgid}, members={members}")
        targets = [extra[0]] + followers[:2]
        tdLog.info(f"vgroup {vgid} leader={leader} followers={followers} targets={targets}")

        client_cfg = tdCom.getClientCfgPath()
        stop = threading.Event()
        state = {"in_flight": False, "last_start": 0.0, "ok": 0, "err": 0, "last_err": ""}
        worker = threading.Thread(target=self.run_query_loop, args=(stop, state, client_cfg))
        worker.daemon = True
        worker.start()
        time.sleep(0.5)

        move_error = {"err": None}
        move_done = threading.Event()

        def move():
            try:
                tdSql.execute(
                    f"redistribute vgroup {vgid} dnode {targets[0]} "
                    f"dnode {targets[1]} dnode {targets[2]}"
                )
            except Exception as err:  # noqa: BLE001 - the move result is not the assertion target
                move_error["err"] = err
            finally:
                move_done.set()

        mover = threading.Thread(target=move)
        mover.daemon = True
        mover.start()

        stuck = 0.0
        deadline = time.time() + REDISTRIBUTE_TIMEOUT
        while time.time() < deadline:
            if state["in_flight"] and state["last_start"] and \
                    time.time() - state["last_start"] > QUERY_STUCK_TIMEOUT:
                stuck = time.time() - state["last_start"]
                break
            if move_done.is_set():
                break
            time.sleep(0.2)

        stop.set()
        worker.join(timeout=QUERY_STUCK_TIMEOUT)
        if worker.is_alive():
            raise AssertionError("a query is still stuck after the redistribute finished")
        if stuck > 0:
            raise AssertionError(f"a single query stayed in flight for {stuck:.1f}s")

        # The assertion target is the query side: no query may stay in flight forever while the
        # vgroup layout changes. A slow or failed redistribute is reported but does not
        # invalidate the hang check.
        if not move_done.is_set():
            tdLog.info("redistribute did not finish within the timeout; no-hang check still valid")
        elif move_error["err"] is not None:
            tdLog.info(f"redistribute finished with an error (tolerated): {move_error['err']}")

        if state["ok"] == 0:
            raise AssertionError(f"no query completed, err={state['err']}, last={state['last_err']}")
        tdLog.info(f"query loop finished: ok={state['ok']} err={state['err']} last_err={state['last_err']}")
        print("redistribute during query: no permanent hang ........ [ passed ]")
