import time

import pytest
from new_test_framework.utils import clusterComCheck, tdLog, tdSql, tdStream


class TestStreamVgroupTopologyChange:
    NODE_COUNT = 4
    TABLE_COUNT = 3
    ROWS_PER_WINDOW_PER_TABLE = 2
    WINDOWS_BEFORE_CHANGE = 4
    WINDOWS_AFTER_CHANGE = 5
    BASE_TS = 1700000000000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def prepare_stream(self, db):
        tdLog.info(f"prepare stream database {db}")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} replica 3 vgroups 1 "
            "wal_retention_period 3600"
        )
        clusterComCheck.checkDbReady(db)
        tdSql.execute(f"use {db}")
        tdSql.execute("create stable meters (ts timestamp, v int) tags (gid int)")
        for table_idx in range(self.TABLE_COUNT):
            tdSql.execute(
                f"create table ct{table_idx} using meters tags ({table_idx})"
            )

        tdSql.execute(
            f"create stream {db}.s_topology interval(1s) sliding(1s) "
            f"from {db}.meters stream_options(ignore_disorder) "
            f"into {db}.stream_out "
            "as select _twstart ts, count(*) cnt, sum(v) sum_v from %%trows"
        )
        tdStream.checkStreamStatus("s_topology")

    def prepare_virtual_stream(self, db):
        tdLog.info(f"prepare virtual table stream database {db}")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} replica 3 vgroups 1 "
            "wal_retention_period 3600"
        )
        clusterComCheck.checkDbReady(db)
        tdSql.execute(f"use {db}")
        tdSql.execute("create stable meters (ts timestamp, v int) tags (gid int)")
        for table_idx in range(self.TABLE_COUNT):
            tdSql.execute(
                f"create table ct{table_idx} using meters tags ({table_idx})"
            )

        tdSql.execute(
            "create stable vmeter (ts timestamp, v int) tags (gid int) virtual 1"
        )
        for table_idx in range(self.TABLE_COUNT):
            tdSql.execute(
                f"create vtable vct{table_idx} (v from {db}.ct{table_idx}.v) "
                f"using {db}.vmeter tags ({table_idx})"
            )

        tdSql.execute(
            f"create stream {db}.s_vtable_topology interval(1s) sliding(1s) "
            f"from {db}.vmeter stream_options(ignore_disorder) "
            f"into {db}.stream_vtable_out "
            "as select _twstart ts, count(*) cnt, sum(v) sum_v from %%trows"
        )
        tdStream.checkStreamStatus("s_vtable_topology")

    def insert_windows(self, db, start_window, window_count):
        for window_idx in range(start_window, start_window + window_count):
            for table_idx in range(self.TABLE_COUNT):
                for row_idx in range(self.ROWS_PER_WINDOW_PER_TABLE):
                    ts = (
                        self.BASE_TS
                        + window_idx * 1000
                        + table_idx * self.ROWS_PER_WINDOW_PER_TABLE
                        + row_idx
                    )
                    value = self.value_for(window_idx, table_idx, row_idx)
                    tdSql.execute(
                        f"insert into {db}.ct{table_idx} values ({ts}, {value})"
                    )
            tdLog.info(
                f"inserted stream source window {window_idx} in timestamp order"
            )
            time.sleep(0.2)

    def value_for(self, window_idx, table_idx, row_idx):
        return window_idx * 100 + table_idx * 10 + row_idx

    def expected_rows(self):
        expected = []
        closed_windows = self.WINDOWS_BEFORE_CHANGE + self.WINDOWS_AFTER_CHANGE
        for window_idx in range(closed_windows):
            cnt = self.TABLE_COUNT * self.ROWS_PER_WINDOW_PER_TABLE
            sum_v = 0
            for table_idx in range(self.TABLE_COUNT):
                for row_idx in range(self.ROWS_PER_WINDOW_PER_TABLE):
                    sum_v += self.value_for(window_idx, table_idx, row_idx)
            expected.append((self.BASE_TS + window_idx * 1000, cnt, sum_v))
        return expected

    def wait_stream_running(self, db, stream_name="s_topology"):
        tdSql.checkResultsByFunc(
            sql=(
                "select status from information_schema.ins_streams "
                f"where db_name='{db}' and stream_name='{stream_name}'"
            ),
            func=lambda: tdSql.getRows() == 1
            and tdSql.getColData(0) == ["Running"],
            retry=90,
        )

    def wait_transactions_done(self):
        clusterComCheck.checkTransactions()

    def wait_stream_result(self, db, output_table="stream_out"):
        expected = self.expected_rows()

        def result_matches():
            if tdSql.getRows() != len(expected):
                return False
            for row_idx, row in enumerate(expected):
                for col_idx, value in enumerate(row):
                    if not tdSql.compareData(row_idx, col_idx, value):
                        return False
            return True

        tdSql.checkResultsByFunc(
            sql=(
                f"select cast(ts as bigint), cnt, sum_v from {db}.{output_table} "
                "order by ts"
            ),
            func=result_matches,
            retry=120,
        )

    def get_vgroup_ids(self, db):
        tdSql.query(
            "select vgroup_id from information_schema.ins_vgroups "
            f"where db_name='{db}' order by vgroup_id"
        )
        return [row[0] for row in tdSql.queryResult]

    def get_vgroup_leader_dnode(self, db, vgroup_id, timeout=30):
        for attempt in range(timeout):
            tdSql.query(f"show {db}.vgroups")
            for row in tdSql.queryResult:
                if row[0] != vgroup_id:
                    continue
                for idx, value in enumerate(row):
                    if value == "leader":
                        return row[idx - 1]
            if attempt < timeout - 1:
                time.sleep(1)
        return None

    def get_cluster_dnode_ids(self):
        tdSql.query(
            "select id from information_schema.ins_dnodes "
            "where status='ready' order by id"
        )
        return [row[0] for row in tdSql.queryResult]

    def get_vgroup_dnode_ids(self, db, vgroup_id):
        tdSql.query(
            "select dnode_id from information_schema.ins_vnodes "
            f"where db_name='{db}' and vgroup_id={vgroup_id} "
            "order by dnode_id"
        )
        return [row[0] for row in tdSql.queryResult]

    def choose_redistribute_dnodes(self, db, vgroup_id):
        cluster_dnodes = self.get_cluster_dnode_ids()
        current_dnodes = self.get_vgroup_dnode_ids(db, vgroup_id)
        new_dnodes = list(current_dnodes)
        for dnode_id in cluster_dnodes:
            if dnode_id not in new_dnodes:
                new_dnodes[-1] = dnode_id
                break
        if len(new_dnodes) != 3:
            tdLog.exit(f"expected replica-3 vgroup, got dnodes={new_dnodes}")
        return sorted(new_dnodes)

    def run_stream_with_change(
        self,
        db,
        change_func,
        prepare_func=None,
        stream_name="s_topology",
        output_table="stream_out",
    ):
        (prepare_func or self.prepare_stream)(db)
        self.insert_windows(db, 0, self.WINDOWS_BEFORE_CHANGE)
        self.wait_stream_running(db, stream_name)
        change_func(db)
        self.wait_transactions_done()
        self.wait_stream_running(db, stream_name)
        self.insert_windows(
            db, self.WINDOWS_BEFORE_CHANGE, self.WINDOWS_AFTER_CHANGE
        )
        close_window = self.WINDOWS_BEFORE_CHANGE + self.WINDOWS_AFTER_CHANGE
        self.insert_windows(db, close_window, 1)
        self.wait_stream_result(db, output_table)

    def balance_one_vgroup_leader(self, db):
        vgroup_id = self.get_vgroup_ids(db)[0]
        old_leader = self.get_vgroup_leader_dnode(db, vgroup_id)
        if old_leader is None:
            tdLog.exit(
                f"failed to get vgroup leader before balancing, db={db}, vgId={vgroup_id}"
            )

        tdLog.info(
            f"balance stream source vgroup leader, db={db}, vgId={vgroup_id}, "
            f"oldLeader={old_leader}"
        )
        new_leader = None
        max_attempts = 300
        for attempt in range(max_attempts):
            tdSql.execute(f"balance vgroup leader on {vgroup_id}")
            new_leader = self.get_vgroup_leader_dnode(db, vgroup_id)
            if new_leader is not None and new_leader != old_leader:
                tdLog.info(
                    f"vgroup leader changed, db={db}, vgId={vgroup_id}, "
                    f"oldLeader={old_leader}, newLeader={new_leader}"
                )
                break
            if attempt % 5 == 0:
                tdLog.info(
                    f"waiting vgroup leader change, db={db}, vgId={vgroup_id}, "
                    f"oldLeader={old_leader}, currentLeader={new_leader}, "
                    f"elapsed={attempt}s"
                )
            time.sleep(1)
        else:
            tdLog.exit(
                f"failed to change vgroup leader after {max_attempts} attempts, "
                f"db={db}, vgId={vgroup_id}, oldLeader={old_leader}, "
                f"currentLeader={new_leader}"
            )

        clusterComCheck.checkDbReady(db)

    def redistribute_one_stream_vgroup(self, db):
        vgroup_id = self.get_vgroup_ids(db)[0]
        before_dnodes = self.get_vgroup_dnode_ids(db, vgroup_id)
        dnodes = self.choose_redistribute_dnodes(db, vgroup_id)
        dnode_clause = " ".join(f"dnode {dnode_id}" for dnode_id in dnodes)
        tdLog.info(
            f"redistribute stream source vgroup, db={db}, vgId={vgroup_id}, "
            f"beforeDnodes={before_dnodes}, targets={dnodes}"
        )
        tdSql.execute(f"redistribute vgroup {vgroup_id} {dnode_clause}")
        clusterComCheck.checkDbReady(db)
        after_dnodes = self.get_vgroup_dnode_ids(db, vgroup_id)
        tdLog.info(
            f"redistribute stream source vgroup completed, db={db}, "
            f"vgId={vgroup_id}, afterDnodes={after_dnodes}"
        )

    def split_one_stream_vgroup(self, db):
        vgroup_id = self.get_vgroup_ids(db)[0]
        tdLog.info(f"split stream source vgroup, db={db}, vgId={vgroup_id}")
        before_vgroups = self.get_vgroup_ids(db)
        tdSql.error(
            f"split vgroup {vgroup_id}",
            expectErrInfo="Db used by stream",
            fullMatched=False,
        )
        after_vgroups = self.get_vgroup_ids(db)
        if after_vgroups != before_vgroups:
            tdLog.exit(
                f"split vgroup changed topology despite stream protection, "
                f"db={db}, before={before_vgroups}, after={after_vgroups}"
            )
        tdLog.info(
            f"split vgroup rejected while stream uses database, db={db}, "
            f"vgId={vgroup_id}"
        )

    # --- impl ---
    def do_prepare_cluster(self):
        clusterComCheck.checkDnodes(self.NODE_COUNT)
        tdStream.ensureSnode(1)
        tdSql.checkResultsByFunc(
            sql="show snodes",
            func=lambda: tdSql.getRows() >= 1,
            retry=60,
        )
        print("prepare stream topology cluster ............... [ passed ]")

    def do_vnode_leader_change_during_stream_calc(self):
        self.run_stream_with_change(
            "st_vnode_leader_change", self.balance_one_vgroup_leader
        )
        print("vnode leader change during stream calc ........ [ passed ]")

    def do_vnode_leader_change_during_vtable_stream_calc(self):
        self.run_stream_with_change(
            "st_vtable_vnode_leader_change",
            self.balance_one_vgroup_leader,
            self.prepare_virtual_stream,
            "s_vtable_topology",
            "stream_vtable_out",
        )
        print("vnode leader change during vtable stream calc . [ passed ]")

    def do_redistribute_vgroup_during_stream_calc(self):
        self.run_stream_with_change(
            "st_redistribute_vgroup", self.redistribute_one_stream_vgroup
        )
        print("redistribute vgroup during stream calc ........ [ passed ]")

    def do_split_vgroup_during_stream_calc(self):
        self.run_stream_with_change("st_split_vgroup", self.split_one_stream_vgroup)
        print("split vgroup during stream calc ............... [ passed ]")

    # --- main ---
    @pytest.mark.cluster
    def test_stream_vgroup_topology_change(self):
        """Stream results stay correct across vgroup topology changes.

        1. Trigger stream calculation, then switch a source vnode leader.
        2. Trigger a virtual-table stream calculation, then switch its source
           vnode leader.
        3. Trigger stream calculation, then redistribute a source vgroup.
        4. Trigger stream calculation, then split a source vgroup.
        5. Verify final stream output equals the source-table aggregate.

        Catalog:
            - Streams:Snode

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-20 Wang Mingming Created
            - 2026-09-22 Wang Mingming Added virtual-table stream leader-change coverage

        """
        self.do_prepare_cluster()
        self.do_vnode_leader_change_during_stream_calc()
        self.do_vnode_leader_change_during_vtable_stream_calc()
        self.do_redistribute_vgroup_during_stream_calc()
        self.do_split_vgroup_during_stream_calc()
