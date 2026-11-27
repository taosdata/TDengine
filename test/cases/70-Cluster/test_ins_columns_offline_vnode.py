import time

import pytest

from new_test_framework.utils import clusterComCheck, sc, tdLog, tdSql


class TestInsColumnsOfflineVnode:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- impl ---
    def do_query_with_offline_vnode(self):
        clusterComCheck.checkDnodes(3)
        tdSql.execute("create database ins_cols_replica3 vgroups 3 replica 3")
        tdSql.execute("create database ins_cols_replica1 vgroups 6 replica 1")
        clusterComCheck.checkDbReady("ins_cols_replica3")
        clusterComCheck.checkDbReady("ins_cols_replica1")
        tdSql.execute(
            "create table ins_cols_replica3.t0 (ts timestamp, c int)"
        )

        for i in range(18):
            tdSql.execute(
                f"create table ins_cols_replica1.t{i} (ts timestamp, c int)"
            )

        tdSql.query("show ins_cols_replica1.vgroups")
        if not any(tdSql.getData(i, 3) == 3 for i in range(tdSql.queryRows)):
            tdLog.exit("dnode 3 does not own a replica-1 vgroup")

        tdSql.query("select count(*) from information_schema.ins_columns")

        try:
            sc.dnodeStop(3)
            clusterComCheck.checkDnodes(2)
            tdSql.execute("use ins_cols_replica3")

            start = time.monotonic()
            tdSql.query(
                "select * from information_schema.ins_columns "
                "where db_name = 'ins_cols_replica3'"
            )
            literal_rows = tdSql.queryRows
            literal_elapsed = time.monotonic() - start

            start = time.monotonic()
            tdSql.query(
                "select * from information_schema.ins_columns "
                "where db_name = database()"
            )
            database_func_rows = tdSql.queryRows
            database_func_elapsed = time.monotonic() - start
        finally:
            sc.dnodeStart(3)
            clusterComCheck.checkDnodes(3)

        tdLog.info(
            f"INS_COLUMNS elapsed: literal={literal_elapsed:.3f}s, "
            f"database()={database_func_elapsed:.3f}s"
        )
        if literal_rows != database_func_rows:
            tdLog.exit(
                f"INS_COLUMNS row count differs: literal={literal_rows}, "
                f"database()={database_func_rows}"
            )
        if literal_elapsed >= 30:
            tdLog.exit(
                f"literal INS_COLUMNS query took {literal_elapsed:.3f}s"
            )
        if database_func_elapsed >= 30:
            tdLog.exit(
                f"database() INS_COLUMNS query took "
                f"{database_func_elapsed:.3f}s"
            )

        print("INS_COLUMNS offline vnode retry ................ [ passed ]")

    # --- main ---
    @pytest.mark.cluster
    def test_ins_columns_offline_vnode(self):
        """INS_COLUMNS database() pruning with an offline vnode

        1. Create replica-3 and replica-1 databases on three dnodes
        2. Stop a dnode that owns an unrelated replica-1 vgroup
        3. Compare literal db_name filtering with db_name=database()
        4. Verify both queries avoid the unrelated offline vnode

        Catalog:
            - Query:InformationSchema

        Since: v3.3.6.55

        Labels: common,ci,cluster

        Jira: None

        History:
            - 2026-09-23 Jing Sima Added offline vnode retry regression coverage
            - 2026-09-24 Jing Sima Added database() vgroup pruning coverage

        """
        self.do_query_with_offline_vnode()
