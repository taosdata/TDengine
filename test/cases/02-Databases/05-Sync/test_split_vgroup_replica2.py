import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestSplitVgroupReplica2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_split_vgroup_replica2(self):
        """Split: replica-2

        1. Start a 4-node cluster with dnode1 configured as supportVnodes=0
        2. Create database d1 (1 vgroup, 2 replicas) and insert data
        3. Execute SPLIT VGROUP to split the vnode

        Catalog:
            - Database:Sync

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-4 Dongming Chen init

        """

        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        tdSql.execute(f"create user u1 pass 'taosdata'")

        tdLog.info(f"=============== step1 check dnodes")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(4)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 1 replica 2")

        tdLog.info(f"=============== step3: split")
        tdLog.info(f"split vgroup 2")
        tdSql.execute("split vgroup 2")
