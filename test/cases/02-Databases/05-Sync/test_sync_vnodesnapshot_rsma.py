import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestSyncVnodeSnapshotRsma:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sync_vnode_snapshot_rsma(self):
        """Query: replica-3 rsma

        1. Start a 4-node cluster with dnode1 supportVnodes=0
        2. Create a 3-replica database with 1 vgroup
        3. Create one RSMA-enabled super table and one child table
        4. Stop dnode4
        5. Insert data and flush database
        6. Repeat steps 4-5 twice
        7. Restart all dnodes
        8. Query and verify results

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/sync/vnodesnapshot-rsma-test.sim

        """

        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)
        clusterComCheck.checkDnodes(4)

        replica = 3
        vgroups = 1
        retentions = "-:7d,15s:21d,1m:365d"

        tdLog.info(f"============= create database")
        tdSql.execute(
            f"create database db replica {replica} vgroups {vgroups} retentions {retentions}"
        )
        clusterComCheck.checkDbReady("db")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(3)
        tdSql.checkData(2, 15, "ready")

        tdSql.execute(f"use db")

        tdSql.query(f"show vgroups")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(vgroups)

        tdLog.info(f"====>  create stable/child table")
        tdSql.execute(
            f"create table stb (ts timestamp, c1 float, c2 float, c3 double) tags (t1 int) rollup(sum) watermark 3s,3s max_delay 3s,3s"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table ct1 using stb tags(1000)")

        tdLog.info(f"===> stop dnode4")
        sc.dnodeStop(4)
        clusterComCheck.checkDnodes(3)
        time.sleep(3)

        tdLog.info(f"===> write 0-50 records")
        ms = 0
        cnt = 0
        while cnt < 50:
            ms = str(cnt) + "m"
            tdSql.execute(f"insert into ct1 values (now + {ms} , {cnt} , 2.1, 3.1)")
            cnt = cnt + 1
        tdLog.info(f"===> flush database db")
        tdSql.execute(f"flush database db;")

        time.sleep(5)

        tdLog.info(f"===> write 51-100 records")
        while cnt < 100:
            ms = str(cnt) + "m"
            tdSql.execute(f"insert into ct1 values (now + {ms} , {cnt} , 2.1, 3.1)")
            cnt = cnt + 1

        tdLog.info(f"===> flush database db")
        tdSql.execute(f"flush database db;")
        time.sleep(5)

        tdLog.info(f"===> stop dnode1 dnode2 dnode3")
        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStop(3)

        ########################################################
        tdLog.info(f"===> start dnode1 dnode2 dnode3 dnode4")
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        sc.dnodeStart(4)
        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkDbReady("db")

        time.sleep(3)

        tdLog.info(f"=============== query data of level 1")
        tdSql.execute(f"use db")

        tdSql.query(f"select * from ct1 where ts > now - 1d")
        tdLog.info(f"rows of level 1: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkRows(100)

        tdLog.info(f"=============== sleep 5s to wait the result")
        time.sleep(5)

        tdLog.info(f"=============== query data of level 2")
        tdSql.query(f"select * from ct1 where ts > now - 10d")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}")
        tdLog.info(tdSql.getRows())
        tdSql.checkRows(100)

        tdLog.info(f"=============== query data of level 3")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}")
        tdLog.info(tdSql.getRows())
        # tdSql.checkRows(100)
