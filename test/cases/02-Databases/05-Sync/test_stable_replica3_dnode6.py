import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestStableReplica3Dnode6:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_replica3_dnode6(self):
        """stable replica3 dnode6

        1. -

        Catalog:
            - DataBase:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/vnode/stable_replica3_dnode6.sim

        """

        tdLog.info(f"========== step1")
        clusterComCheck.checkDnodes(6)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 2 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 3 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 4 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 5 'supportVnodes' '4'")
        tdSql.execute(f"alter dnode 6 'supportVnodes' '4'")
        clusterComCheck.checkDnodeSupportVnodes(1, 4)
        clusterComCheck.checkDnodeSupportVnodes(2, 4)
        clusterComCheck.checkDnodeSupportVnodes(3, 4)
        clusterComCheck.checkDnodeSupportVnodes(4, 4)
        clusterComCheck.checkDnodeSupportVnodes(5, 4)
        clusterComCheck.checkDnodeSupportVnodes(6, 4)
        clusterComCheck.checkDnodes(6)

        tdLog.info(f"======================== dnode1 start")
        dbPrefix = "r3d6_db"
        tbPrefix = "r3d6_tb"
        mtPrefix = "r3d6_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdLog.info(f"dnodes ==> {tdSql.getRows()})")
        tdSql.checkRows(6)

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db} replica 3 vgroups 3")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                val = x * 60000
                ms = 1519833600000 + val
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1

            i = i + 1

        tdSql.query(f"show vgroups")
        tdLog.info(f"vgroups ==> {tdSql.getRows()})")
        tdSql.checkRows(3)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select count(*) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select count(tbcol) from {tb} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select _wstart, count(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select _wstart, count(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, rowNum)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select _wstart, count(tbcol) as b from {tb} where ts <= 1519833840000 interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 1)

        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select count(*) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select count(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select count(tbcol) as c from {mt} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50)

        tdSql.query(f"select count(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol) as c from {mt} where tgcol < 5 and ts <= 1519833840000"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step9")

        tdSql.query(f"select _wstart, count(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 200)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select count(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.checkRows(tbNum)
