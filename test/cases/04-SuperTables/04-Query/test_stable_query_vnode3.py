import pytest
import sys
import time
import random
import taos
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, cluster, sc, clusterComCheck


class TestStableQueryVnode3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_query_vnode3(self):
        """超级表查询（3 vnode）

        1. 待补充

        Catalog:
            - SuperTables:Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/stable/vnode3.sim

        """

        tdLog.info(f"======================== dnode1 start")
        dbPrefix = "v3_db"
        tbPrefix = "v3_tb"
        mtPrefix = "v3_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(dbname=db, vgroups=3)
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
        tdSql.query(f"select count(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol) as b from {tb} where ts <= 1519833840000 interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

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
        tdSql.query(f"select count(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select count(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select count(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol) as b from {mt}  where ts <= 1519833840000 partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
