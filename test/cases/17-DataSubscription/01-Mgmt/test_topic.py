import os
import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTmpTopic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tmq_topic(self):
        """Tmq: topic mgmt

        1. Create, delete, show topics
        2. Create topics of database type, super table type, and batch query type

        Catalog:
            - Subscribe

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan migrated from tsim/tmq/topic.sim

        """

        # ---- global parameters start ----#
        dbName = "db"
        vgroups = 1
        stbPrefix = "stb"
        ctbPrefix = "ctb"
        ntbPrefix = "ntb"
        stbNum = 1
        ctbNum = 10
        ntbNum = 10
        rowsPerCtb = 10
        tstart = 1640966400000  # 2022-01-01 00:00:"00+000"
        # ---- global parameters end ----#

        tdSql.connect("root")
        tdLog.info(f"== create database {dbName} vgroups {vgroups}")
        tdSql.execute(f"create database {dbName} vgroups {vgroups}")
        clusterComCheck.checkDbReady(dbName)

        tdSql.execute(f"use {dbName}")

        tdLog.info(f"== alter database")

        tdLog.info(f"== create super table")
        tdSql.execute(
            f"create table {stbPrefix} (ts timestamp, c1 int, c2 float, c3 binary(16)) tags (t1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"== create child table, normal table and insert data")
        i = 0
        while i < ctbNum:
            ctb = ctbPrefix + str(i)
            ntb = ntbPrefix + str(i)
            tdSql.execute(f"create table {ctb} using {stbPrefix} tags( {i} )")
            tdSql.execute(
                f"create table {ntb} (ts timestamp, c1 int, c2 float, c3 binary(16))"
            )
            i = i + 1

        tdLog.info(f"== create topics from super table")
        tdSql.execute(f"create topic topic_stb_column as select ts, c3 from stb")
        tdSql.execute(f"create topic topic_stb_all as select ts, c1, c2, c3 from stb")
        tdSql.execute(
            f"create topic topic_stb_function as select ts, abs(c1), sin(c2) from stb"
        )

        tdLog.info(f"== create topics from child table")
        tdSql.execute(f"create topic topic_ctb_column as select ts, c3 from ctb0")
        tdSql.execute(f"create topic topic_ctb_all as select * from ctb0")
        tdSql.execute(
            f"create topic topic_ctb_function as select ts, abs(c1), sin(c2) from ctb0"
        )

        tdLog.info(f"== create topics from normal table")
        tdSql.execute(f"create topic topic_ntb_column as select ts, c3 from ntb0")
        tdSql.execute(f"create topic topic_ntb_all as select * from ntb0")
        tdSql.execute(
            f"create topic topic_ntb_function as select ts, abs(c1), sin(c2) from ntb0"
        )

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"show topics")
        tdSql.checkRows(9)

        tdSql.execute(f"drop topic topic_stb_column")

        tdSql.query(f"show topics")
        tdSql.checkRows(8)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"== show topics")
        tdSql.query(f"show topics")
        tdSql.checkRows(8)

        tdLog.info(f"== drop topic")
        tdSql.execute(f"drop topic topic_ctb_column")
        tdSql.execute(f"drop topic topic_ntb_column")

        tdLog.info(f"== show topics")
        tdSql.query(f"show topics")
        tdSql.checkRows(6)

        tdSql.execute(f"create topic topic_stable_1 as stable stb where t1 > 0")
        tdSql.execute(
            f"create topic topic_stable_2 as stable stb where t1 > 0 and t1 < 0"
        )
        tdSql.execute(f"create topic topic_stable_3 as stable stb where 1 > 0")
        tdSql.execute(f"create topic topic_stable_4 as stable stb where abs(t1) > 0")
        tdSql.error(f"create topic topic_stable_5 as stable stb where last(t1) > 0")
        tdSql.error(f"create topic topic_stable_5 as stable stb where sum(t1) > 0")
        tdSql.execute(
            f"create topic topic_stable_6 as stable stb where tbname is not null"
        )
        tdSql.execute(f"create topic topic_stable_7 as stable stb where tbname > 'a'")
        tdSql.error(
            f"create topic topic_stable_8 as stable stb where tbname > 0 and xx < 0"
        )
        tdSql.error(
            f"create topic topic_stable_9 as stable stb where tbname > 0 and c1 < 0"
        )
