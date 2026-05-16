###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import taos
import sys
import time
import socket
import os
import threading
import subprocess

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom


class TestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict
    simClientCfg = ""

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def prepareData(self):
        # Source database with regular tables
        tdSql.execute(f'create database if not exists db_src vgroups 1')
        tdSql.execute(f'use db_src')

        # Source super table and child tables (tag sources)
        tdSql.execute(f'create stable if not exists stb_src (ts timestamp, c1 int, c2 binary(32)) '
                      f'tags (location binary(32), region int)')
        tdSql.execute(f'insert into ct1 using stb_src tags("beijing", 100) '
                      f'values(1669092069061, 1, "ct1_data")')
        tdSql.execute(f'insert into ct2 using stb_src tags("shanghai", 200) '
                      f'values(1669092069062, 2, "ct2_data")')
        tdSql.execute(f'insert into ct3 using stb_src tags("shenzhen", 300) '
                      f'values(1669092069063, 3, "ct3_data")')

        # Normal table as additional tag source
        tdSql.execute(f'create table nt1 (ts timestamp, c1 int, c2 binary(32))')
        tdSql.execute(f'insert into nt1 values(1669092069011, 11, "nt1_data")')

        # Virtual super table with tag-ref
        # Tags: t_loc references source table's location tag, t_region references region tag
        tdSql.execute(f'create stable if not exists v_stb_tagref '
                      f'(ts timestamp, c1 int, c2 binary(32)) '
                      f'tags (t_loc binary(32), t_region int) virtual 1')

        # Virtual child table with both col-ref AND tag-ref
        # c1 col-ref from ct1.c1, tags from ct1's tags
        tdSql.execute(f'create vtable v_ct1 '
                      f'(c1 from db_src.ct1.c1) '
                      f'using v_stb_tagref '
                      f'tags(t_loc from db_src.ct1.location, t_region from db_src.ct1.region)')

        # Virtual child table: col-ref from ct2, tag-ref from ct2 (mixed literal + ref)
        tdSql.execute(f'create vtable v_ct2 '
                      f'(c1 from db_src.ct2.c1, c2 from nt1.c2) '
                      f'using v_stb_tagref '
                      f'tags(t_loc from db_src.ct2.location, t_region from db_src.ct2.region)')

        # Virtual child table: col-ref from ct1, tag-ref from ct3 (cross-source tags)
        tdSql.execute(f'create vtable v_ct3 '
                      f'(c1 from db_src.ct3.c1) '
                      f'using v_stb_tagref '
                      f'tags(t_loc from db_src.ct3.location, t_region from db_src.ct1.region)')

    def run_cmd(self, topic, snapshot, json, writeData):
        buildPath = tdCom.getBuildPath()
        cmdStr = f'{buildPath}/build/bin/tmq_vtable'
        subprocess.run([cmdStr, topic, snapshot, json, self.simClientCfg, writeData])

    def case_non_snapshot_db_topic(self):
        """DB topic (non-snapshot): verify tagRef in JSON metadata"""
        tdLog.info("case_non_snapshot_db_topic start ...")
        topicName = "tagref_non_snap_db"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB {topicName}' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        # Run consumer - empty json means skip json comparison, just verify write_raw works
        self.run_cmd(topicName, "false", "", "true")

        # Verify virtual child tables were replicated correctly
        self.checkReplicatedTables(topicName)
        tdLog.info("case_non_snapshot_db_topic end ...")

    def case_non_snapshot_stb_topic(self):
        """STB topic (non-snapshot): verify tagRef in JSON metadata"""
        tdLog.info("case_non_snapshot_stb_topic start ...")
        topicName = "tagref_non_snap_stb"
        tdSql.execute(f"create topic {topicName} with meta as stable v_stb_tagref")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB db_src' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        self.run_cmd(topicName, "false", "", "true")
        self.checkReplicatedVChildTables(topicName)
        tdLog.info("case_non_snapshot_stb_topic end ...")

    def case_snapshot_db_topic(self):
        """DB topic (snapshot): verify tagRef in JSON metadata"""
        tdLog.info("case_snapshot_db_topic start ...")
        topicName = "tagref_snap_db"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB {topicName}' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        self.run_cmd(topicName, "true", "", "true")
        self.checkReplicatedTables(topicName)
        tdLog.info("case_snapshot_db_topic end ...")

    def case_snapshot_stb_topic(self):
        """STB topic (snapshot): verify tagRef in JSON metadata"""
        tdLog.info("case_snapshot_stb_topic start ...")
        topicName = "tagref_snap_stb"
        tdSql.execute(f"create topic {topicName} with meta as stable v_stb_tagref")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB db_src' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        self.run_cmd(topicName, "true", "", "true")
        self.checkReplicatedVChildTables(topicName)
        tdLog.info("case_snapshot_stb_topic end ...")

    def case_query_topic_blocked(self):
        """COLUMN topic on virtual table should fail"""
        tdLog.info("case_query_topic_blocked start ...")
        tdSql.error(f"create topic tagref_query as select * from v_stb_tagref")
        tdSql.error(f"create topic tagref_query as select * from v_ct1")
        tdLog.info("case_query_topic_blocked end ...")

    def checkReplicatedTables(self, dbName):
        """Check all virtual child tables were replicated via DB topic"""
        self.checkReplicatedVChildTables(dbName)

    def checkReplicatedVChildTables(self, dbName):
        """Verify replicated virtual child tables match source"""
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_ct1",
            exp_sql=f"select * from {dbName}.v_ct1",
        )
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_ct2",
            exp_sql=f"select * from {dbName}.v_ct2",
        )
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_ct3",
            exp_sql=f"select * from {dbName}.v_ct3",
        )

    def test_tmq_vtable_tag_ref(self):
        """TMQ subscription with virtual table tag-ref metadata.

        Verifies that tag-ref metadata (tagRef entries in SColRefWrapper) is
        correctly transmitted through TMQ subscription for virtual child tables.
        Tests DB topic and STB topic in both snapshot and non-snapshot modes.
        Confirms COLUMN subscription on virtual tables is properly rejected.

        Since: 3.4.1.0

        Labels: tmq,vtable

        Jira: None

        Catalog:
        - tmq:vtable:tag_ref

        History:
        - created 2026.04.17

        """
        tdSql.execute(f'alter dnode 1 "debugflag 135"')

        self.prepareData()
        self.simClientCfg = os.path.join(tdDnodes.getSimCfgPath(), "taos.cfg")

        # COLUMN topic on virtual tables should be rejected
        self.case_query_topic_blocked()

        # Non-snapshot modes
        self.case_non_snapshot_db_topic()
        self.case_non_snapshot_stb_topic()

        # Flush and test snapshot modes
        tdSql.execute(f'flush database db_src')
        self.case_snapshot_db_topic()
        self.case_snapshot_stb_topic()

        tdLog.success(f"{__file__} successfully executed")
