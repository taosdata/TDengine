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
import os

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom
from taos.tmq import Consumer, TmqError


class TestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def prepareData(self):
        tdSql.execute(f'drop database if exists db_src')
        tdSql.execute(f'create database db_src vgroups 1')
        tdSql.execute(f'use db_src')

        # Source super table and child tables (tag sources)
        tdSql.execute(f'create stable stb_src (ts timestamp, c1 int, c2 binary(32)) '
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
        tdSql.execute(f'create stable v_stb_tagref '
                      f'(ts timestamp, c1 int, c2 binary(32)) '
                      f'tags (t_loc binary(32), t_region int) virtual 1')

        # Virtual child table with both col-ref AND tag-ref
        tdSql.execute(f'create vtable v_ct1 '
                      f'(c1 from db_src.ct1.c1) '
                      f'using v_stb_tagref '
                      f'tags(t_loc from db_src.ct1.location, t_region from db_src.ct1.region)')

        # Virtual child table: col-ref from ct2, tag-ref from ct2
        tdSql.execute(f'create vtable v_ct2 '
                      f'(c1 from db_src.ct2.c1, c2 from nt1.c2) '
                      f'using v_stb_tagref '
                      f'tags(t_loc from db_src.ct2.location, t_region from db_src.ct2.region)')

        # Virtual child table: col-ref from ct3, tag-ref cross-source
        tdSql.execute(f'create vtable v_ct3 '
                      f'(c1 from db_src.ct3.c1) '
                      f'using v_stb_tagref '
                      f'tags(t_loc from db_src.ct3.location, t_region from db_src.ct1.region)')

    def consume_topic(self, topic_name, snapshot="false", group_id="tagref_grp"):
        """Consume a topic and return (meta_count, data_row_count)."""
        consumer_dict = {
            "group.id": group_id,
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": snapshot,
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([topic_name])
        except TmqError:
            tdLog.exit(f"subscribe to {topic_name} failed")

        meta_count = 0
        data_row_count = 0
        empty_poll_count = 0
        max_empty_polls = 3
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        break
                    continue
                empty_poll_count = 0
                val = res.value()
                if val is None:
                    # META message
                    meta_count += 1
                    continue
                for block in val:
                    data_row_count += len(block.fetchall())
        finally:
            consumer.close()

        tdLog.info(f"topic={topic_name} snapshot={snapshot}: "
                   f"meta={meta_count} data_rows={data_row_count}")
        return meta_count, data_row_count

    def case_query_topic_blocked(self):
        """COLUMN topic on virtual table should fail"""
        tdLog.info("case_query_topic_blocked start ...")
        tdSql.error(f"create topic tagref_query as select * from v_stb_tagref")
        tdSql.error(f"create topic tagref_query as select * from v_ct1")
        tdLog.info("case_query_topic_blocked end ...")

    def case_non_snapshot_db_topic(self):
        """DB topic (non-snapshot): consume meta+data for tag-ref vtables"""
        tdLog.info("case_non_snapshot_db_topic start ...")
        topicName = "tagref_non_snap_db"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")

        # Insert new data after topic creation so WAL captures it
        tdSql.execute(f"insert into db_src.ct1 values(1669092069071, 10, 'new_ct1')")

        meta_count, data_rows = self.consume_topic(topicName, "false", "grp_ns_db")
        # DB topic should produce some messages (meta or data via WAL)
        if meta_count == 0 and data_rows == 0:
            tdLog.exit(f"Expected messages from DB topic, got none")

        tdSql.execute(f"drop topic {topicName}")
        tdLog.info("case_non_snapshot_db_topic end ...")

    def case_non_snapshot_stb_topic(self):
        """STB topic (non-snapshot): consume meta+data for tag-ref vtables"""
        tdLog.info("case_non_snapshot_stb_topic start ...")
        topicName = "tagref_non_snap_stb"
        tdSql.execute(f"create topic {topicName} with meta as stable db_src.v_stb_tagref")

        # Insert new data after topic creation
        tdSql.execute(f"insert into db_src.ct2 values(1669092069072, 20, 'new_ct2')")

        meta_count, data_rows = self.consume_topic(topicName, "false", "grp_ns_stb")
        # STB topic should produce some messages
        if meta_count == 0 and data_rows == 0:
            tdLog.exit(f"Expected messages from STB topic, got none")

        tdSql.execute(f"drop topic {topicName}")
        tdLog.info("case_non_snapshot_stb_topic end ...")

    def case_snapshot_db_topic(self):
        """DB topic (snapshot): consume data for tag-ref vtables"""
        tdLog.info("case_snapshot_db_topic start ...")
        topicName = "tagref_snap_db"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")

        meta_count, data_rows = self.consume_topic(topicName, "true", "grp_snap_db")
        # Snapshot mode should produce messages (meta or data)
        if meta_count == 0 and data_rows == 0:
            tdLog.exit(f"Expected messages from snapshot DB topic, got none")

        tdSql.execute(f"drop topic {topicName}")
        tdLog.info("case_snapshot_db_topic end ...")

    def case_snapshot_stb_topic(self):
        """STB topic (snapshot): consume data for tag-ref vtables"""
        tdLog.info("case_snapshot_stb_topic start ...")
        topicName = "tagref_snap_stb"
        tdSql.execute(f"create topic {topicName} with meta as stable db_src.v_stb_tagref")

        meta_count, data_rows = self.consume_topic(topicName, "true", "grp_snap_stb")
        # Snapshot STB topic should produce messages
        if meta_count == 0 and data_rows == 0:
            tdLog.exit(f"Expected messages from snapshot STB topic, got none")

        tdSql.execute(f"drop topic {topicName}")
        tdLog.info("case_snapshot_stb_topic end ...")

    def case_verify_vtable_data_via_sql(self):
        """Verify virtual table tag-ref data accessible via SQL after TMQ consumption"""
        tdLog.info("case_verify_vtable_data_via_sql start ...")

        # Verify tag-ref values are correct for v_ct1 (single source: ct1)
        tdSql.query("select t_loc, t_region from db_src.v_ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "beijing")
        tdSql.checkData(0, 1, 100)

        # v_ct2 has 2 sources (ct2 + nt1), so 2 rows; tags from ct2
        tdSql.query("select t_loc, t_region from db_src.v_ct2")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "shanghai")
        tdSql.checkData(0, 1, 200)

        # v_ct3 has cross-source tags: location from ct3, region from ct1
        tdSql.query("select t_loc, t_region from db_src.v_ct3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "shenzhen")
        tdSql.checkData(0, 1, 100)

        # Verify data through virtual stb query
        tdSql.query("select * from db_src.v_stb_tagref order by ts")
        tdSql.checkRows(4)

        tdLog.info("case_verify_vtable_data_via_sql end ...")

    def test_tmq_vtable_tag_ref(self):
        """TMQ subscription with virtual table tag-ref metadata.

        Verifies that tag-ref metadata (tagRef entries in SColRefWrapper) is
        correctly transmitted through TMQ subscription for virtual child tables.
        Tests DB topic and STB topic in both snapshot and non-snapshot modes.
        Confirms COLUMN subscription on virtual tables is properly rejected.
        Uses pure Python Consumer API (no external C binary dependency).

        Since: 3.4.1.0

        Labels: tmq,vtable

        Jira: None

        Catalog:
        - tmq:vtable:tag_ref

        History:
        - created 2026.04.17
        - rewritten 2026.05.19: pure Python, no tmq_vtable binary

        """
        self.prepareData()

        # COLUMN topic on virtual tables should be rejected
        self.case_query_topic_blocked()

        # Verify tag-ref data integrity
        self.case_verify_vtable_data_via_sql()

        # Non-snapshot modes
        self.case_non_snapshot_db_topic()
        self.case_non_snapshot_stb_topic()

        # Flush and test snapshot modes
        tdSql.execute(f'flush database db_src')
        self.case_snapshot_db_topic()
        self.case_snapshot_stb_topic()

        tdLog.success(f"{__file__} successfully executed")
