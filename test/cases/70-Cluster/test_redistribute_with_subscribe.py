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
import os
import sys
import time

from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from redistribute_concurrent_common import RedistributeConcurrentBase


class TestRedistributeWithSubscribe(RedistributeConcurrentBase):
    """Verify data subscription (TMQ) works while redistribute is in-flight."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_redistribute_with_subscribe(self):
        """Redistribute concurrent with data subscription

        Verify that a TMQ consumer keeps consuming correctly while a
        REDISTRIBUTE VGROUP transaction is running in the background.

        1. Deploy a 4-dnode cluster (dnode1 mnode-only), prepare a database
           with a super table and a batch of data, then create a topic.
        2. Start a background thread that keeps redistributing the vgroups.
        3. In parallel, subscribe with a TMQ consumer and consume all the data;
           the consumer must eventually receive every row despite the vnodes
           being moved between dnodes.
        4. Stop the background thread and verify data integrity.

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-07-16 Created

        """
        dbname = "rdst_tmq"
        topicName = "topic_rdst_tmq"
        stbname = "stb"
        ctbNum = 10
        rowsPerTbl = 100

        # Prepare cluster + a database with data on the vgroups to redistribute.
        self.prepare_cluster()
        totalRows = self.prepare_data(dbname, stbname=stbname, ctbNum=ctbNum,
                                      rowsPerTbl=rowsPerTbl, vgroups=2, replica=1)

        # Create a topic over the whole super table BEFORE starting the
        # background redistribute so topic creation itself does not race with
        # the redistribute transaction. (Topic creation is an mnode DDL and
        # would otherwise conflict with the redistribute mnode transaction.)
        tdSql.execute(f"use {dbname}")
        tdSql.execute(
            f"create topic {topicName} as select ts, c1, c2 from {dbname}.{stbname}"
        )
        tdLog.info(f"created topic {topicName}")

        # Start churning the vgroups in the background.
        thread, stop_event, errors = self.start_redistribute(dbname)

        consumer = None
        try:
            # Build a TMQ consumer and subscribe. auto.offset.reset=earliest so
            # it consumes the pre-inserted data from the WAL.
            consumer = Consumer({
                "group.id": "cgrp_rdst_tmq",
                "client.id": "client_rdst_tmq",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
                "auto.commit.interval.ms": "1000",
                "auto.offset.reset": "earliest",
                "experimental.snapshot.enable": "false",
            })
            # The FIRST subscribe of a consumer group creates the
            # subscription/consumer-group metadata on the mnode, which is a
            # mnode DDL transaction.  While a redistribute (GLOBAL) transaction
            # is in-flight this fails with 0x03D3 "Conflict transaction not
            # completed", so we retry it in the idle window between
            # redistributes, exactly like the create/alter stable DDLs.
            self.call_with_conflict_retry(
                lambda: consumer.subscribe([topicName]),
                desc=f"tmq subscribe {topicName}",
            )
            tdLog.info("consumer subscribed, start polling")

            consumed = 0
            noDataCount = 0
            # Poll until all rows are consumed, or we see no data for a while.
            # A generous no-data budget is used because redistribute can briefly
            # make a vgroup unavailable while its leader is moving.
            while consumed < totalRows:
                res = consumer.poll(2)
                if not res:
                    noDataCount += 1
                    # 60 * 2s = 120s of silence before giving up.
                    if noDataCount >= 60:
                        tdLog.info("no new data for 120s, stop polling")
                        break
                    continue

                if res.error() is not None:
                    raise Exception(f"tmq poll error: {res.error()}")

                noDataCount = 0
                for block in res.value():
                    consumed += len(block.fetchall())

                tdLog.info(f"consumed {consumed}/{totalRows} rows")

            # The consumer must have received every row that was inserted.
            if consumed < totalRows:
                tdLog.exit(
                    f"tmq consume incomplete: got {consumed}, expected {totalRows}"
                )
            tdLog.info(f"tmq consumed all {consumed} rows during redistribute")
        finally:
            if consumer:
                consumer.close()
            # Always stop the background thread, even on assertion failure.
            self.stop_redistribute(thread, stop_event, errors)

        # Cleanup topic and verify data integrity.
        tdSql.execute(f"drop topic {topicName}")
        tdSql.query(f"select count(*) from {dbname}.{stbname}")
        tdSql.checkData(0, 0, totalRows)
        tdLog.info(f"data integrity verified: {totalRows} rows preserved")
