###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

# Concurrent DDL tests for VST BASE ON inheritance. Verifies mnode
# transaction serialization of parallel CREATE ... BASE ON.

import threading
import taos
from new_test_framework.utils import tdLog, tdSql

DB = "test_vst_concurrent"


class TestVstConcurrent:

    def setup_class(cls):
        tdLog.info("setup database for VST concurrent tests")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")

    def test_concurrent_create_children(self):
        """VST Concurrent: parallel CREATE child BASE ON same parent

        Multiple connections concurrently create child VSTs inheriting
        from the same parent. The mnode transaction layer must serialize
        these creates so every child is created and every one shows up
        in ins_vstable_inherits.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, concurrent

        Jira: None

        History:
            - 2026-06-24 Created
        """
        tdSql.execute(f"use {DB}")
        tdSql.execute("drop stable if exists conc_parent")
        tdSql.execute(
            "create stable conc_parent (ts timestamp, c int) tags (g int) virtual 1"
        )

        N = 8
        ok = [False] * N
        err = [None] * N

        def worker(i):
            try:
                conn = taos.connect(host="localhost", user="root", password="taosdata")
                cur = conn.cursor()
                cur.execute(f"use {DB}")
                cur.execute(f"drop stable if exists conc_child_{i}")
                cur.execute(
                    f"create stable conc_child_{i} (ts timestamp, o int) "
                    f"tags (h int) base on {DB}.conc_parent virtual 1"
                )
                ok[i] = True
                conn.close()
            except Exception as e:
                err[i] = e

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for i in range(N):
            assert ok[i], f"concurrent create child_{i} failed: {err[i]}"

        tdSql.query(
            "select count(*) from information_schema.ins_vstable_inherits "
            "where child_stable_name like 'conc_child_%'"
        )
        assert tdSql.queryResult[0][0] == N, (
            f"expected {N} inherited children, got {tdSql.queryResult[0][0]}"
        )

        for i in range(N):
            tdSql.execute(f"drop stable if exists conc_child_{i}")
        tdSql.execute("drop stable conc_parent")
