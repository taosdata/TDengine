# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_basic2(self):
        """valgrind basic 2

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/basic2.sim

        """

        tdLog.info(
            f"=============== step1: create drop select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(1)

        tbPrefix = "tb"
        tbNum = 5
        rowNum = 10

        tdLog.info(f"=============== step2: prepare data")
        tdSql.execute(f"create database db vgroups 2")
        tdSql.execute(f"use db")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, tbcol int, tbcol2 float, tbcol3 double) tags (tgcol int unsigned)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using stb tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} , {x} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step3: tb")
        tdSql.query(
            f"select * from tb1 where ts in ('2018-07-10 16:31:01', '2022-07-10 16:31:03', 1657441865000);"
        )
        tdSql.query(f"select * from tb1 where tbcol2 in (257);")
        tdSql.query(f"select * from tb1 where tbcol3 in (2, 257);")
        tdSql.query(
            f"select * from stb where ts in ('2018-07-10 16:31:01', '2022-07-10 16:31:03', 1657441865000);"
        )
        tdSql.query(f"select * from stb where tbcol2 in (257);")
        tdSql.query(f"select * from stb where tbcol3 in (2, 257);")
