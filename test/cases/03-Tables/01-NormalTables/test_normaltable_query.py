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

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import time


class TestNormalTableQuery:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")


    def test_normal_table_query(self):
        """Normal table filter

        1. Create a normal table
        2. Insert data
        3. Execute projection queries
        4. Execute aggregate queries
        5. Execute field filtering queries

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/field/single.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_query.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "st"
        rowNum = 20

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol int)")

        x = 0
        while x < rowNum:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (1626739200000 + {ms} , {x} )")
            x = x + 1

        tdLog.info(f"=============== step2")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rowNum)

        tdSql.query(f"select * from {tb} where tbcol = 10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)

        tdSql.query(f"select * from {tb} where tbcol = 8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 8)

        tdSql.query(f"select * from {tb} where tbcol < 10")
        tdSql.checkRows(10)
        tdSql.checkData(9, 1, 9)

        tdSql.query(f"select * from {tb} where tbcol <= 10")
        tdSql.checkRows(11)
        tdSql.checkData(8, 1, 8)

        tdSql.query(f"select * from {tb} where tbcol > 10")
        tdSql.checkRows(9)
        tdSql.checkData(8, 1, 19)

        tdSql.query(f"select * from {tb} where tbcol > 10 order by ts asc")
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 11)

        tdSql.query(
            f"select * from {tb} where tbcol < 10 and tbcol > 5 order by ts desc"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(3, 1, 6)

        tdSql.query(
            f"select * from {tb} where tbcol < 10 and tbcol > 5  order by ts asc"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(3, 1, 9)

        tdSql.query(
            f"select * from {tb} where tbcol > 10 and tbcol < 5  order by ts asc"
        )
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from {tb} where ts < 1626739440001")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {tb} where tbcol = 10 and ts < 1626739440001")
        tdLog.info(f"select * from {tb} where tbcol = 10 and ts < 1626739440001")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where tbcol = 4 and ts < 1626739440001 order by ts desc"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 4)

        tdSql.query(
            f"select * from {tb} where tbcol < 10  and ts < 1626739440001 order by ts desc"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 4)

        tdSql.query(
            f"select * from {tb} where tbcol < 10 and ts > 1626739440001 and ts < 1626739500001 order by ts desc"
        )
        tdLog.info(f"{tdSql.getRows()} {tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(*) from {tb}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(*) from {tb} where tbcol = 10")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from {tb} where tbcol < 10")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select count(*) from {tb} where tbcol <= 10")
        tdSql.checkData(0, 0, 11)

        tdSql.query(f"select count(*) from {tb} where tbcol < 10 and tbcol > 5")
        tdSql.checkData(0, 0, 4)

        tdSql.error(
            f"select count(*) from {tb} where tbcol < 10 and tbcol > 5 order by ts asc"
        )

        tdLog.info(f"=============== step5")
        tdSql.query(f"select count(*) from {tb} where ts < 1626739440001")
        tdSql.checkData(0, 0, 5)

        tdSql.query(f"select count(*) from {tb} where tbcol = 4 and ts < 1626739440001")
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select count(*) from {tb} where tbcol < 10  and ts < 1626739440001"
        )
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select count(*) from {tb} where tbcol < 10 and ts > 1626739440001 and ts < 1626739500001"
        )
        tdSql.checkData(0, 0, 1)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


    def test_normal_table_aggregate(self):
        """Normal table aggregate

        1. Create a table with 256 columns
        2. Insert data
        3. Execute projection queries
        4. Execute filter queries
        5. Execute aggregate queries
        6. Kill the process and restart the database

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/column/table.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_query.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"=============== step1")
        tdSql.prepare("d1", drop=True)
        tdSql.execute(f"use d1")
        tdSql.execute(
            f"create table d1.t1 (ts timestamp, c000 int, c001 int, c002 int, c003 int, c004 int, c005 int, c006 int, c007 int, c008 int, c009 int, c010 int, c011 int, c012 int, c013 int, c014 int, c015 int, c016 int, c017 int, c018 int, c019 int, c020 int, c021 int, c022 int, c023 int, c024 int, c025 int, c026 int, c027 int, c028 int, c029 int, c030 int, c031 int, c032 int, c033 int, c034 int, c035 int, c036 int, c037 int, c038 int, c039 int, c040 int, c041 int, c042 int, c043 int, c044 int, c045 int, c046 int, c047 int, c048 int, c049 int, c050 int, c051 int, c052 int, c053 int, c054 int, c055 int, c056 int, c057 int, c058 int, c059 int, c060 int, c061 int, c062 int, c063 int, c064 int, c065 int, c066 int, c067 int, c068 int, c069 int, c070 int, c071 int, c072 int, c073 int, c074 int, c075 int, c076 int, c077 int, c078 int, c079 int, c080 int, c081 int, c082 int, c083 int, c084 int, c085 int, c086 int, c087 int, c088 int, c089 int, c090 int, c091 int, c092 int, c093 int, c094 int, c095 int, c096 int, c097 int, c098 int, c099 int, c100 int, c101 int, c102 int, c103 int, c104 int, c105 int, c106 int, c107 int, c108 int, c109 int, c110 int, c111 int, c112 int, c113 int, c114 int, c115 int, c116 int, c117 int, c118 int, c119 int, c120 int, c121 int, c122 int, c123 int, c124 int, c125 int, c126 int, c127 int, c128 int, c129 int, c130 int, c131 int, c132 int, c133 int, c134 int, c135 int, c136 int, c137 int, c138 int, c139 int, c140 int, c141 int, c142 int, c143 int, c144 int, c145 int, c146 int, c147 int, c148 int, c149 int, c150 int, c151 int, c152 int, c153 int, c154 int, c155 int, c156 int, c157 int, c158 int, c159 int, c160 int, c161 int, c162 int, c163 int, c164 int, c165 int, c166 int, c167 int, c168 int, c169 int, c170 int, c171 int, c172 int, c173 int, c174 int, c175 int, c176 int, c177 int, c178 int, c179 int, c180 int, c181 int, c182 int, c183 int, c184 int, c185 int, c186 int, c187 int, c188 int, c189 int, c190 int, c191 int, c192 int, c193 int, c194 int, c195 int, c196 int, c197 int, c198 int, c199 int, c200 int, c201 int, c202 int, c203 int, c204 int, c205 int, c206 int, c207 int, c208 int, c209 int, c210 int, c211 int, c212 int, c213 int, c214 int, c215 int, c216 int, c217 int, c218 int, c219 int, c220 int, c221 int, c222 int, c223 int, c224 int, c225 int, c226 int, c227 int, c228 int, c229 int, c230 int, c231 int, c232 int, c233 int, c234 int, c235 int, c236 int, c237 int, c238 int, c239 int, c240 int, c241 int, c242 int, c243 int, c244 int, c245 int, c246 int, c247 int, c248 int, c249 int, c250 int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step2")
        tdSql.execute(
            f"insert into d1.t1 values (now,0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+1m,1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+2m,2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+3m,3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+4m,4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+5m,5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+6m,6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+7m,7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+8m,8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+9m,9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 )"
        )

        tdLog.info(f"======= step3")
        tdSql.query(f"select * from d1.t1")
        tdLog.info(f"select * from d1.t1 => rows {tdSql.getRows()}")
        tdSql.checkRows(10)

        tdSql.query(f"select * from d1.t1 where ts < now + 4m")
        tdLog.info(f"select * from d1.t1 where ts < now + 4m => rows {tdSql.getRows()}")
        tdSql.checkRows(5)

        tdSql.query(f"select * from d1.t1 where c001 = 1")
        tdLog.info(f"select * from d1.t1 where c001 = 1 => rows {tdSql.getRows()}")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d1.t1 where c002 = 2 and c003 = 2")
        tdLog.info(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 => rows {tdSql.getRows()}"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 and ts < now + 4m"
        )
        tdLog.info(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 and ts < now + 4m => rows {tdSql.getRows()}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from d1.t1")
        tdLog.info(f"select count(*) from d1.t1 => {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250) from d1.t1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d1.t1"
        )

        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)

        tdLog.info(f"=============== step4")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"==============  step5")

        tdSql.query(f"select * from d1.t1")
        tdLog.info(f"select * from d1.t1 => rows {tdSql.getRows()}")
        tdSql.checkRows(10)

        tdSql.query(f"select * from d1.t1 where c001 = 1")
        tdLog.info(f"select * from d1.t1 where c001 = 1 => rows {tdSql.getRows()}")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d1.t1 where c002 = 2 and c003 = 2")
        tdLog.info(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 => rows {tdSql.getRows()}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from d1.t1")
        tdLog.info(f"select count(*) from d1.t1 => {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250) from d1.t1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d1.t1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)


    def test_composite_key_load(self):
        """Normal table composite key

        Error Reading Composite Key Data from Memory and STT Files

        Root Cause:
        The minKeyused a shallow copy. During backward iteration in STT, this accidentally modified the minKeyvalue. When merging keys from memory and STT, inconsistent key states caused data corruption.

        Reproduction Steps:
        1. Create table with timestamp column and composite key string column. Insert 4 records into STT files and 1 record into memory.
        2. Execute query → Trigger failure

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: TS-6787

        History:
            - 2025-07-04 Haojun Liao
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_query.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        dbPrefix = "m_si_db"

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table t(ts timestamp, k varchar(12) primary key)")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00001')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00003')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00005')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00007')")
        tdSql.execute(f"flush database {db}; ")

        time.sleep(5)
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00005')")

        tdLog.info("insert completed, check results")
        tdSql.query("select * from t")

        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 'abcd-00001')
        tdSql.checkData(1, 1, 'abcd-00003')
        tdSql.checkData(2, 1, 'abcd-00005')
        tdSql.checkData(3, 1, 'abcd-00007')

        print(f"=============== {tdSql.getData(1,1)}")
