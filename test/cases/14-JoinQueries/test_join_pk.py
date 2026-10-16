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

from new_test_framework.utils import tdLog, tdSql, tdCom
import os

class TestJoinPk:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_pk(self):
        """Join inner 

        1. Create 1 database and 1 super table
        2. Create 2 child tables
        3. Insert 1 rows data to each child table with different timestamps
        4. Inner join two child tables on timestamp with interval(1s)
        5. Check the result of join correctly 

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/query/join_pk.sim

        """
        
        self.join_pk()
        self.join_time_keep_func()

    def join_pk(self):
        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values('2023-08-07 13:30:56', 0)('2023-08-07 13:30:57', 1)")
        tdSql.execute(f"insert into ct2 using st tags(2) values('2023-08-07 13:30:58', 2)('2023-08-07 13:30:59', 3)")
        tdSql.query(
            f"select * from (select _wstart - 1s as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )

        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(2, 1, 1)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(1, 3, 1)

        tdSql.checkData(2, 3, 1)

        tdSql.query(
            f"select * from (select _wstart - 1d as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from (select _wstart + 1a as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from (select _wstart *  3 as ts, count(*) as num1 from st interval(1s)) as t1 inner join (select _wstart as ts, count(*) as num2 from st interval(1s)) as t2 on t1.ts = t2.ts"
        )
        tdSql.checkRows(0)

        tdSql.execute(
            f"create table sst(ts timestamp, ts2 timestamp, f int) tags(t int);"
        )
        tdSql.execute(
            f"insert into sct1 using sst tags(1) values('2023-08-07 13:30:56', '2023-08-07 13:30:56', 0)('2023-08-07 13:30:57', '2023-08-07 13:30:57', 1)"
        )
        tdSql.execute(
            f"insert into sct2 using sst tags(2) values('2023-08-07 13:30:58', '2023-08-07 13:30:58', 2)('2023-08-07 13:30:59', '2023-08-07 13:30:59', 3)"
        )
        tdSql.query(
            f"select * from (select ts - 1s as jts from sst) as t1 inner join (select ts-1s as jts from sst) as t2 on t1.jts = t2.jts"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from (select ts - 1s as jts from sst) as t1 inner join (select ts as jts from sst) as t2 on t1.jts = t2.jts"
        )
        tdSql.checkRows(3)

        # ts2 is a non-primary timestamp expression. It is materialized by
        # each subquery and can be used as a regular Hash Join key.
        tdSql.query(
            f"select * from (select ts2 - 1s as jts from sst) as t1 inner join (select ts2 as jts from sst) as t2 on t1.jts = t2.jts"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select /*+ JOIN_ALGO(HASH) */ * from (select ts2 - 1s as jts from sst) as t1 inner join (select ts2 as jts from sst) as t2 on t1.jts = t2.jts"
        )
        tdSql.checkRows(3)

        # Forced Merge materializes the timestamp expression and inserts Sort.
        tdSql.query(
            f"select /*+ JOIN_ALGO(MERGE) */ * from (select ts2 - 1s as jts from sst) as t1 inner join (select ts2 as jts from sst) as t2 on t1.jts = t2.jts"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select b.*, a.ats from (select _rowts ats, first(f) from sst) as a full join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)
        
        tdSql.query(
            f"select b.*, a.ats from (select _rowts ats, first(f) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 0)
        
        tdSql.query(
            f"select b.*, a.ats from (select _rowts ats, max(f) from sst) as a full join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)
        
        tdSql.query(
            f"select b.*, a.ats from (select _rowts ats, max(f) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 3)
        
        tdSql.query(
            f"select b.*, a.ats from (select _rowts ats, min(f) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 0)
        
    def time_keep_func(self):
        tdLog.info("test join with time keep function")
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, STATECOUNT(f, 'LE', 2) from sst) as a full join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)      

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, STATECOUNT(f, 'LE', 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)  

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, STATEDURATION(f, 'LE', 2) from sst) as a full join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)      

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, STATEDURATION(f, 'LE', 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)  

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, mavg(f, 4) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, mavg(f, 3) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, mavg(f, 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(3)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, top(f, 1) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, top(f, 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, top(f, 2) from sst order by ats) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ats desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ats) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ats desc limit 2) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ats limit 2) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, top(f, 2) from sst order by ats desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, tail(f, 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, tail(f, 2) from sst order by ats desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, tail(f, 3) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(3)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, tail(f, 1) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
           
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ts asc limit 1) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ts desc limit 2) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ts desc limit 1) as a inner join sst b on a.ats = b.ts and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
 
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, bottom(f, 1) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
                        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, bottom(f, 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, bottom(f, 2) from sst order by ats) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, bottom(f, 2) from sst order by ats desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, f from sst order by ats desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, fill_forward(f) from sst order by ats desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 1) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 3) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(3)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 1) from sst order by ts) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 2) from sst order by ts) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 3) from sst order by ts) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 2) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 2) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 3) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 1) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 2) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 2) from sst order by ats) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, sample(f, 2) from sst order by ats desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(2)
 
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, mode(f) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(1)
        
        # Multi-row function result keeps a usable timeline; JOIN may add the required time ordering.
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)
        
    def join_testcase(self, testCase):
        tdLog.info(f"test {testCase} with constant condition")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)
     
    def join_with_const_condition(self):
        tdLog.info("test join with constant condition")
        
        # Multi-row function result keeps a usable timeline; JOIN may add the required time ordering.
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(4)
        
        self.join_testcase("test_join_pk_inner_join_with_const_condition")
        self.join_testcase("test_join_pk_full_join_with_const_condition")
        self.join_testcase("test_join_pk_left_join_with_const_condition")
        self.join_testcase("test_join_pk_right_join_with_const_condition")
        self.join_testcase("test_join_pk_left_semi_with_const_condition")
        self.join_testcase("test_join_pk_right_semi_with_const_condition")
        self.join_testcase("test_join_pk_left_anti_with_const_condition")
        self.join_testcase("test_join_pk_right_anti_with_const_condition")
        self.join_testcase("test_join_pk_left_asof_with_const_condition")
        self.join_testcase("test_join_pk_right_asof_with_const_condition")
        self.join_testcase("test_join_pk_left_window_with_const_condition")
        self.join_testcase("test_join_pk_right_window_with_const_condition")
            
    def join_time_keep_func(self):
        
        tdSql.execute(f"create database test1")
        tdSql.execute(f"use test1")
      
        tdSql.execute(
            f"create table sst(ts timestamp, ts2 timestamp, f int) tags(t int);"
        )
        tdSql.execute(
            f"insert into sct1 using sst tags(1) values('2023-08-07 13:30:56', '2023-08-07 13:30:56', 0)('2023-08-07 13:30:57', '2023-08-07 13:30:57', 1)"
        )
        tdSql.execute(
            f"insert into sct2 using sst tags(2) values('2023-08-07 13:30:58', '2023-08-07 13:30:58', 2)('2023-08-07 13:30:59', '2023-08-07 13:30:59', 3)"
        )
        
        self.time_keep_func()
        self.join_with_const_condition()
       
        tdSql.query(
            f"select count(*) from (select _rowts ats, first(f) from sst) as a full join (select * from sst order by ts desc) b on timetruncate(now+1a, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(now, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        tdSql.checkRows(0)
          
        tdSql.error(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats+1a, 1s) = timetruncate(b.ts+1a, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a",
            expectedErrno=int(0x2703)
        )
        tdSql.error(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats+1a, 1s) = timetruncate(b.ts+2a, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a",
            expectedErrno=int(0x2703)
        )
        tdSql.error(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on timetruncate(a.ats+1a, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a",
            expectedErrno=int(0x2703)
        )
        
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on a.ats+1a = b.ts+1a",
        )
        tdSql.checkRows(4)
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on a.ats = b.ts+1a",
        )
        tdSql.checkRows(0)
        tdSql.query(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst order by ts desc) as a inner join sst b on a.ats+1a = b.ts",
        )
        tdSql.checkRows(0)
