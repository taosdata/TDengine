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

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import threading
from new_test_framework.utils import tdLog, tdSql
from taos.error import ProgrammingError
import re
import time
import os
import platform


class TestDatabaseAlterMisc:

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


    def test_database_alter_option_keep(self):
        """Alter keep

        1. Use invalid input to alter the KEEP option
        2. Verify results after changing KEEP
        3. Add or drop columns on the super table
        4. Insert data
        5. Check results
        6. Repeat steps 3-5 several times


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/alter.sim
            - 2025-5-6 Simon Guan Migrated from tsim/parser/alter__for_community_version.sim
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        dbPrefix = "m_alt_db"
        tbPrefix = "m_alt_tb"
        mtPrefix = "m_alt_mt"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} duration 3 keep 20")
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "20d,20d,20d")

        tdSql.error(f'alter database {db} keep "20"')
        tdSql.error(f'alter database {db} keep "20","20","20"')
        tdSql.error(f"alter database {db} keep 0")
        tdSql.error(f"alter database {db} keep 20.0")
        tdSql.error(f"alter database {db} keep 20.0,20.0,20.0")
        tdSql.error(f"alter database {db} keep 0,0,0")
        tdSql.error(f"alter database {db} keep 3")
        tdSql.error(f"alter database {db} keep -1,-1,-1")
        tdSql.execute(f"alter database {db} keep 20,20")
        tdSql.error(f"alter database {db} keep 8,9,9")
        tdSql.error(f"alter database {db} keep 20,20,19")
        tdSql.error(f"alter database {db} keep 20,19,20")
        tdSql.error(f"alter database {db} keep 20,19,19")
        tdSql.error(f"alter database {db} keep 20,19,18")
        tdSql.error(f"alter database {db} keep 20,20,20,20")
        tdSql.error(f"alter database {db} keep 365001,365001,365001")
        tdSql.error(f"alter database {db} keep 365001")
        tdSql.execute(f"alter database {db} keep 20")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "20d,20d,20d")

        tdSql.execute(f"alter database {db} keep 10")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "10d,10d,10d")

        tdSql.execute(f"alter database {db} keep 11")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,11d,11d")

        tdSql.execute(f"alter database {db} keep 13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "13d,13d,13d")

        tdSql.execute(f"alter database {db} keep 365000")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "365000d,365000d,365000d")

        ##### alter table test, simeplest case
        tdSql.execute(f"create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)

        tdSql.execute(f"alter table tb drop column c3")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.execute(f"alter table tb add column c3 nchar(4)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 3, None)

        tdSql.execute(f"insert into tb values (now, 2, 2, 'taos')")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)

        tdLog.info(f"tdSql.getData(0,3) = {tdSql.getData(0,3)}")
        tdSql.checkData(0, 3, "taos")

        tdSql.execute(f"drop table tb")

        ##### alter metric test, simplest case
        tdSql.execute(
            f"create table mt (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int)"
        )
        tdSql.execute(f"create table tb using mt tags(1)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.execute(f"alter table mt drop column c3")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.execute(f"alter table mt add column c3 nchar(4)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 3, None)

        tdSql.execute(f"insert into tb values (now, 2, 2, 'taos')")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, "taos")
        tdSql.checkData(1, 3, None)

        tdSql.execute(f"drop table tb")
        tdSql.execute(f"drop table mt")

        ## [TBASE272]
        tdSql.execute(f"create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.execute(f"alter table tb drop column c3")
        tdSql.execute(f"alter table tb add column c3 nchar(5)")
        tdSql.execute(f"insert into tb values(now, 2, 2, 'taos')")
        tdSql.execute(f"drop table tb")
        tdSql.execute(
            f"create table mt (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int)"
        )
        tdSql.execute(f"create table tb using mt tags(1)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.execute(f"alter table mt drop column c3")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table tb")
        tdSql.execute(f"drop table mt")

        ### ALTER TABLE WHILE STREAMING [TBASE271]
        # sql create table tb1 (ts timestamp, c1 int, c2 nchar(5), c3 int)
        # sql create table strm as select count(*), avg(c1), first(c2), sum(c3) from tb1 interval(2s)
        # sql select * from strm
        # if $rows != 0 then
        #  return -1
        # endi
        # sql insert into tb1 values (now, 1, 'taos', 1)
        # sql select * from strm
        # print rows = $rows
        # if $rows != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != 1 then
        #  return -1
        # endi
        # sql alter table tb1 drop column c3
        # sql insert into tb1 values (now, 2, 'taos')
        # sql select * from strm
        # if $rows != 2 then
        #   return -1
        # endi
        # if $tdSql.getData(0,4) != 1 then
        #  return -1
        # endi
        # sql alter table tb1 add column c3 int
        # sql insert into tb1 values (now, 3, 'taos', 3);
        # sql select * from strm
        # if $rows != 3 then
        #   return -1
        # endi
        # if $tdSql.getData(0,4) != 1 then
        #  return -1
        # endi

        ## ALTER TABLE AND INSERT BY COLUMNS
        tdSql.execute(f"create table mt (ts timestamp, c1 int, c2 int) tags(t1 int)")
        tdSql.execute(f"create table tb using mt tags(0)")
        tdSql.execute(f"insert into tb values (now-1m, 1, 1)")
        tdSql.execute(f"alter table mt drop column c2")
        tdSql.error(f"insert into tb (ts, c1, c2) values (now, 2, 2)")
        tdSql.execute(f"insert into tb (ts, c1) values (now, 2)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"alter table mt add column c2 int")
        tdSql.execute(f"insert into tb (ts, c2) values (now, 3)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 2, 3)

        ## ALTER TABLE AND IMPORT
        tdSql.execute(f"drop database {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table mt (ts timestamp, c1 int, c2 nchar(7), c3 int) tags (t1 int)"
        )
        tdSql.execute(f"create table tb using mt tags(1)")
        tdSql.execute(
            f"insert into tb values ('2018-11-01 16:30:00.000', 1, 'insert', 1)"
        )
        tdSql.execute(f"alter table mt drop column c3")

        tdSql.execute(f"insert into tb values ('2018-11-01 16:29:59.000', 1, 'insert')")
        tdSql.execute(f"import into tb values ('2018-11-01 16:29:59.000', 1, 'import')")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "insert")

        tdSql.execute(f"alter table mt add column c3 nchar(4)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 3, None)

        tdLog.info(f"============================>TD-3366 TD-3486")
        tdSql.execute(
            f"insert into td_3366(ts, c3, c1) using mt(t1) tags(911) values('2018-1-1 11:11:11', 'new1', 12);"
        )
        tdSql.execute(
            f"insert into td_3486(ts, c3, c1) using mt(t1) tags(-12) values('2018-1-1 11:11:11', 'new1', 12);"
        )
        tdSql.execute(
            f"insert into ttxu(ts, c3, c1) using mt(t1) tags('-121') values('2018-1-1 11:11:11', 'new1', 12);"
        )

        tdSql.execute(
            f"insert into tb(ts, c1, c3) using mt(t1) tags(123) values('2018-11-01 16:29:58.000', 2, 'port')"
        )

        tdSql.execute(
            f"insert into tb values ('2018-11-01 16:29:58.000', 2, 'import', 3)"
        )
        tdSql.execute(
            f"import into tb values ('2018-11-01 16:29:58.000', 2, 'import', 3)"
        )
        tdSql.execute(
            f"import into tb values ('2018-11-01 16:39:58.000', 2, 'import', 3)"
        )
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 3, 3)

        ##### ILLEGAL OPERATIONS

        # try dropping columns that are defined in metric
        tdSql.error(f"alter table tb drop column c1;")

        # try dropping primary key
        tdSql.error(f"alter table mt drop column ts;")

        # try modifying two columns in a single statement
        tdSql.error(f"alter table mt add column c5 nchar(3) c6 nchar(4)")

        # duplicate columns
        tdSql.error(f"alter table mt add column c1 int")

        # drop non-existing columns
        tdSql.error(f"alter table mt drop column c9")

        # merge other cases
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} duration 3 keep 20,20,20")
        tdSql.execute(f"use {db}")

        tdSql.error(f'alter database {db} keep "20"')
        tdSql.error(f'alter database {db} keep "20","20","20"')
        tdSql.error(f"alter database {db} keep 20,19")
        tdSql.error(f"alter database {db} keep 20.0")
        tdSql.error(f"alter database {db} keep 20.0,20.0,20.0")
        tdSql.error(f"alter database {db} keep 0,0,0")
        tdSql.error(f"alter database {db} keep -1,-1,-1")
        tdSql.error(f"alter database {db} keep 8,20")
        tdSql.error(f"alter database {db} keep 8,9,9")
        tdSql.error(f"alter database {db} keep 20,20,19")
        tdSql.error(f"alter database {db} keep 20,19,20")
        tdSql.error(f"alter database {db} keep 20,19,19")
        tdSql.error(f"alter database {db} keep 20,19,18")
        tdSql.error(f"alter database {db} keep 20,20,20,20")
        tdSql.error(f"alter database {db} keep 365001,365001,365001")
        tdSql.execute(f"alter database {db} keep 21")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "21d,21d,21d")

        tdSql.execute(f"alter database {db} keep 11,12")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "11d,12d,12d")

        tdSql.execute(f"alter database {db} keep 20,20,20")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "20d,20d,20d")

        tdSql.execute(f"alter database {db} keep 10,10,10")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "10d,10d,10d")

        tdSql.execute(f"alter database {db} keep 10,10,11")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "10d,10d,11d")

        tdSql.execute(f"alter database {db} keep 11,12,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "11d,12d,13d")

        tdSql.execute(f"alter database {db} keep 365000,365000,365000")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "365000d,365000d,365000d")


    def test_cacheshardbits_create(self):
        """create database with cacheshardbits option

        1. Create database with auto-calculated shards (-1, default)
        2. Create database with explicit shard values (0, 1, 2, 3, 4)
        3. Verify databases are created successfully

        Catalog:
            - Database:Create

        Since: v3.3.6.0

        Labels: common,ci,integration,functional

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info("========== test_cacheshardbits_create")

        # Test with auto-calculated shards (default)
        tdSql.execute("drop database if exists db_auto_shards")
        tdSql.execute("create database db_auto_shards cachemodel 'both' cachesize 16")
        tdSql.query("select * from information_schema.ins_databases where name='db_auto_shards'")
        tdSql.checkRows(1)
        tdLog.info("Database created with auto shards")

        # Test with explicit shard values
        test_cases = [
            (0, "db_shard_0"),   # 1 shard (2^0)
            (1, "db_shard_1"),   # 2 shards (2^1)
            (2, "db_shard_2"),   # 4 shards (2^2)
            (3, "db_shard_3"),   # 8 shards (2^3)
            (4, "db_shard_4"),   # 16 shards (2^4)
        ]

        for shard_bits, dbname in test_cases:
            tdSql.execute(f"drop database if exists {dbname}")
            tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16 cacheshardbits {shard_bits}")
            tdSql.query(f"select * from information_schema.ins_databases where name='{dbname}'")
            tdSql.checkRows(1)
            tdLog.info(f"Database {dbname} created with cacheshardbits={shard_bits}")

        # Cleanup
        tdSql.execute("drop database if exists db_auto_shards")
        for _, dbname in test_cases:
            tdSql.execute(f"drop database if exists {dbname}")


    def test_cacheshardbits_alter(self):
        """alter database cacheshardbits option

        1. Create database with cacheshardbits=1
        2. Create tables and insert data
        3. Query to populate cache
        4. Alter cacheshardbits to 3
        5. Insert more data and verify cache still works

        Catalog:
            - Database:Alter

        Since: v3.3.6.0

        Labels: common,ci,integration,functional

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info("========== test_cacheshardbits_alter")

        dbname = "db_alter_shards"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16 cacheshardbits 1")

        # Create tables and insert data
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(t1 int)")

        for i in range(10):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i}, {i*1.5})")

        # Query to populate cache
        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("Initial last query completed")

        # Alter cacheshardbits
        tdSql.execute(f"alter database {dbname} cacheshardbits 3")
        tdLog.info(f"Altered {dbname} to cacheshardbits=3")

        # Insert more data after alter
        for i in range(10):
            tdSql.execute(f"insert into tb{i} values(now+1s, {i+100}, {(i+100)*1.5})")

        # Query again to verify cache still works
        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("Last query after alter completed")

        # Verify the new values are returned
        tdSql.query("select last(c1) from tb0")
        tdSql.checkData(0, 0, 100)

        # Cleanup
        tdSql.execute(f"drop database if exists {dbname}")


    def test_cacheshardbits_concurrent(self):
        """concurrent access with multiple shards

        1. Create database with cacheshardbits=3 (8 shards)
        2. Create 50 tables with data
        3. Run concurrent queries from multiple threads
        4. Verify all queries succeed

        Catalog:
            - Database:Query

        Since: v3.3.6.0

        Labels: common,integration,functional

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info("========== test_cacheshardbits_concurrent")

        dbname = "db_concurrent_shards"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 32 cacheshardbits 3")

        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float, c3 binary(20)) tags(t1 int)")

        # Create multiple tables
        num_tables = 50
        for i in range(num_tables):
            tdSql.execute(f"create table tb{i} using st tags({i})")

        # Insert data
        for i in range(num_tables):
            for j in range(10):
                tdSql.execute(f"insert into tb{i} values(now+{j}s, {i*100+j}, {(i*100+j)*1.5}, 'data_{i}_{j}')")

        # Concurrent queries
        def query_last_cache(table_id):
            try:
                import taos
                conn = taos.connect()
                cursor = conn.cursor()
                cursor.execute(f"use {dbname}")

                for _ in range(20):
                    cursor.execute(f"select last(*) from tb{table_id}")
                    result = cursor.fetchall()
                    if not result:
                        tdLog.exit(f"Query failed for tb{table_id}")

                cursor.close()
                conn.close()
            except Exception as e:
                tdLog.exit(f"Thread error: {str(e)}")

        # Create threads for concurrent access
        threads = []
        for i in range(10):
            t = threading.Thread(target=query_last_cache, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        tdLog.info("Concurrent access test completed successfully")

        # Cleanup
        tdSql.execute(f"drop database if exists {dbname}")


    def test_cacheshardbits_invalid(self):
        """invalid cacheshardbits values

        1. Test that invalid shard bits (>= 20) are rejected
        2. Test negative values (except -1)
        3. Verify proper error messages

        Catalog:
            - Database:Create

        Since: v3.3.6.0

        Labels: common,ci,integration,functional

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info("========== test_cacheshardbits_invalid")

        # Test invalid values >= 20
        tdSql.error("create database db_invalid1 cacheshardbits 20")
        tdSql.error("create database db_invalid2 cacheshardbits 25")
        tdSql.error("create database db_invalid3 cacheshardbits 100")

        # Test negative values (except -1 which is valid for auto-calculation)
        tdSql.error("create database db_invalid4 cacheshardbits -2")
        tdSql.error("create database db_invalid5 cacheshardbits -10")

        tdLog.info("Invalid values properly rejected")


    def test_cacheshardbits_with_cache_models(self):
        """cacheshardbits with different cache models

        1. Test cacheshardbits with cachemodel 'none'
        2. Test cacheshardbits with cachemodel 'last_row'
        3. Test cacheshardbits with cachemodel 'last_value'
        4. Test cacheshardbits with cachemodel 'both'

        Catalog:
            - Database:Create

        Since: v3.3.6.0

        Labels: common,ci,integration,functional

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info("========== test_cacheshardbits_with_cache_models")

        cache_models = ['none', 'last_row', 'last_value', 'both']

        for model in cache_models:
            dbname = f"db_cache_{model}"
            tdSql.execute(f"drop database if exists {dbname}")
            tdSql.execute(f"create database {dbname} cachemodel '{model}' cachesize 16 cacheshardbits 2")
            tdSql.query(f"select * from information_schema.ins_databases where name='{dbname}'")
            tdSql.checkRows(1)
            tdLog.info(f"Database {dbname} created with cachemodel='{model}' and cacheshardbits=2")

            # Create table and insert data
            tdSql.execute(f"use {dbname}")
            tdSql.execute("create stable st(ts timestamp, c1 int) tags(t1 int)")
            tdSql.execute("create table tb1 using st tags(1)")
            tdSql.execute("insert into tb1 values(now, 100)")

            # Query based on cache model
            if model != 'none':
                tdSql.query("select last(*) from st")
                tdSql.checkRows(1)

            # Cleanup
            tdSql.execute(f"drop database if exists {dbname}")


    def test_cacheshardbits_performance(self):
        """compare performance with different shard counts

        1. Create database with 1 shard
        2. Create database with 4 shards
        3. Insert data and query
        4. Compare basic functionality (not strict performance measurement)

        Catalog:
            - Database:Query

        Since: v3.3.6.0

        Labels: common,integration,functional

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info("========== test_cacheshardbits_performance")

        # Test with 1 shard
        db1 = "db_perf_1shard"
        tdSql.execute(f"drop database if exists {db1}")
        tdSql.execute(f"create database {db1} cachemodel 'both' cachesize 32 cacheshardbits 0")
        tdSql.execute(f"use {db1}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(t1 int)")

        for i in range(100):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i}, {i*1.5})")

        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("1 shard test completed")

        # Test with 4 shards
        db2 = "db_perf_4shards"
        tdSql.execute(f"drop database if exists {db2}")
        tdSql.execute(f"create database {db2} cachemodel 'both' cachesize 32 cacheshardbits 2")
        tdSql.execute(f"use {db2}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(t1 int)")

        for i in range(100):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i}, {i*1.5})")

        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("4 shards test completed")

        # Cleanup
        tdSql.execute(f"drop database if exists {db1}")
        tdSql.execute(f"drop database if exists {db2}")


    #
    # --- util ---
    #

    def wait_compact_finish(self, timeout=60):
        for _ in range(timeout):
            rows = tdSql.query("show compacts")
            if rows == 0:
                return
            time.sleep(1)
        raise Exception("compact did not finish within timeout")


    def alter_maxrows(self, maxrows, timeout=1800):
        sql = f"alter database {self.db} minrows 10 maxrows {maxrows}"
        deadline = time.monotonic() + timeout
        waiting = False
        while True:
            try:
                tdSql.cursor.execute(sql)
                return
            except ProgrammingError as error:
                # Retention may outlive SHOW COMPACTS and temporarily block ALTER.
                if (error.errno & 0xFFFFFFFF) != 0x800003DC or time.monotonic() >= deadline:
                    raise
                if not waiting:
                    tdLog.info(f"Waiting for retention before {sql}")
                    waiting = True
                time.sleep(1)


    def get_block_dist(self, tbname):
        """Parse `show table distributed` text output into (total_blocks, block_max_rows).

        History:
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        tdSql.query(f"show table distributed {tbname}")
        total_blocks = None
        block_max_rows = None
        for row in tdSql.queryResult:
            line = row[0]
            m = re.search(r"Total_Blocks=\[(\d+)\]", line)
            if m:
                total_blocks = int(m.group(1))
            m = re.search(r"MaxRows=\[(\d+)\]", line)
            if m:
                block_max_rows = int(m.group(1))
        if total_blocks is None or block_max_rows is None:
            raise Exception(f"failed to parse 'show table distributed' output: {tdSql.queryResult}")
        return total_blocks, block_max_rows


    def get_fixed_dist_total_blocks(self, tbname):
        tdSql.query(
            f"select * from information_schema.ins_table_fixed_distributed "
            f"where db_name='{self.db}' and table_name='{tbname}'"
        )
        tdSql.checkRows(1)
        return tdSql.queryResult[0][2]


    #
    # --- impl ---
    #

    def prepare(self):
        self.db = "test_alter_maxrows_compact"
        self.tb = "nt"
        self.rows = 1200

        tdSql.execute(f"drop database if exists {self.db}")
        # keep both bounds small so a modest amount of data already spans several
        # blocks, avoiding a large data write just to exercise the feature
        tdSql.execute(f"create database {self.db} vgroups 1 minrows 10 maxrows 400")
        tdSql.execute(f"use {self.db}")
        tdSql.execute(f"create table {self.tb} (ts timestamp, v int)")

        sql = f"insert into {self.tb} values "
        for i in range(self.rows):
            sql += f"(now+{i + 1}s, {i}) "
        tdSql.execute(sql)

        # commit the data to disk so it is visible to compact/block-distribution
        tdSql.execute(f"flush database {self.db}")
        time.sleep(2)

        print("prepare ................................ [ passed ]")


    def do_baseline(self):
        self.baseline_blocks, baseline_max_rows = self.get_block_dist(self.tb)
        tdLog.info(f"baseline: total_blocks={self.baseline_blocks}, block_max_rows={baseline_max_rows}")

        assert baseline_max_rows <= 400, \
            f"baseline block max rows {baseline_max_rows} exceeds created maxrows 400"
        assert self.baseline_blocks == self.get_fixed_dist_total_blocks(self.tb), \
            "show table distributed and ins_table_fixed_distributed disagree on total_blocks"

        print("do baseline ............................ [ passed ]")


    def do_shrink_maxrows_and_compact(self):
        self.alter_maxrows(200)
        tdSql.query(f"select * from information_schema.ins_databases where name='{self.db}'")
        tdSql.checkData(0, 12, '200')

        # no new write happened since the alter, so a plain compact would skip this
        # fileset (tsdbShouldCompact() returns false because lastCompact > lastCommit);
        # FORCE bypasses that skip and rewrites every file in the time range, which is
        # required for the new MAXROWS to actually apply to already-committed data
        tdSql.execute(f"compact database {self.db} force")
        self.wait_compact_finish()

        shrink_blocks, shrink_max_rows = self.get_block_dist(self.tb)
        tdLog.info(f"after shrink: total_blocks={shrink_blocks}, block_max_rows={shrink_max_rows}")

        assert shrink_max_rows <= 200, \
            f"block max rows {shrink_max_rows} exceeds altered maxrows 200"
        assert shrink_blocks > self.baseline_blocks, \
            f"expected more, smaller blocks after shrinking maxrows and force-compacting, " \
            f"got {shrink_blocks} blocks vs baseline {self.baseline_blocks}"
        assert shrink_blocks == self.get_fixed_dist_total_blocks(self.tb), \
            "show table distributed and ins_table_fixed_distributed disagree on total_blocks"

        self.shrink_blocks = shrink_blocks

        print("do shrink maxrows and compact .......... [ passed ]")


    def do_grow_maxrows_and_compact(self):
        self.alter_maxrows(800)
        tdSql.query(f"select * from information_schema.ins_databases where name='{self.db}'")
        tdSql.checkData(0, 12, '800')

        tdSql.execute(f"compact database {self.db} force")
        self.wait_compact_finish()

        grow_blocks, grow_max_rows = self.get_block_dist(self.tb)
        tdLog.info(f"after grow: total_blocks={grow_blocks}, block_max_rows={grow_max_rows}")

        assert grow_max_rows <= 800, \
            f"block max rows {grow_max_rows} exceeds altered maxrows 800"
        assert grow_blocks < self.shrink_blocks, \
            f"expected fewer, larger blocks after growing maxrows and force-compacting, " \
            f"got {grow_blocks} blocks vs {self.shrink_blocks} after the shrink step"
        assert grow_blocks == self.get_fixed_dist_total_blocks(self.tb), \
            "show table distributed and ins_table_fixed_distributed disagree on total_blocks"

        print("do grow maxrows and compact ............ [ passed ]")


    def cleanup(self):
        tdSql.execute(f"drop database {self.db}")


    #
    # --- main ---
    #

    def test_db_alter_maxrows_compact(self):
        """Verify ALTER DATABASE MAXROWS takes effect on existing data via COMPACT ... FORCE

        1. Create a database with a small MAXROWS/MINROWS and write a modest amount of
           data into a single table, then flush it to disk and record the baseline
           block distribution
        2. Shrink MAXROWS (and MINROWS), run COMPACT DATABASE ... FORCE (required since
           there is no new write after the alter, so a plain compact would skip the
           fileset), poll SHOW COMPACTS until the task finishes, then verify via
           SHOW TABLE DISTRIBUTED / ins_table_fixed_distributed that the block count
           increased and no block exceeds the new MAXROWS
        3. Grow MAXROWS beyond the original value and repeat step 2, verifying the
           block count decreases again and blocks stay within the new bound

        Catalog:
            - Database

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-24 Claude Added to verify ALTER DATABASE MAXROWS + COMPACT FORCE
            - 2026-11-01 Wait for transient retention conflicts before ALTER DATABASE
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        self.prepare()
        self.do_baseline()
        self.do_shrink_maxrows_and_compact()
        self.do_grow_maxrows_and_compact()
        self.cleanup()


    def alter_buffer(self):
        tdSql.execute('create database db')
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            tdLog.debug("Skip check points for Linux aarch64 due to environment settings")
        else:
            for buffer in self.buffer_boundary:
                tdSql.execute(f'alter database db buffer {buffer}')
                tdSql.query(
                    'select * from information_schema.ins_databases where name = "db"')
                tdSql.checkEqual(tdSql.queryResult[0][8], buffer)
        tdSql.execute('drop database db')
        tdSql.execute('create database db vgroups 10')
        for buffer in self.buffer_error:
            tdSql.error(f'alter database db buffer {buffer}')
        tdSql.execute('drop database db')


    def alter_pages(self):
        tdSql.execute('create database db')
        for pages in self.pages_boundary:
            tdSql.execute(f'alter database db pages {pages}')
            tdSql.query(
                'select * from information_schema.ins_databases where name = "db"')
            tdSql.checkEqual(tdSql.queryResult[0][10], pages)
        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.query(
            'select * from information_schema.ins_databases where name = "db"')
        # self.pages_error.append(tdSql.queryResult[0][10])
        for pages in self.pages_error:
            tdSql.error(f'alter database db pages {pages}')
        tdSql.execute('drop database db')


    def alter_encrypt_alrogithm(self):
        tdSql.execute('create database db')
        tdSql.checkEqual("Encryption is not allowed to be changed after database is created", tdSql.error('alter database db encrypt_algorithm \'sM4-CBC\''))
        tdSql.execute('alter database db encrypt_algorithm \'noNe\'')
        tdSql.checkEqual("Invalid option encrypt_algorithm", tdSql.error('alter database db encrypt_algorithm \'\''))
        tdSql.checkEqual("Encryption algorithm does not exist in the list", tdSql.error('alter database db encrypt_algorithm \'none \''))
        tdSql.execute('drop database db')


    def alter_same_options(self):
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdSql.query('select * from information_schema.ins_databases where name = "db"')
        
        db_options_items = ["replica","keep","buffer","pages","minrows","cachemodel","cachesize","wal_level","wal_fsync_period",
                      "wal_retention_period","wal_retention_size","stt_trigger", "compact_interval", "compact_time_range", "compact_time_offset"]
        db_options_result_idx = [4,7,8,10,11,18,19,21,22,23,24,25,35,36,37]
        
        self.option_result = []
        for idx in db_options_result_idx:
            self.option_result.append(tdSql.queryResult[0][idx])
        
        index = 0
        for option in db_options_items:
            if option == "cachemodel":
                option_sql = "alter database db %s '%s'" % (option, self.option_result[index] )
            else:
                option_sql = "alter database db %s %s" % (option, self.option_result[index] )
            tdLog.debug(option_sql)
            tdSql.query(option_sql)
            index += 1
        tdSql.execute('drop database db')


    def showCreateDbCheck(self, dbname, expectShowCreateDb, maxRetry=30, dropDbAndRecheck=False, dropDbAfterCheck=False):
        retry_count = 0
        success = False
        while retry_count < maxRetry and not success:
            try:
                tdSql.query(f"show create database {dbname};")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, dbname)
                tdSql.checkEqual(tdSql.queryResult[0][1], expectShowCreateDb)
                tdLog.info(f"check passed after {retry_count} seconds")
                success = True
            except Exception as e:
                retry_count += 1
                tdLog.info(f"Attempt to check {retry_count} time(s): show create database {dbname}")
                if retry_count >= maxRetry:
                    raise Exception(repr(e))
                time.sleep(1)
        if dropDbAndRecheck == True:
            tdSql.execute(f"drop database if exists {dbname}")
            tdSql.execute(expectShowCreateDb)
            tdSql.query(f"show create database {dbname};")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, dbname)
            tdSql.checkData(0, 1, expectShowCreateDb)
        if dropDbAfterCheck == True:
            tdSql.execute(f"drop database if exists {dbname}")


    def alter_keep_time_offset(self):
        default_wal_level = 2 if platform.system() == "Windows" else 1
        tdSql.execute('drop database if exists db')
        tdSql.checkEqual("Invalid option keep_time_offset unit: d, only h allowed", tdSql.error('create database db keep_time_offset 0d'))
        tdSql.checkEqual("syntax error near \"-1\"", tdSql.error('create database db keep_time_offset -1'))
        tdSql.checkEqual("syntax error near \"-100h\"", tdSql.error('create database db keep_time_offset -100h'))
        tdSql.checkEqual("Invalid option keep_time_offset: 24 valid range: [0, 23]", tdSql.error('create database db keep_time_offset 24h'))
        tdSql.execute('create database db keep_time_offset 20h SS_CHUNKPAGES 131072')
        self.showCreateDbCheck('db', f"CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' CACHESHARDBITS -1 COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL {default_wal_level} VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 20 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h IS_AUDIT 0 SECURE_DELETE 0 ALLOW_DROP 1 SECURITY_LEVEL 0", 30, True, False)
        tdSql.checkEqual("Invalid option keep_time_offset unit: d, only h allowed", tdSql.error('alter database db keep_time_offset 0d'))
        tdSql.checkEqual("syntax error near \"-1\"", tdSql.error('alter database db keep_time_offset -1'))
        tdSql.checkEqual("syntax error near \"-100h\"", tdSql.error('alter database db keep_time_offset -100h'))
        tdSql.checkEqual("Invalid option keep_time_offset: 24 valid range: [0, 23]", tdSql.error('alter database db keep_time_offset 24h'))
        
        tdLog.info('alter database db keep_time_offset 23h')
        tdSql.execute('alter database db keep_time_offset 23h')
        self.showCreateDbCheck('db', f"CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' CACHESHARDBITS -1 COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL {default_wal_level} VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 23 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h IS_AUDIT 0 SECURE_DELETE 0 ALLOW_DROP 1 SECURITY_LEVEL 0", 30, True, False)
        tdLog.info('alter database db keep_time_offset 0')
        tdSql.execute('alter database db keep_time_offset 0')
        self.showCreateDbCheck('db', f"CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' CACHESHARDBITS -1 COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL {default_wal_level} VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h IS_AUDIT 0 SECURE_DELETE 0 ALLOW_DROP 1 SECURITY_LEVEL 0", 30, True, True)


    def test_db_alter_database(self):
        """Alter database

        1. Alter database buffer
        2. Alter database pages
        3. Alter database encrypt_algorithm
        4. Alter database with same options
        5. Alter database keep_time_offset


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-09-12 AlexDaun Migrated from uncatelog/system-test/test_alter_database.py
            - 2026-11-20 Alex Merged into 03-Alter/test_db_alter_misc.py so the methods share one taosd deployment (CI deploys once per test class)
 
        """
        self._setup_alterdb()
        # self.alter_buffer()
        self.alter_pages()
        self.alter_encrypt_alrogithm()
        self.alter_same_options()
        self.alter_keep_time_offset()

    def _setup_alterdb(self):
        #tdSql.init(conn.cursor(), logSql)
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql), logSql)
        if platform.system().lower() == 'windows':
            self.buffer_boundary = [3, 16384]
        else:
            self.buffer_boundary = [3]
        # remove the value > free_memory, 70% is the weight to calculate the max value
        # if platform.system() == "Linux" and platform.machine() == "aarch64":
            # mem = psutil.virtual_memory()
            # free_memory = mem.free * 0.7 / 1024 / 1024
            # for item in self.buffer_boundary:
            #     if item > free_memory:
            #         self.buffer_boundary.remove(item)

        self.buffer_error = [self.buffer_boundary[0] -
                             1, self.buffer_boundary[-1]+1]
        # pages_boundary >= 64
        self.pages_boundary = [64, 128, 512]
        self.pages_error = [self.pages_boundary[0]-1]
