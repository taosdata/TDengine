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
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseNameRules:

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


    def test_database_len(self):
        """DB Name: length

        1. Create database with an excessively long name
        2. Test with invalid values

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/len.sim
            - 2026-11-20 Alex Merged into 01-Create/test_db_name_rules.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"=============== step1")
        tdSql.error(f"drop database dd")
        tdSql.error(f"create database ")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create database a")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"drop database a")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"create database a12345678")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"drop database a12345678")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step4")
        tdSql.error(
            f"create database a012345678901201234567890120123456789012a012345678901201234567890120123456789012"
        )

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step5")
        tdSql.execute(f"create database a;1")
        tdSql.execute(f"drop database a")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step6")
        tdSql.error(f"create database a'1")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step7")
        tdSql.error(f"create database (a)")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step8")
        tdSql.error(f"create database a.1")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


    def test_database_create(self):
        """DB Name: basic

        1. Case sensitivity
        2. Illegal names
        3. Chinese names

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/create_db.sim
            - 2026-11-20 Alex Merged into 01-Create/test_db_name_rules.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
        mtPrefix = "fi_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"excuting test script create_db.sim")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.error(f"createdatabase {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.checkRows(3)
        tdSql.checkData(2, 0, db)
        tdSql.execute(f"drop database {db}")

        # case1: case_insensitivity test
        tdLog.info(f"=========== create_db.sim case1: case insensitivity test")
        tdSql.error(f"CREATEDATABASE {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.checkRows(3)
        tdSql.checkData(2, 0, db)

        tdSql.execute(f"drop database {db}")
        tdLog.info(f"case_insensitivity test passed")

        # case2: illegal_db_name test
        tdLog.info(f"=========== create_db.sim case2: illegal_db_name test")
        illegal_db1 = "1db"
        illegal_db2 = "d@b"

        tdSql.error(f"create database {illegal_db1}")
        tdSql.error(f"create database {illegal_db2}")
        tdLog.info(f"illegal_db_name test passed")

        # case3: chinese_char_in_db_name test
        tdLog.info(f"========== create_db.sim case3: chinese_char_in_db_name test")
        CN_db1 = "数据库"
        CN_db2 = "数据库1"
        CN_db3 = "db数据库1"
        tdSql.error(f"create database {CN_db1}")
        tdSql.error(f"create database {CN_db2}")
        tdSql.error(f"create database {CN_db3}")
        # sql select * from information_schema.ins_databases
        # if $rows != 3 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != $CN_db1 then
        #  return -1
        # endi
        # if $tdSql.getData(1,0) != $CN_db2 then
        #  return -1
        # endi
        # if $tdSql.getData(2,0) != $CN_db3 then
        #  return -1
        # endi
        # sql drop database $CN_db1
        # sql drop database $CN_db2
        # sql drop database $CN_db3
        tdLog.info(f"case_chinese_char_in_db_name test passed")

        # case4: db_already_exists
        tdLog.info(f"create_db.sim case4: db_already_exists")
        tdSql.execute(f"create database db0")
        tdSql.error(f"create database db0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"drop database db0")
        tdLog.info(f"db_already_exists test passed")

        # case5: db_meta_data
        tdLog.info(f"create_db.sim case5: db_meta_data test")
        # cfg params
        replica = 1  # "max"=3
        duration = 10
        keep = "365,365,365"
        rows_db = 1000
        cache = 16  # "16MB"
        ablocks = 100
        tblocks = 32  # "max"=512, "automatically" "trimmed" "when" "exceeding"
        ctime = 36000  # 10 "hours"
        wal = 1  # "valid" "value" "is" 1, 2
        comp = 1  # "max"=32, "automatically" "trimmed" "when" "exceeding"

        tdSql.execute(
            f"create database {db} replica {replica} duration {duration} keep {keep} maxrows {rows_db} wal_level {wal} comp {comp}"
        )
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, db)
        tdSql.checkData(2, 4, replica)
        tdSql.checkData(2, 6, "10d")
        tdSql.checkData(2, 7, "365d,365d,365d")

        tdSql.execute(f"drop database {db}")

        ## param range tests
        # replica [1,3]
        # sql_error create database $db replica 0
        tdSql.error(f"create database {db} replica 4")

        # day [1, 3650]
        tdSql.error(f"create database {db} day 0")
        tdSql.error(f"create database {db} day 3651")

        # keep [1, infinity]
        tdSql.error(f"create database {db} keep 0")
        tdSql.error(f"create database {db} keep 0,0,0")
        tdSql.error(f"create database {db} keep 3,3,3")
        tdSql.error(f"create database {db} keep 11.0")
        tdSql.error(f"create database {db} keep 11.0,11.0,11.0")
        tdSql.error(f'create database {db} keep "11","11","11"')
        tdSql.error(f'create database {db} keep "11"')
        tdSql.error(f"create database {db} keep 13,12,11")
        tdSql.error(f"create database {db} keep 11,12,11")
        tdSql.error(f"create database {db} keep 12,11,12")
        tdSql.error(f"create database {db} keep 8")
        tdSql.error(f"create database {db} keep 12,11")
        tdSql.error(f"create database {db} keep 365001,365001,365001")
        tdSql.execute(f"create database dbk0 keep 39")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "39d,39d,39d")

        tdSql.execute(f"drop database dbk0")
        tdSql.execute(f"create database dbka keep 39,40")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "39d,40d,40d")

        tdSql.execute(f"drop database dbka")

        tdSql.execute(f"create database dbk1 duration 3 keep 11,11,11")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,11d,11d")

        tdSql.execute(f"drop database dbk1")
        tdSql.execute(f"create database dbk2 duration 3 keep 11,12,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,12d,13d")

        tdSql.execute(f"drop database dbk2")
        tdSql.execute(f"create database dbk3 duration 3 keep 11,11,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,11d,13d")

        tdSql.execute(f"drop database dbk3")
        tdSql.execute(f"create database dbk4 duration 3 keep 11,13,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,13d,13d")

        tdSql.execute(f"drop database dbk4")
        # sql_error create database $db keep 3651

        # rows [200, 10000]
        tdSql.error(f"create database {db} maxrows 199")
        # sql_error create database $db maxrows 10001

        # cache [100, 10485760]
        tdSql.error(f"create database {db} cache 0")
        # sql_error create database $db cache 10485761

        # blocks [32, 4096 overwriten by 4096 if exceeds, Note added:2018-10-24]
        # sql_error create database $db tblocks 31
        # sql_error create database $db tblocks 4097

        # ctime [30, 40960]
        tdSql.error(f"create database {db} ctime 29")
        tdSql.error(f"create database {db} ctime 40961")

        # wal {0, 2}
        # sql_error create database testwal wal_level 0
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdSql.execute(f"create database testwal wal_level 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query(f"select * from information_schema.ins_databases")
        # tdLog.info(f"wallevel {data20_testwal}")
        # if data20_testwal != 1 "":

        tdSql.execute(f"drop database testwal")

        tdSql.execute(f"create database testwal wal_level 2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        # tdLog.info(f"wallevel {data13_testwal}")
        # if data13_testwal != 2 "":

        tdSql.execute(f"drop database testwal")

        tdSql.error(f"create database {db} wal_level -1")
        tdSql.error(f"create database {db} wal_level 3")

        # comp {0, 1, 2}
        tdSql.error(f"create database {db} comp -1")
        tdSql.error(f"create database {db} comp 3")

        tdSql.error(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


    def test_database_table_prefix_suffix(self):
        """Options: table prefix & suffix

        1. Create database with TABLE_PREFIX and TABLE_SUFFIX options
        2. Create tables
        3. Verify that tables are distributed across vgroups as expected

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/table_prefix_suffix.sim
            - 2026-11-20 Alex Merged into 01-Create/test_db_name_rules.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 5 TABLE_PREFIX 1 TABLE_SUFFIX 2;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table atb1aa (ts timestamp, f1 int);")
        tdSql.execute(f"create table btb1bb (ts timestamp, f1 int);")
        tdSql.execute(f"create table ctb1cc (ts timestamp, f1 int);")
        tdSql.execute(f"create table dtb1dd (ts timestamp, f1 int);")
        tdSql.execute(f"create table atb2aa (ts timestamp, f1 int);")
        tdSql.execute(f"create table btb2bb (ts timestamp, f1 int);")
        tdSql.execute(f"create table ctb2cc (ts timestamp, f1 int);")
        tdSql.execute(f"create table dtb2dd (ts timestamp, f1 int);")
        tdSql.execute(f"create table etb2ee (ts timestamp, f1 int);")
        tdSql.query(f"show create database db1;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db1' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db1;")

        tdSql.execute(f"drop database if exists db2;")
        tdSql.execute(f"create database db2 vgroups 5 TABLE_PREFIX -1 TABLE_SUFFIX -2;")
        tdSql.execute(f"use db2;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table teee22 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db2;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db2' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db2;")

        tdSql.execute(f"drop database if exists db3;")
        tdSql.execute(f"create database db3 vgroups 5 TABLE_PREFIX -1;")
        tdSql.execute(f"use db3;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zaaa22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zccc22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zddd22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zeee22 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db3;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db3' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db3;")

        tdSql.execute(f"drop database if exists db4;")
        tdSql.execute(f"create database db4 vgroups 5 TABLE_SUFFIX -2;")
        tdSql.execute(f"use db4;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zaaa22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zccc22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zddd22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table zeee22 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db4;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db4' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db4;")

        tdSql.execute(f"drop database if exists db5;")
        tdSql.execute(f"create database db5 vgroups 5 TABLE_PREFIX 1;")
        tdSql.execute(f"use db5;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table baaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table caaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table daaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table faaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table gbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table hbbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table ibbb11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table jbbb11 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db5;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db5' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db5;")

        tdSql.execute(f"drop database if exists db6;")
        tdSql.execute(f"create database db6 vgroups 5 TABLE_SUFFIX 2;")
        tdSql.execute(f"use db6;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa12 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa13 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa14 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb23 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb24 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb31 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb32 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb33 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db6;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db6' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdSql.execute(f"drop database if exists db6;")

        tdSql.execute(f"drop database if exists db7;")
        tdSql.execute(
            f"create database db7 vgroups 5 TABLE_PREFIX -100 TABLE_SUFFIX -92;"
        )
        tdSql.execute(f"use db7;")
        tdSql.execute(f"create table taaa11 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa12 (ts timestamp, f1 int);")
        tdSql.execute(f"create table taaa13 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb21 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb22 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb23 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tbbb24 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc31 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc32 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tccc33 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd24 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd31 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd32 (ts timestamp, f1 int);")
        tdSql.execute(f"create table tddd33 (ts timestamp, f1 int);")
        tdSql.query(f"show create database db7;")
        tdSql.query(
            f"select count(*) a from information_schema.ins_tables where db_name='db7' group by vgroup_id having(count(*) > 0) order by a;"
        )
        tdSql.execute(f"drop database if exists db7;")

        tdSql.error(f"create database db8 vgroups 5 TABLE_PREFIX -1 TABLE_SUFFIX 2;")
        tdSql.error(f"create database db8 vgroups 5 TABLE_PREFIX 191 TABLE_SUFFIX 192;")
        tdSql.error(
            f"create database db8 vgroups 5 TABLE_PREFIX -192 TABLE_SUFFIX -191;"
        )
        tdSql.error(f"create database db8 vgroups 5 TABLE_PREFIX 100 TABLE_SUFFIX 92;")


