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


class TestNormalTableAlter:

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


    def test_normal_table_alter_1(self):
        """Alter: then desc

        1. ALTER TABLE … ADD/DROP/MODIFY columns
        2. DESCRIBE each change to confirm
        3. Restart the database
        4. Resume ADD/DROP/MODIFY operations and verify again


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/alter/table.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_alter.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("d1", drop=True)
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.execute(f"insert into tb values(now-27d, -27)")
        tdSql.execute(f"insert into tb values(now-26d, -26)")
        tdSql.query(f"select * from tb")
        tdSql.checkRows(3)

        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")

        tdLog.info(f"======== step2")
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")

        tdLog.info(f"======== step3")
        tdSql.execute(f"alter table tb add column c tinyint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")
        tdSql.checkData(3, 0, "c")
        tdSql.checkData(3, 1, "TINYINT")

        tdLog.info(f"======== step4")
        tdSql.execute(f"alter table tb add column d int")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")
        tdSql.checkData(3, 0, "c")
        tdSql.checkData(3, 1, "TINYINT")
        tdSql.checkData(4, 0, "d")
        tdSql.checkData(4, 1, "INT")

        tdLog.info(f"======== step5")
        tdSql.execute(f"alter table tb add column e bigint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")
        tdSql.checkData(3, 0, "c")
        tdSql.checkData(3, 1, "TINYINT")
        tdSql.checkData(4, 0, "d")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(5, 0, "e")
        tdSql.checkData(5, 1, "BIGINT")

        tdLog.info(f"======== step6")
        tdSql.execute(f"alter table tb add column f float")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")
        tdSql.checkData(3, 0, "c")
        tdSql.checkData(3, 1, "TINYINT")
        tdSql.checkData(4, 0, "d")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(5, 0, "e")
        tdSql.checkData(5, 1, "BIGINT")
        tdSql.checkData(6, 0, "f")
        tdSql.checkData(6, 1, "FLOAT")

        tdLog.info(f"======== step7")
        tdSql.execute(f"alter table tb add column g double")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")
        tdSql.checkData(3, 0, "c")
        tdSql.checkData(3, 1, "TINYINT")
        tdSql.checkData(4, 0, "d")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(5, 0, "e")
        tdSql.checkData(5, 1, "BIGINT")
        tdSql.checkData(6, 0, "f")
        tdSql.checkData(6, 1, "FLOAT")
        tdSql.checkData(7, 0, "g")
        tdSql.checkData(7, 1, "DOUBLE")

        tdLog.info(f"======== step8")
        tdSql.execute(f"alter table tb add column h binary(10)")
        tdSql.query(f"select * from tb")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")
        tdSql.checkData(3, 0, "c")
        tdSql.checkData(3, 1, "TINYINT")
        tdSql.checkData(4, 0, "d")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(5, 0, "e")
        tdSql.checkData(5, 1, "BIGINT")
        tdSql.checkData(6, 0, "f")
        tdSql.checkData(6, 1, "FLOAT")
        tdSql.checkData(7, 0, "g")
        tdSql.checkData(7, 1, "DOUBLE")
        tdSql.checkData(8, 0, "h")
        tdSql.checkData(8, 1, "VARCHAR")
        tdSql.checkData(8, 2, 10)

        tdLog.info(f"======== step9")
        tdLog.info(f"======== step10")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use d1")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(2, 1, "SMALLINT")
        tdSql.checkData(3, 0, "c")
        tdSql.checkData(3, 1, "TINYINT")
        tdSql.checkData(4, 0, "d")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(5, 0, "e")
        tdSql.checkData(5, 1, "BIGINT")
        tdSql.checkData(6, 0, "f")
        tdSql.checkData(6, 1, "FLOAT")
        tdSql.checkData(7, 0, "g")
        tdSql.checkData(7, 1, "DOUBLE")
        tdSql.checkData(8, 0, "h")
        tdSql.checkData(8, 1, "VARCHAR")
        tdSql.checkData(8, 2, 10)

        tdLog.info(f"======== step11")
        tdSql.error(f"alter table drop column a")
        tdSql.error(f"alter table drop column ts")
        tdSql.error(f"alter table drop column cdfg")
        tdSql.error(f"alter table add column a")
        tdSql.error(f"alter table add column b")

        tdLog.info(f"======== step12")
        tdSql.execute(f"alter table tb drop column b")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "c")
        tdSql.checkData(2, 1, "TINYINT")
        tdSql.checkData(3, 0, "d")
        tdSql.checkData(3, 1, "INT")
        tdSql.checkData(4, 0, "e")
        tdSql.checkData(4, 1, "BIGINT")
        tdSql.checkData(5, 0, "f")
        tdSql.checkData(5, 1, "FLOAT")
        tdSql.checkData(6, 0, "g")
        tdSql.checkData(6, 1, "DOUBLE")
        tdSql.checkData(7, 0, "h")
        tdSql.checkData(7, 1, "VARCHAR")
        tdSql.checkData(7, 2, 10)

        tdLog.info(f"======== step13")
        tdSql.execute(f"alter table tb drop column c")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "d")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "e")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.checkData(4, 0, "f")
        tdSql.checkData(4, 1, "FLOAT")
        tdSql.checkData(5, 0, "g")
        tdSql.checkData(5, 1, "DOUBLE")
        tdSql.checkData(6, 0, "h")
        tdSql.checkData(6, 1, "VARCHAR")
        tdSql.checkData(6, 2, 10)

        tdLog.info(f"======== step14")
        tdSql.execute(f"alter table tb drop column d")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "e")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 0, "f")
        tdSql.checkData(3, 1, "FLOAT")
        tdSql.checkData(4, 0, "g")
        tdSql.checkData(4, 1, "DOUBLE")
        tdSql.checkData(5, 0, "h")
        tdSql.checkData(5, 1, "VARCHAR")
        tdSql.checkData(5, 2, 10)

        tdLog.info(f"======== step15")
        tdSql.execute(f"alter table tb drop column e")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "f")
        tdSql.checkData(2, 1, "FLOAT")
        tdSql.checkData(3, 0, "g")
        tdSql.checkData(3, 1, "DOUBLE")
        tdSql.checkData(4, 0, "h")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 10)

        tdLog.info(f"======== step16")
        tdSql.execute(f"alter table tb drop column f")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "g")
        tdSql.checkData(2, 1, "DOUBLE")
        tdSql.checkData(3, 0, "h")
        tdSql.checkData(3, 1, "VARCHAR")
        tdSql.checkData(3, 2, 10)

        tdLog.info(f"======== step17")
        tdSql.execute(f"alter table tb drop column g")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 0, "h")
        tdSql.checkData(2, 1, "VARCHAR")
        tdSql.checkData(2, 2, 10)

        tdLog.info(f"============= step18")
        tdSql.execute(f"alter table tb drop column h")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")

        tdLog.info(f"=============== error for normal table")
        tdSql.execute(f"create table tb2023(ts timestamp, f int);")
        tdSql.error(f"alter table tb2023 add column v varchar(65518);")
        tdSql.error(f"alter table tb2023 add column v varchar(65531);")
        tdSql.error(f"alter table tb2023 add column v varchar(65535);")
        tdSql.execute(f"alter table tb2023 add column v varchar(65517);")
        tdSql.error(f"alter table tb2023 modify column v varchar(65518);")
        tdSql.query(f"desc tb2023")
        tdSql.execute(f"alter table tb2023 drop column v")
        tdSql.error(f"alter table tb2023 add column v nchar(16380);")
        tdSql.execute(f"alter table tb2023 add column v nchar(16379);")
        tdSql.error(f"alter table tb2023 modify column v nchar(16380);")
        tdSql.query(f"desc tb2023")

        tdLog.info(f"=============== modify column for normal table")
        tdSql.execute(f"create table ntb_ts3841(ts timestamp, c0 varchar(64000));")
        tdSql.execute(f"alter table ntb_ts3841 modify column c0 varchar(64001);")
        tdSql.execute(f"create table ntb1_ts3841(ts timestamp, c0 nchar(15000));")
        tdSql.execute(f"alter table ntb1_ts3841 modify column c0 nchar(15001);")

        tdLog.info(f"=============== error for super table")
        tdSql.execute(f"create table stb2023(ts timestamp, f int) tags(t1 int);")
        tdSql.error(f"alter table stb2023 add column v varchar(65518);")
        tdSql.error(f"alter table stb2023 add column v varchar(65531);")
        tdSql.error(f"alter table stb2023 add column v varchar(65535);")
        tdSql.execute(f"alter table stb2023 add column v varchar(65517);")
        tdSql.error(f"alter table stb2023 modify column v varchar(65518);")
        tdSql.query(f"desc stb2023")
        tdSql.execute(f"alter table stb2023 drop column v")
        tdSql.error(f"alter table stb2023 add column v nchar(16380);")
        tdSql.execute(f"alter table stb2023 add column v nchar(16379);")
        tdSql.error(f"alter table stb2023 modify column v nchar(16380);")
        tdSql.query(f"desc stb2023")

        tdLog.info(f"=============== modify column/tag for super table")
        tdSql.execute(
            f"create table stb_ts3841(ts timestamp, c0 varchar(64000)) tags(t1 binary(16380));"
        )
        tdSql.execute(f"alter table stb_ts3841 modify column c0 varchar(64001);")
        tdSql.execute(f"alter table stb_ts3841 modify tag t1 binary(16381);")
        tdSql.execute(f"alter table stb_ts3841 modify tag t1 binary(16382);")
        tdSql.error(f"alter table stb_ts3841 modify tag t1 binary(16383);")

        tdSql.execute(
            f"create table stb1_ts3841(ts timestamp, c0 nchar(15000)) tags(t1 nchar(4093));"
        )
        tdSql.execute(f"alter table stb1_ts3841 modify column c0 nchar(15001);")
        tdSql.execute(f"alter table stb1_ts3841 modify tag t1 nchar(4094);")
        tdSql.execute(f"alter table stb1_ts3841 modify tag t1 nchar(4095);")
        tdSql.error(f"alter table stb1_ts3841 modify tag t1 nchar(4096);")
        tdSql.error(f"alter table stb1_ts3841 modify tag t1 binary(16382);")

        tdLog.info(f"======= over")
        tdSql.execute(f"drop database d1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


    def test_normal_table_alter_2(self):
        """Alter: then insert

        1. Execute ADD COLUMN, DROP COLUMN, MODIFY COLUMN operations.
        2. Insert data and run SELECT COUNT queries to verify.
        3. Restart the database.
        4. Continue modifying columns and verify the changes.


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/stable/alter_count.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_alter.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"========= start dnode1 as master")

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database d1 replica 1 duration 7 keep 50")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.execute(f"insert into tb values(now-27d, -27)")
        tdSql.execute(f"insert into tb values(now-26d, -26)")
        tdSql.query(f"select count(a) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step2")
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.execute(f"insert into tb values(now-25d, -25, 0)")
        tdSql.execute(f"insert into tb values(now-24d, -24, 1)")
        tdSql.execute(f"insert into tb values(now-23d, -23, 2)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step3")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint)
        tdSql.execute(f"alter table tb add column c tinyint")
        tdSql.execute(f"insert into tb values(now-22d, -22, 3, 0)")
        tdSql.execute(f"insert into tb values(now-21d, -21, 4, 1)")
        tdSql.execute(f"insert into tb values(now-20d, -20, 5, 2)")
        tdSql.query(f"select count(c) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step4")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int)
        tdSql.execute(f"alter table tb add column d int")
        tdSql.execute(f"insert into tb values(now-19d, -19, 6, 0, 0)")
        tdSql.execute(f"insert into tb values(now-18d, -18, 7, 1, 1)")
        tdSql.execute(f"insert into tb values(now-17d, -17, 8, 2, 2)")
        tdSql.query(f"select count(d) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step5")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint)
        tdSql.execute(f"alter table tb add column e bigint")
        tdSql.execute(f"insert into tb values(now-16d, -16, 9, 0, 0, 0)")
        tdSql.execute(f"insert into tb values(now-15d, -15, 10, 1, 1, 1)")
        tdSql.execute(f"insert into tb values(now-14d, -14, 11, 2, 2, 2)")
        tdSql.query(f"select count(e) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step6")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float)
        tdSql.execute(f"alter table tb add column f float")
        tdSql.execute(f"insert into tb values(now-13d, -13, 12, 0, 0, 0, 0)")
        tdSql.execute(f"insert into tb values(now-12d, -12, 13, 1, 1, 1, 1)")
        tdSql.execute(f"insert into tb values(now-11d, -11, 24, 2, 2, 2, 2)")
        tdSql.query(f"select count(f) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step7")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float, g double)
        tdSql.execute(f"alter table tb add column g double")
        tdSql.execute(f"insert into tb values(now-10d, -10, 15, 0, 0, 0, 0, 0)")
        tdSql.execute(f"insert into tb values(now-9d, -9, 16, 1, 1, 1, 1, 1)")
        tdSql.execute(f"insert into tb values(now-8d, -8, 17, 2, 2, 2, 2, 2)")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step8")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float, g double, h binary(10) )
        tdSql.execute(f"alter table tb add column h binary(10)")
        tdSql.execute(f"insert into tb values(now-7d, -7, 18, 0, 0, 0, 0, 0, '0')")
        tdSql.execute(f"insert into tb values(now-6d, -6, 19, 1, 1, 1, 1, 1, '1')")
        tdSql.execute(f"insert into tb values(now-5d, -5, 20, 2, 2, 2, 2, 2, '2')")
        tdSql.query(f"select count(h) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step9")
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 0, 24)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 1, 21)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 2, 18)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 3, 15)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 4, 12)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 5, 9)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 6, 6)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 7, 3)

        tdLog.info(f"============= step10")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from d1.tb;"
        )
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from d1.tb;"
        )

        tdSql.execute(f"use d1")
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 0, 24)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 1, 21)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 2, 18)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 3, 15)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 4, 12)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 5, 9)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 6, 6)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 7, 3)

        tdLog.info(f"======== step11")
        # sql alter table tb(ts timestamp, b smallint, c tinyint, d int, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column a")
        tdSql.execute(f"insert into tb values(now-4d, 1, 1, 1, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 7)

        tdLog.info(f"======== step12")
        # sql alter table tb(ts timestamp, c tinyint, d int, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column b")
        tdSql.execute(f"insert into tb values(now-3d, 1, 1, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 8)

        tdLog.info(f"======== step13")
        # sql alter table tb(ts timestamp, d int, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column c")
        tdSql.execute(f"insert into tb values(now-2d, 1, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 9)

        tdLog.info(f"======== step14")
        # sql alter table tb(ts timestamp, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column d")
        tdSql.execute(f"insert into tb values(now-1d, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 10)

        tdLog.info(f"======== step15")
        # sql alter table tb(ts timestamp, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column e")
        tdSql.execute(f"insert into tb values(now, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 11)

        tdLog.info(f"======== step16")
        # sql alter table tb(ts timestamp, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column f")
        tdSql.execute(f"insert into tb values(now+1d, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"======== step17")
        # sql alter table tb(ts timestamp, h binary(20))
        tdSql.execute(f"alter table tb drop column g")
        tdSql.execute(f"insert into tb values(now+2d, '1')")
        tdSql.query(f"select count(h) from tb")
        tdSql.checkData(0, 0, 10)

        tdLog.info(f"============== step18")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        # sql select count(g) from tb
        # if $tdSql.getData(0,0) != 12 then
        #  return -1
        # endi
        tdSql.query(f"select count(*) from tb")
        tdSql.checkData(0, 0, 31)


    def test_normal_table_alter_3(self):
        """Alter: import old data

        1. Add column
        2. Insert out-of-order data
        3. Query data

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/stable/alter_import.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_alter.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database d1 replica 1 duration 7 keep 50")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-30d, -28)")
        tdSql.execute(f"insert into tb values(now-27d, -27)")
        tdSql.execute(f"insert into tb values(now-26d, -26)")
        tdSql.query(f"select count(a) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step2")
        # sql alter table tb(ts timestamp, a int, b smallint)
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.execute(f"insert into tb values(now-25d, -25, 0)")
        tdSql.execute(f"insert into tb values(now-22d, -24, 1)")
        tdSql.execute(f"insert into tb values(now-20d, -23, 2)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"========= step3")
        tdSql.execute(f"insert into tb values(now-23d, -23, 0)")
        tdSql.execute(f"insert into tb values(now-21d, -21, 0)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 5)

        tdSql.execute(f"insert into tb values(now-29d, -29, 0)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 6)


    def test_normal_table_alter_4(self):
        """Alter: repeatedly add

        1. Add column
        2. Insert data
        3. Project query
        4. Loop for 7 times
        5. Kill then restart

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/stable/alter_insert1.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_alter.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("d3", drop=True)
        tdSql.execute(f"use d3")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.query(f"select * from tb order by ts desc")

        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -28)

        tdLog.info(f"======== step2")
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.execute(f"insert into tb values(now-25d, -25, 0)")
        tdSql.query(f"select * from tb order by ts desc")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, -25)
        tdSql.checkData(1, 1, -28)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, None)

        tdLog.info(f"======== step3")
        tdSql.execute(f"alter table tb add column c tinyint")
        tdSql.execute(f"insert into tb values(now-22d, -22, 3, 0)")
        tdSql.query(f"select * from tb order by ts asc")

        tdSql.checkRows(3)
        tdSql.checkData(0, 1, -28)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 1, -25)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(2, 1, -22)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 0)

        tdLog.info(f"======== step4")
        tdSql.execute(f"alter table tb add column d int")
        tdSql.execute(f"insert into tb values(now-19d, -19, 6, 3, 0)")
        tdSql.query(f"select * from tb order by ts asc")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, -28)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(1, 1, -25)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(2, 1, -22)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(3, 1, -19)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 0)

        tdLog.info(f"======== step5")
        tdSql.execute(f"alter table tb add column e bigint")
        tdSql.execute(f"insert into tb values(now-16d, -16, 9, 5, 4, 3)")
        tdSql.query(f"select count(e) from tb")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(c) from tb")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select * from tb order by ts asc")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, -28)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(1, 1, -25)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(1, 5, None)
        tdSql.checkData(2, 1, -22)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(2, 5, None)
        tdSql.checkData(3, 1, -19)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(3, 5, None)
        tdSql.checkData(4, 1, -16)
        tdSql.checkData(4, 2, 9)
        tdSql.checkData(4, 3, 5)
        tdSql.checkData(4, 4, 4)
        tdSql.checkData(4, 5, 3)

        tdLog.info(f"======== step6")
        tdSql.execute(f"alter table tb add column f float")
        tdSql.execute(f"insert into tb values(now-13d, -13, 12, 11, 10, 9, 8)")
        tdSql.query(f"select * from tb order by ts asc")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, -28)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(0, 6, None)

        tdSql.checkData(1, 1, -25)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(1, 5, None)
        tdSql.checkData(1, 6, None)

        tdSql.checkData(2, 1, -22)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(2, 5, None)
        tdSql.checkData(2, 6, None)

        tdSql.checkData(3, 1, -19)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(3, 5, None)
        tdSql.checkData(3, 6, None)

        tdSql.checkData(4, 1, -16)
        tdSql.checkData(4, 2, 9)
        tdSql.checkData(4, 3, 5)
        tdSql.checkData(4, 4, 4)
        tdSql.checkData(4, 5, 3)
        tdSql.checkData(4, 6, None)

        tdSql.checkData(5, 1, -13)
        tdSql.checkData(5, 2, 12)
        tdSql.checkData(5, 3, 11)
        tdSql.checkData(5, 4, 10)
        tdSql.checkData(5, 5, 9)
        tdSql.checkData(5, 6, 8.00000)

        tdLog.info(f"======== step7")
        tdSql.execute(f"alter table tb add column g double")
        tdSql.execute(f"insert into tb values(now-10d, -10, 15, 14, 13, 12, 11, 10)")
        tdSql.query(f"select * from tb order by ts asc")
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, -28)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(0, 6, None)
        tdSql.checkData(0, 7, None)

        tdSql.checkData(1, 1, -25)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(1, 5, None)
        tdSql.checkData(1, 6, None)
        tdSql.checkData(1, 7, None)

        tdSql.checkData(2, 1, -22)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(2, 5, None)
        tdSql.checkData(2, 6, None)
        tdSql.checkData(2, 7, None)

        tdSql.checkData(3, 1, -19)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(3, 5, None)
        tdSql.checkData(3, 6, None)
        tdSql.checkData(3, 7, None)

        tdSql.checkData(4, 1, -16)
        tdSql.checkData(4, 2, 9)
        tdSql.checkData(4, 3, 5)
        tdSql.checkData(4, 4, 4)
        tdSql.checkData(4, 5, 3)
        tdSql.checkData(4, 6, None)
        tdSql.checkData(4, 7, None)

        tdSql.checkData(5, 1, -13)
        tdSql.checkData(5, 2, 12)
        tdSql.checkData(5, 3, 11)
        tdSql.checkData(5, 4, 10)
        tdSql.checkData(5, 5, 9)
        tdSql.checkData(5, 6, 8.00000)
        tdSql.checkData(5, 7, None)

        tdSql.checkData(6, 1, -10)
        tdSql.checkData(6, 2, 15)
        tdSql.checkData(6, 3, 14)
        tdSql.checkData(6, 4, 13)
        tdSql.checkData(6, 5, 12)
        tdSql.checkData(6, 6, 11.00000)
        tdSql.checkData(6, 7, 10.000000000)

        tdLog.info(f"======== step8")
        tdSql.execute(f"alter table tb add column h binary(10)")
        tdSql.execute(
            f"insert into tb values(now-7d, -7, 18, 17, 16, 15, 14, 13, '11')"
        )
        tdSql.query(f"select * from tb order by ts asc")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, -28)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(0, 6, None)
        tdSql.checkData(0, 7, None)
        tdSql.checkData(0, 8, None)

        tdSql.checkData(1, 1, -25)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(1, 5, None)
        tdSql.checkData(1, 6, None)
        tdSql.checkData(1, 7, None)
        tdSql.checkData(1, 8, None)

        tdSql.checkData(2, 1, -22)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(2, 5, None)
        tdSql.checkData(2, 6, None)
        tdSql.checkData(2, 7, None)
        tdSql.checkData(2, 8, None)

        tdSql.checkData(3, 1, -19)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(3, 5, None)
        tdSql.checkData(3, 6, None)
        tdSql.checkData(3, 7, None)
        tdSql.checkData(3, 8, None)

        tdSql.checkData(4, 1, -16)
        tdSql.checkData(4, 2, 9)
        tdSql.checkData(4, 3, 5)
        tdSql.checkData(4, 4, 4)
        tdSql.checkData(4, 5, 3)
        tdSql.checkData(4, 6, None)
        tdSql.checkData(4, 7, None)
        tdSql.checkData(4, 8, None)

        tdSql.checkData(5, 1, -13)
        tdSql.checkData(5, 2, 12)
        tdSql.checkData(5, 3, 11)
        tdSql.checkData(5, 4, 10)
        tdSql.checkData(5, 5, 9)
        tdSql.checkData(5, 6, 8.00000)
        tdSql.checkData(5, 7, None)
        tdSql.checkData(5, 8, None)

        tdSql.checkData(6, 1, -10)
        tdSql.checkData(6, 2, 15)
        tdSql.checkData(6, 3, 14)
        tdSql.checkData(6, 4, 13)
        tdSql.checkData(6, 5, 12)
        tdSql.checkData(6, 6, 11.00000)
        tdSql.checkData(6, 7, 10.000000000)
        tdSql.checkData(6, 8, None)

        tdSql.checkData(7, 1, -7)
        tdSql.checkData(7, 2, 18)
        tdSql.checkData(7, 3, 17)
        tdSql.checkData(7, 4, 16)
        tdSql.checkData(7, 5, 15)
        tdSql.checkData(7, 6, 14.00000)
        tdSql.checkData(7, 7, 13.000000000)
        tdSql.checkData(7, 8, 11)

        tdLog.info(f"======== step9")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from tb order by ts asc")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, -28)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(0, 6, None)
        tdSql.checkData(0, 7, None)
        tdSql.checkData(0, 8, None)

        tdSql.checkData(1, 1, -25)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(1, 5, None)
        tdSql.checkData(1, 6, None)
        tdSql.checkData(1, 7, None)
        tdSql.checkData(1, 8, None)

        tdSql.checkData(2, 1, -22)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(2, 5, None)
        tdSql.checkData(2, 6, None)
        tdSql.checkData(2, 7, None)
        tdSql.checkData(2, 8, None)

        tdSql.checkData(3, 1, -19)
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 0)
        tdSql.checkData(3, 5, None)
        tdSql.checkData(3, 6, None)
        tdSql.checkData(3, 7, None)
        tdSql.checkData(3, 8, None)

        tdSql.checkData(4, 1, -16)
        tdSql.checkData(4, 2, 9)
        tdSql.checkData(4, 3, 5)
        tdSql.checkData(4, 4, 4)
        tdSql.checkData(4, 5, 3)
        tdSql.checkData(4, 6, None)
        tdSql.checkData(4, 7, None)
        tdSql.checkData(4, 8, None)

        tdSql.checkData(5, 1, -13)
        tdSql.checkData(5, 2, 12)
        tdSql.checkData(5, 3, 11)
        tdSql.checkData(5, 4, 10)
        tdSql.checkData(5, 5, 9)
        tdSql.checkData(5, 6, 8.00000)
        tdSql.checkData(5, 7, None)
        tdSql.checkData(5, 8, None)

        tdSql.checkData(6, 1, -10)
        tdSql.checkData(6, 2, 15)
        tdSql.checkData(6, 3, 14)
        tdSql.checkData(6, 4, 13)
        tdSql.checkData(6, 5, 12)
        tdSql.checkData(6, 6, 11.00000)
        tdSql.checkData(6, 7, 10.000000000)
        tdSql.checkData(6, 8, None)

        tdSql.checkData(7, 1, -7)
        tdSql.checkData(7, 2, 18)
        tdSql.checkData(7, 3, 17)
        tdSql.checkData(7, 4, 16)
        tdSql.checkData(7, 5, 15)
        tdSql.checkData(7, 6, 14.00000)
        tdSql.checkData(7, 7, 13.000000000)
        tdSql.checkData(7, 8, 11)

        tdSql.checkCols(9)


    def test_normal_table_alter_5(self):
        """Alter: repeatedly drop

        1. Drop column
        2. Insert data
        3. Project query
        4. Loop for 7 times
        5. Kill then restart

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/stable/alter_insert2.sim
            - 2026-11-20 Alex Merged into 01-NormalTables/test_normaltable_alter.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("d4", drop=True)
        tdSql.execute(f"use d4")
        tdSql.execute(
            f"create table tb (ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float, g double, h binary(10))"
        )
        tdSql.execute(f"insert into tb values(now-28d, 1, 2, 3, 4, 5, 6, 7, 8)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)
        tdSql.checkCols(9)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, 4)

        tdSql.checkData(0, 5, 5)

        tdSql.checkData(0, 6, 6.00000)

        tdSql.checkData(0, 7, 7.000000000)

        tdSql.checkData(0, 8, 8)

        tdLog.info(f"======== step2")
        tdSql.error(f"alter table tb add column b smallint")
        tdSql.error(f"alter table tb add column b int")
        tdSql.execute(f"alter table tb drop column b")
        tdSql.execute(f"insert into tb values(now-25d, 2, 3, 4, 5, 6, 7, 8)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkCols(8)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 6.00000)

        tdSql.checkData(0, 6, 7.000000000)

        tdSql.checkData(0, 7, 8)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, 3)

        tdSql.checkData(1, 3, 4)

        tdSql.checkData(1, 4, 5)

        tdSql.checkData(1, 5, 6.00000)

        tdSql.checkData(1, 6, 7.000000000)

        tdSql.checkData(1, 7, 8)

        tdLog.info(f"======== step3")
        tdSql.execute(f"alter table tb drop column c")
        tdSql.execute(f"insert into tb values(now-22d, 3, 4, 5, 6, 7, 8)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(3)
        tdSql.checkCols(7)

        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6.00000)

        tdSql.checkData(0, 5, 7.000000000)

        tdSql.checkData(0, 6, 8)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(1, 2, 4)

        tdSql.checkData(1, 3, 5)

        tdSql.checkData(1, 4, 6.00000)

        tdSql.checkData(1, 5, 7.000000000)

        tdSql.checkData(1, 6, 8)

        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 4)

        tdSql.checkData(2, 3, 5)

        tdSql.checkData(2, 4, 6.00000)

        tdSql.checkData(2, 5, 7.000000000)

        tdSql.checkData(2, 6, 8)

        tdLog.info(f"======== step4")
        tdSql.execute(f"alter table tb drop column d")
        tdSql.execute(f"alter table tb drop column e")
        tdSql.execute(f"insert into tb values(now-19d, -19, 6, 3, 0)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(4)
        tdSql.checkCols(5)

        tdSql.checkData(0, 1, -19)

        tdSql.checkData(0, 2, 6.00000)

        tdSql.checkData(0, 3, 3.000000000)

        tdSql.checkData(0, 4, 0)

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 6.00000)

        tdSql.checkData(1, 3, 7.000000000)

        tdSql.checkData(1, 4, 8)

        tdSql.checkData(2, 1, 2)

        tdSql.checkData(2, 2, 6.00000)

        tdSql.checkData(2, 3, 7.000000000)

        tdSql.checkData(2, 4, 8)

        tdSql.checkData(3, 1, 1)

        tdSql.checkData(3, 2, 6.00000)

        tdSql.checkData(3, 3, 7.000000000)

        tdSql.checkData(3, 4, 8)

        tdLog.info(f"======== step5")
        tdSql.execute(f"alter table tb drop column g")
        tdSql.execute(f"insert into tb values(now-16d, -16, 9, 5)")
        tdSql.query(f"select count(f) from tb")
        tdSql.checkData(0, 0, 5)

        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        tdSql.checkData(0, 1, -16)

        tdSql.checkData(0, 2, 9.00000)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(1, 1, -19)

        tdSql.checkData(1, 2, 6.00000)

        tdSql.checkData(1, 3, 0)

        tdSql.checkData(2, 1, 3)

        tdSql.checkData(2, 2, 6.00000)

        tdSql.checkData(2, 3, 8)

        tdSql.checkData(3, 1, 2)

        tdSql.checkData(3, 2, 6.00000)

        tdSql.checkData(3, 3, 8)

        tdSql.checkData(4, 1, 1)

        tdSql.checkData(4, 2, 6.00000)

        tdSql.checkData(4, 3, 8)

        tdLog.info(f"======== step6")
        tdSql.execute(f"alter table tb drop column f")
        tdSql.execute(f"insert into tb values(now-13d, -13, 7)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 1, -13)

        tdSql.checkData(1, 1, -16)

        tdSql.checkData(1, 2, 5)

        tdSql.checkData(2, 1, -19)

        tdSql.checkData(2, 2, 0)

        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 8)

        tdSql.checkData(4, 1, 2)

        tdSql.checkData(4, 2, 8)

        tdSql.checkData(5, 1, 1)

        tdSql.checkData(5, 2, 8)

        tdLog.info(f"======== step7")
        tdSql.execute(f"alter table tb drop column h")
        tdSql.execute(f"insert into tb values(now-10d, -10)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(7)
        tdSql.checkCols(2)

        tdLog.info(f"tdSql.getData(0,1) = {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, -10)

        tdSql.checkData(1, 1, -13)

        tdSql.checkData(2, 1, -16)

        tdSql.checkData(3, 1, -19)

        tdSql.checkData(4, 1, 3)

        tdSql.checkData(5, 1, 2)

        tdSql.checkData(6, 1, 1)

        tdSql.error(f"alter table tb drop column a")

        tdLog.info(f"======== step9")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(7)
        tdSql.checkCols(2)

        tdSql.checkData(0, 1, -10)

        tdSql.checkData(1, 1, -13)

        tdSql.checkData(2, 1, -16)

        tdSql.checkData(3, 1, -19)

        tdSql.checkData(4, 1, 3)

        tdSql.checkData(5, 1, 2)

        tdSql.checkData(6, 1, 1)
