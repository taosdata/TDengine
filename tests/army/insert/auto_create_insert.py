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

import sys
import time
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):

    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS test")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS test")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")


    def insert_table_auto_create(self):
        tdLog.info(f"insert table auto create")
        tdSql.execute("USE test")
        tdLog.info("start to test auto create insert...")
        tdSql.execute("INSERT INTO t_0 USING stb TAGS (0) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.execute("INSERT INTO t_0 USING stb TAGS (0) VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_0")
        tdSql.checkRows(2)

    def insert_table_pre_create(self):
        tdLog.info(f"insert table pre create")
        tdSql.execute("USE test")
        tdLog.info("start to pre create table...")
        tdSql.execute("CREATE TABLE t_1 USING stb TAGS (1)")
        tdLog.info("start to test pre create insert...")
        tdSql.execute("INSERT INTO t_1 USING stb TAGS (1) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.execute("INSERT INTO t_1 VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_1")
        tdSql.checkRows(2)

    def insert_table_auto_insert_with_cache(self):
        tdLog.info(f"insert table auto insert with cache")
        tdSql.execute("USE test")
        tdLog.info("start to test auto insert with cache...")
        tdSql.execute("CREATE TABLE t_2 USING stb TAGS (2)")
        tdLog.info("start to insert to init cache...")
        tdSql.execute("INSERT INTO t_2 VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.execute("INSERT INTO t_2 USING stb TAGS (2) VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_2")
        tdSql.checkRows(2)

    def insert_table_auto_insert_with_multi_rows(self):
        tdLog.info(f"insert table auto insert with multi rows")
        tdSql.execute("USE test")
        tdLog.info("start to test auto insert with multi rows...")
        tdSql.execute("CREATE TABLE t_3 USING stb TAGS (3)")
        tdLog.info("start to insert multi rows...")
        tdSql.execute("INSERT INTO t_3 VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test'), ('2024-01-01 00:00:01', 1, 2.0, 'test')")
        tdSql.query("select * from t_3")
        tdSql.checkRows(2)

        tdLog.info("start to insert multi rows with direct insert and auto create...")
        tdSql.execute("INSERT INTO t_4 USING stb TAGS (4) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test'), t_3 VALUES ('2024-01-01 00:00:02', 1, 2.0, 'test')")
        tdSql.query("select * from t_4")
        tdSql.checkRows(1)
        tdSql.query("select * from t_3")
        tdSql.checkRows(3)

        tdLog.info("start to insert multi rows with auto create and direct insert...")
        tdSql.execute("INSERT INTO t_3 VALUES ('2024-01-01 00:00:03', 1, 2.0, 'test'),t_4 USING stb TAGS (4) VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test'),")
        tdSql.query("select * from t_4")
        tdSql.checkRows(2)
        tdSql.query("select * from t_3")
        tdSql.checkRows(4)

        tdLog.info("start to insert multi rows with auto create into same table...")
        tdSql.execute("INSERT INTO t_10 USING stb TAGS (10) VALUES ('2024-01-01 00:00:04', 1, 2.0, 'test'),t_10 USING stb TAGS (10) VALUES ('2024-01-01 00:00:05', 1, 2.0, 'test'),")
        tdSql.query("select * from t_10")
        tdSql.checkRows(2)

    def check_some_err_case(self):
        tdLog.info(f"check some err case")
        tdSql.execute("USE test")

        tdLog.info("start to test err stb name...")
        tdSql.error("INSERT INTO t_5 USING errrrxx TAGS (5) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table does not exist")

        tdLog.info("start to test err syntax name...")
        tdSql.error("INSERT INTO t_5 USING stb TAG (5) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error")

        tdLog.info("start to test err syntax values...")
        tdSql.error("INSERT INTO t_5 USING stb TAG (5) VALUS ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error")

        tdLog.info("start to test err tag counts...")
        tdSql.error("INSERT INTO t_5 USING stb TAG (5,1) VALUS ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error")

        tdLog.info("start to test err tag counts...")
        tdSql.error("INSERT INTO t_5 USING stb TAG ('dasds') VALUS ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error")

        tdLog.info("start to test err values counts...")
        tdSql.error("INSERT INTO t_5 USING stb TAGS (5) VALUES ('2024-01-01 00:00:00', 1, 1 ,2.0, 'test')", expectErrInfo="Illegal number of columns")

        tdLog.info("start to test err values...")
        tdSql.error("INSERT INTO t_5 USING stb TAGS (5) VALUES ('2024-01-01 00:00:00', 'dasdsa', 1 ,2.0, 'test')", expectErrInfo="syntax error")

    def check_same_table_same_ts(self):
        tdLog.info(f"check same table same ts")
        tdSql.execute("USE test")
        tdSql.execute("INSERT INTO t_6 USING stb TAGS (6) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test') t_6 USING stb TAGS (6) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.query("select * from t_6")
        tdSql.checkRows(1)

    def check_tag_parse_error_with_cache(self):
        tdLog.info(f"check tag parse error with cache")
        tdSql.execute("USE test")
        tdSql.execute("INSERT INTO t_7 USING stb TAGS (7) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.error("INSERT INTO t_7 USING stb TAGS ('ddd') VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="syntax error")
        tdSql.query("select * from t_7")
        tdSql.checkRows(1)

    def check_duplicate_table_with_err_tag(self):
        tdLog.info(f"check tag parse error with cache")
        tdSql.execute("USE test")
        tdSql.execute("INSERT INTO t_8 USING stb TAGS (8) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test') t_8 USING stb TAGS (ddd) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.query("select * from t_8")
        tdSql.checkRows(1)

    def check_table_with_another_stb_name(self):
        tdLog.info(f"check table with another stb name")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb2 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
        tdSql.execute("INSERT INTO t_20 USING stb2 TAGS (20) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')")
        tdSql.query("select * from t_20")
        tdSql.checkRows(1)
        tdSql.error("INSERT INTO t_20 USING stb TAGS (20) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables")
        tdSql.error("INSERT INTO t_20 USING stb TAGS (20) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables")

    def check_table_with_same_name(self):
        tdLog.info(f"check table with same name")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb3 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
        tdSql.error("INSERT INTO stb3 USING stb3 TAGS (30) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables")
        tdSql.query("select * from stb3")
        tdSql.checkRows(0)

    def check_table_with_same_name(self):
        tdLog.info(f"check table with same name")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE IF NOT EXISTS stb3 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
        tdSql.error("INSERT INTO stb3 USING stb3 TAGS (30) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')", expectErrInfo="Table already exists in other stables")
        tdSql.query("select * from stb3")
        tdSql.checkRows(0)



    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # prepare database
        self.prepare_database()

        # insert table auto create
        self.insert_table_auto_create()

        # insert table pre create
        self.insert_table_pre_create()

        # insert table auto insert with cache
        self.insert_table_auto_insert_with_cache()

        # insert table auto insert with multi rows
        self.insert_table_auto_insert_with_multi_rows()

        # check some err case
        self.check_some_err_case()

        # check same table same ts
        self.check_same_table_same_ts()

        # check tag parse error with cache
        self.check_tag_parse_error_with_cache()

        # check duplicate table with err tag
        self.check_duplicate_table_with_err_tag()

        # check table with another stb name
        self.check_table_with_another_stb_name()

        # check table with same name
        self.check_table_with_same_name()

        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())