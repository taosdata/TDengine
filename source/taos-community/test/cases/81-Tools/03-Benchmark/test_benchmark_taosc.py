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
from new_test_framework.utils import tdLog, tdSql, etool,sc
import os
import subprocess
import time

class TestTaoscAutoCreateTableJson:
    #
    # ------------------- test_taosc_auto_create_table_json.py ----------------
    #
    def do_taosc_auto_create_table_json(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_auto_create_table.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select distinct(c5) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.stb1")
        tdSql.checkData(0, 0, None)

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.`stb1-2`")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select distinct(c5) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)

        if major_ver == "3":
            tdSql.query(
                "select `ttl` from information_schema.ins_tables where db_name = 'db' and table_name like 'stb\_%' limit 1"
            )
            tdSql.checkData(0, 0, 360)

            tdSql.query(
                "select `ttl` from information_schema.ins_tables where db_name = 'db' and table_name like 'stb1-%' limit 1"
            )
            tdSql.checkData(0, 0, 180)

        print("do taosc auto-create table ............ [passed]")

    #
    # ------------------- test_taosc_insert_alltypes_json_partial_col.py ----------------
    #
    def do_taosc_insert_alltypes_json_partial_col(self):
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f %s/json/taosc_insert_alltypes-partial-col.json"
            % (binPath, os.path.dirname(__file__))
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 160)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkRows(29)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TIMESTAMP")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.checkData(4, 1, "FLOAT")
        tdSql.checkData(5, 1, "DOUBLE")
        tdSql.checkData(6, 1, "SMALLINT")
        tdSql.checkData(7, 1, "TINYINT")
        tdSql.checkData(8, 1, "BOOL")
        tdSql.checkData(9, 1, "NCHAR")
        tdSql.checkData(9, 2, 29)
        tdSql.checkData(10, 1, "INT UNSIGNED")
        tdSql.checkData(11, 1, "BIGINT UNSIGNED")
        tdSql.checkData(12, 1, "TINYINT UNSIGNED")
        tdSql.checkData(13, 1, "SMALLINT UNSIGNED")
        # binary/varchar diff in 2.x/3.x
        # tdSql.checkData(14, 1, "BINARY")
        tdSql.checkData(14, 2, 23)
        tdSql.checkData(15, 1, "TIMESTAMP")
        tdSql.checkData(16, 1, "INT")
        tdSql.checkData(17, 1, "BIGINT")
        tdSql.checkData(18, 1, "FLOAT")
        tdSql.checkData(19, 1, "DOUBLE")
        tdSql.checkData(20, 1, "SMALLINT")
        tdSql.checkData(21, 1, "TINYINT")
        tdSql.checkData(22, 1, "BOOL")
        tdSql.checkData(23, 1, "NCHAR")
        tdSql.checkData(23, 2, 17)
        tdSql.checkData(24, 1, "INT UNSIGNED")
        tdSql.checkData(25, 1, "BIGINT UNSIGNED")
        tdSql.checkData(26, 1, "TINYINT UNSIGNED")
        tdSql.checkData(27, 1, "SMALLINT UNSIGNED")
        # binary/varchar diff in 2.x/3.x
        # tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(28, 2, 19)
        tdSql.query("select count(*) from db.stb where c1 >= 0 and c1 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select c0,c1,c2 from db.stb limit 1")
        dbresult = tdSql.queryResult
        for i in range(len(dbresult[0])):
            if i in (0, 1) and dbresult[0][i] is None:
                tdLog.exit("result[0][%d] is NULL, which should not be" % i)
            else:
                tdLog.info("result[0][{0}] is {1}".format(i, dbresult[0][i]))
        tdSql.checkData(0, 2, None)

        print("do taosc insert partial_col ............ [passed]")

    #
    # ------------------- test_taosc_insert_alltypes_json.py ----------------
    #
    def do_taosc_insert_alltypes_json(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_insert_alltypes.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 160)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkRows(29)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TIMESTAMP")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.checkData(4, 1, "FLOAT")
        tdSql.checkData(5, 1, "DOUBLE")
        tdSql.checkData(6, 1, "SMALLINT")
        tdSql.checkData(7, 1, "TINYINT")
        tdSql.checkData(8, 1, "BOOL")
        tdSql.checkData(9, 1, "NCHAR")
        tdSql.checkData(9, 2, 29)
        tdSql.checkData(10, 1, "INT UNSIGNED")
        tdSql.checkData(11, 1, "BIGINT UNSIGNED")
        tdSql.checkData(12, 1, "TINYINT UNSIGNED")
        tdSql.checkData(13, 1, "SMALLINT UNSIGNED")
        # binary/varchar diff in 2.x/3.x
        # tdSql.checkData(14, 1, "BINARY")
        tdSql.checkData(14, 2, 23)
        tdSql.checkData(15, 1, "TIMESTAMP")
        tdSql.checkData(16, 1, "INT")
        tdSql.checkData(17, 1, "BIGINT")
        tdSql.checkData(18, 1, "FLOAT")
        tdSql.checkData(19, 1, "DOUBLE")
        tdSql.checkData(20, 1, "SMALLINT")
        tdSql.checkData(21, 1, "TINYINT")
        tdSql.checkData(22, 1, "BOOL")
        tdSql.checkData(23, 1, "NCHAR")
        tdSql.checkData(23, 2, 17)
        tdSql.checkData(24, 1, "INT UNSIGNED")
        tdSql.checkData(25, 1, "BIGINT UNSIGNED")
        tdSql.checkData(26, 1, "TINYINT UNSIGNED")
        tdSql.checkData(27, 1, "SMALLINT UNSIGNED")
        # binary/varchar diff in 2.x/3.x
        # tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(28, 2, 19)
        tdSql.query("select count(*) from db.stb where c1 >= 0 and c1 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c2 >= 0 and c2 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c3 >= 0 and c3 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c4 >= 0 and c4 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c5 >= 0 and c5 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c6 >= 0 and c6 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c8 = 'd1' or c8 = 'd2'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c9 >= 0 and c9 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c10 >= 0 and c10 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c11 >= 0 and c11 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c12 >= 0 and c12 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c13 = 'b1' or c13 = 'b2'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t1 >= 0 and t1 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t2 >= 0 and t2 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t3 >= 0 and t3 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t4 >= 0 and t4 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t5 >= 0 and t5 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t6 >= 0 and t6 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t8 = 'd1' or t8 = 'd2'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t9 >= 0 and t9 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t10 >= 0 and t10 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t11 >= 0 and t11 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t12 >= 0 and t12 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t13 = 'b1' or t13 = 'b2'")
        tdSql.checkData(0, 0, 160)

        if major_ver == "3":
            tdSql.query(
                "select `ttl` from information_schema.ins_tables where db_name = 'db' limit 1"
            )
            tdSql.checkData(0, 0, 360)

        print("do taosc insert all types ............. [passed]")

    #
    # ------------------- test_taosc_insert_alltypes_same_min_max.py ----------------
    #
    def do_taosc_insert_alltypes_same_min_max(self):
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f %s/json/taosc_insert_alltypes-same-min-max.json"
            % (binPath, os.path.dirname(__file__))
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.t0")
        rows = tdSql.queryResult[0]
        tdSql.query("select * from db.t0")
        for row in range(rows[0]):
            tdSql.checkData(row, 1, 1)
            tdSql.checkData(row, 2, 3000000000)
            tdSql.checkData(row, 3, 1.0)
            tdSql.checkData(row, 4, 1.0)
            tdSql.checkData(row, 5, 1)
            tdSql.checkData(row, 6, 1)
            tdSql.checkData(row, 7, True)
            tdSql.checkData(row, 8, 4000000000)
            tdSql.checkData(row, 9, 5000000000)
            tdSql.checkData(row, 10, 30)
            tdSql.checkData(row, 11, 60000)

        print("do taosc all types same min max ....... [passed]")

    #
    # ------------------- test_taosc_insert_retry_json_global.py ----------------
    #
    def do_taosc_insert_retry_json_global(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_insert_retry-global.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)
        sc.dnodeStopAll()
        time.sleep(2)
        sc.dnodeStart(1)
        time.sleep(2)

        sql = "select count(*) from test.meters"
        tdSql.checkDataLoop(0, 0, 10, sql, loopCount=30) 

        print("do taosc insert retry ................. [passed]")
    
    #
    # ------------------- test_taosc_insert_retry_json_stb.py ----------------
    #
    def do_taosc_insert_retry_json_stb(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_insert_retry-stb.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)
        sc.dnodeStopAll()
        time.sleep(2)
        sc.dnodeStart(1)
        time.sleep(2)

        sql = "select count(*) from test.meters"
        tdSql.checkDataLoop(0, 0, 10, sql, loopCount=30) 
    
        print("do taosc insert retry stb ............. [passed]")
    
    #
    # ------------------- test_taosc_insert_table_creating_interval.py ----------------
    #
    def do_taosc_insert_table_creating_interval(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f %s/json/taosc_insert_table-creating-interval.json -g 2>&1| grep sleep | wc -l"
            % (binPath, os.path.dirname(__file__))
        )
        tdLog.info("%s" % cmd)

        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")

        if int(sleepTimes) != 8:
            tdLog.exit("expected sleep times 4, actual %d" % int(sleepTimes))

        if major_ver == "3":
            tdSql.query(
                "select count(*) from (select distinct(tbname) from test.meters)"
            )
        else:
            tdSql.query("select count(tbname) from test.meters")
        tdSql.checkData(0, 0, 20)

        print("do taosc auto-create table ............ [passed]")


    #
    # ------------------- main ----------------
    #
    def test_benchmark_taosc(self):
        """taosBenchmark taosc insert

        1. Taosc insert auto create table with json
        2. Taosc insert alltypes with json partial col
        3. Taosc insert alltypes with json
        4. Taosc insert alltypes same min max
        5. Taosc insert mix
        6. Taosc insert retry global with json
        7. Taosc insert retry stb with json
        8. Taosc insert with table creating interval
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_auto_create_table_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_alltypes_same_min_max.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_alltypes_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_alltypes_json_partial_col.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_table_creating_interval.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_retry_json_global.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_retry_json_stb.py

        """
        self.do_taosc_auto_create_table_json()
        self.do_taosc_insert_alltypes_json_partial_col()
        self.do_taosc_insert_alltypes_json()
        self.do_taosc_insert_alltypes_same_min_max()
        self.do_taosc_insert_retry_json_global()
        self.do_taosc_insert_retry_json_stb()
        self.do_taosc_insert_table_creating_interval()
        
        