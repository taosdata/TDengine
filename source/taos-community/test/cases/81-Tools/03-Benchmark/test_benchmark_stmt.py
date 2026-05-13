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
from new_test_framework.utils import tdLog, tdSql, etool, sc
import json
import os
import threading
import time


class TestBenchmarkStmt:

    #
    # ------------------- test_stmt_auto_create_table_json.py ----------------
    #
    def do_stmt_auto_create_table_json(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -f %s/json/stmt_auto_create_table.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.`stb2-2`")
        tdSql.checkData(0, 0, 160)

        print("do stmt auto create table json ........ [passed]")

    #
    # ------------------- test_stmt_insert_alltypes_json.py ----------------
    #
    def do_stmt_insert_alltypes_json(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -f %s/json/stmt_insert_alltypes.json" % (binPath, os.path.dirname(__file__))
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
        tdSql.query("select count(*) from db.stb where c0 >= 0 and c0 <= 10")
        tdSql.checkData(0, 0, 160)
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
        tdSql.query("select count(*) from db.stb where c8 like 'd1%' or c8 like 'd2%'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c9 >= 0 and c9 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c10 >= 0 and c10 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c11 >= 0 and c11 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c12 >= 0 and c12 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query(
            "select count(*) from db.stb where c13 like 'b1%' or c13 like 'b2%'"
        )
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t0 >= 0 and t0 <= 10")
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
        tdSql.query("select count(*) from db.stb where t8 like 'd1%' or t8 like 'd2%'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t9 >= 0 and t9 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t10 >= 0 and t10 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t11 >= 0 and t11 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t12 >= 0 and t12 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query(
            "select count(*) from db.stb where t13 like 'b1%' or t13 like 'b2%'"
        )
        tdSql.checkData(0, 0, 160)

        print("do stmt insert all types .............. [passed]")


    #
    # ------------------- test_stmt_insert_alltypes_same_min_max.py ----------------
    #
    def do_stmt_insert_alltypes_same_min_max(self):
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f %s/json/stmt_insert_alltypes-same-min-max.json"
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

        print("do stmt insert all types min=max ...... [passed]")


    #
    # ------------------- test_stmt_offset_json.py ----------------
    #
    def do_stmt_offset_json(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/taosc_only_create_table.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)

        tdSql.query("describe db.stb")
        tdSql.checkData(9, 1, "NCHAR")
        # varchar in 3.0 but binary in 2.x
        # tdSql.checkData(14, 1, "BINARY")
        tdSql.checkData(23, 1, "NCHAR")
        # tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(9, 2, 64)
        tdSql.checkData(14, 2, 64)
        tdSql.checkData(23, 2, 64)
        tdSql.checkData(28, 2, 64)

        cmd = "%s -f %s/json/stmt_limit_offset.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 40)
        tdSql.query("select distinct(c3) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c4) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c5) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c13) from db.stb")
        tdSql.checkData(0, 0, None)

        print("do stmt offset ........................ [passed]")


    #
    # ------------------- test_stmt2_insert_retry_json_stb.py ----------------
    #
    def dbInsertThread(self):
        tdLog.info(f"dbInsertThread start")
        # taosBenchmark run
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/stmt2_insert_retry-stb.json" % (binPath, os.path.dirname(__file__))
        output, error, code = etool.run(cmd, ret_code=True)
        if code != 0:
            print(f"taosBenchmark run failed, cmd:{cmd} output:{output} error:{error} code:{code}")
            tdLog.exit("dbInsertThread run taosBenchmark failed")

    def restartTaosdThread(self):
        time.sleep(5)
        sc.dnodeStopAll()
        time.sleep(3)
        sc.dnodeStart(1)    

    def do_stmt2_insert_retry_json_stb(self):
        t1 = threading.Thread(target=self.dbInsertThread)
        t2 = threading.Thread(target=self.restartTaosdThread)
        
        t1.start()
        t2.start()
        # wait thread end 
        t1.join()
        t2.join()
        
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 100)
        print("do stmt2 insert retry ................. [passed]")

    #
    # ------------------- test_stmt2_insert.py ----------------
    #
    def testBenchmarkJson(self, benchmark, jsonFile):
        # exe insert 
        cmd = f"{benchmark} -f {jsonFile}"
        os.system(cmd)
        
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        db  = data["databases"][0]["dbinfo"]["name"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(f"get json info: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
        tdSql.query(sql)
        tdSql.checkRows(0)


    def do_stmt2_insert(self):
        benchmark = etool.benchMarkFile()
        # batch - auto-create-table(yes or no)
        jsonDir = os.path.dirname(__file__) + "/json"
        self.testBenchmarkJson(benchmark, f"{jsonDir}/stmt2_insert_batch_autoctb_yes.json")
        self.testBenchmarkJson(benchmark, f"{jsonDir}/stmt2_insert_batch_autoctb_no.json")
        # interlace - auto-create-table(yes or no)
        self.testBenchmarkJson(benchmark, f"{jsonDir}/stmt2_insert_interlace_autoctb_yes.json")
        self.testBenchmarkJson(benchmark, f"{jsonDir}/stmt2_insert_interlace_autoctb_no.json")
        # csv - (batch or interlace)
        self.testBenchmarkJson(benchmark, f"{jsonDir}/stmt2_insert_csv_interlace_autoctb_yes.json")
        self.testBenchmarkJson(benchmark, f"{jsonDir}/stmt2_insert_csv_batch_autoctb_no.json")

        print("do stmt2 insert ....................... [passed]")


    #
    # ------------------- test_taosdemo_test_insert_with_json_stmt_other_para.py ----------------
    #
    def do_taosdemo_test_insert_with_json_stmt_other_para(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark use %s" % binPath)

        # insert:  sample json
        os.system("%s -f %s/json/insert-sample-ts-stmt.json -y " % (binPath, os.path.dirname(__file__)))
        tdSql.execute("use dbtest123")
        tdSql.query("select c2 from stb0")
        tdSql.checkData(0, 0, 2147483647)
        if major_ver == "2":
            tdSql.query("select c0 from stb0_0 order by ts")
            tdSql.checkData(3, 0, 4)
            tdSql.query("select count(*) from stb0 order by ts")
            tdSql.checkData(0, 0, 40)
            tdSql.query("select * from stb0_1 order by ts")
            tdSql.checkData(0, 0, "2021-10-28 15:34:44.735")
            tdSql.checkData(3, 0, "2021-10-31 15:34:44.735")
        tdSql.query("select * from stb1 where t1=-127")
        tdSql.checkRows(20)
        tdSql.query("select * from stb1 where t2=127")
        tdSql.checkRows(10)
        tdSql.query("select * from stb1 where t2=126")
        tdSql.checkRows(10)

        # insert: timestamp and step
        os.system("%s -f %s/json/insert-timestep-stmt.json -y " % (binPath, os.path.dirname(__file__)))
        tdSql.execute("use db")
        tdSql.query("show stables")
        if major_ver == "3":
            tdSql.query("select count (*) from (select distinct(tbname) from stb0)")
        else:
            tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        if major_ver == "3":
            tdSql.query("select count (*) from (select distinct(tbname) from stb1)")
        else:
            tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select last(ts) from db.stb00_0")
        tdSql.checkData(0, 0, "2020-10-01 00:00:00.019000")
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select last(ts) from db.stb01_0")
        tdSql.checkData(0, 0, "2020-11-01 00:00:00.190000")
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 400)

        # # insert:  disorder_ratio
        os.system(
            "%s -f %s/json/insert-disorder-stmt.json 2>&1  -y " % (binPath, os.path.dirname(__file__))
        )
        tdSql.execute("use db")
        if major_ver == "3":
            tdSql.query("select count (*) from (select distinct(tbname) from stb0)")
        else:
            tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1)
        if major_ver == "3":
            tdSql.query("select count (*) from (select distinct(tbname) from stb1)")
        else:
            tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 10)

        # insert: test interlace parament
        os.system(
            "%s -f %s/json/insert-interlace-row-stmt.json -y " % (binPath, os.path.dirname(__file__))
        )
        tdSql.execute("use db")
        if major_ver == "3":
            tdSql.query("select count (*) from (select distinct(tbname) from stb0)")
        else:
            tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count (*) from stb0")
        tdSql.checkData(0, 0, 15000)

        # insert: auto_create

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db")
        tdSql.execute("use db")
        os.system(
            "%s -y -f %s/json/insert-drop-exist-auto-N00-stmt.json "
            % (binPath, os.path.dirname(__file__))
        )  # drop = no, child_table_exists, auto_create_table varies
        tdSql.execute("use db")
        tdSql.query(
            "show tables like 'NN123%'"
        )  # child_table_exists = no, auto_create_table varies = 123
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'NNN%'"
        )  # child_table_exists = no, auto_create_table varies = no
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'NNY%'"
        )  # child_table_exists = no, auto_create_table varies = yes
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'NYN%'"
        )  # child_table_exists = yes, auto_create_table varies = no
        tdSql.checkRows(0)
        tdSql.query(
            "show tables like 'NY123%'"
        )  # child_table_exists = yes, auto_create_table varies = 123
        tdSql.checkRows(0)
        tdSql.query(
            "show tables like 'NYY%'"
        )  # child_table_exists = yes, auto_create_table varies = yes
        tdSql.checkRows(0)

        tdSql.execute("drop database if exists db")
        os.system(
            "%s -y -f %s/json/insert-drop-exist-auto-Y00-stmt.json "
            % (binPath, os.path.dirname(__file__))
        )  # drop = yes, child_table_exists, auto_create_table varies
        tdSql.execute("use db")
        tdSql.query(
            "show tables like 'YN123%'"
        )  # child_table_exists = no, auto_create_table varies = 123
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'YNN%'"
        )  # child_table_exists = no, auto_create_table varies = no
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'YNY%'"
        )  # child_table_exists = no, auto_create_table varies = yes
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'YYN%'"
        )  # child_table_exists = yes, auto_create_table varies = no
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'YY123%'"
        )  # child_table_exists = yes, auto_create_table varies = 123
        tdSql.checkRows(20)
        tdSql.query(
            "show tables like 'YYY%'"
        )  # child_table_exists = yes, auto_create_table varies = yes
        tdSql.checkRows(20)

        os.system("rm -rf ./insert_res.txt")
        
        print("do stmt2 old taosdemo case ............ [passed]")


    #
    # ------------------- main ----------------
    #
    def test_benchmark_stmt(self):
        """taosBenchmark stmt & stmt2 insert

        1. Stmt auto create table with json
        2. Stmt insert alltypes with json
        3. Stmt insert alltypes same min max
        4. Stmt offset with json
        5. Stmt2 restart dnode during insert with json
        6. Stmt2 insert with auto-create-table(yes or no), interlace or batch, csv or not
        7. Old case (taosdemo) for insert with stmt other parameters
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt_auto_create_table_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt_insert_alltypes_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt_insert_alltypes_same_min_max.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt_offset_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt2_insert.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_stmt2_insert_retry_json_stb.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosdemo_test_insert_with_json_stmt_other_para.py

        """
        self.do_stmt_auto_create_table_json()
        self.do_stmt_insert_alltypes_json()
        self.do_stmt_insert_alltypes_same_min_max()
        self.do_stmt_offset_json()
        self.do_stmt2_insert_retry_json_stb()
        self.do_stmt2_insert()
        self.do_taosdemo_test_insert_with_json_stmt_other_para()