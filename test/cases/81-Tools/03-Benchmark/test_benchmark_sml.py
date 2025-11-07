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
from new_test_framework.utils import tdLog, tdSql, etool
import os

class TestBenchmarkSml:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """
    #
    # ------------------- test_sml_auto_create_table_json.py ----------------
    #
    def do_sml_auto_create_table_json(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        binPath = etool.benchMarkFile()

        cmd = "%s -f %s/json/sml_auto_create_table.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.`stb4-2`")
        tdSql.checkData(0, 0, 160)

        print("do sml_auto_create_table_json ......... [passed]")

    #
    # ------------------- test_sml_insert_alltypes_json.py ----------------
    #
    def do_sml_insert_alltypes_json(self):

        binPath = etool.benchMarkFile()

        cmd = "%s -f %s/json/sml_insert_alltypes.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 160)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkRows(27)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "INT")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 1, "FLOAT")
        tdSql.checkData(4, 1, "DOUBLE")
        tdSql.checkData(5, 1, "SMALLINT")
        tdSql.checkData(6, 1, "TINYINT")
        tdSql.checkData(7, 1, "BOOL")
        tdSql.checkData(8, 1, "NCHAR")
        tdSql.checkData(9, 1, "INT UNSIGNED")
        tdSql.checkData(10, 1, "BIGINT UNSIGNED")
        tdSql.checkData(11, 1, "TINYINT UNSIGNED")
        tdSql.checkData(12, 1, "SMALLINT UNSIGNED")
        tdSql.checkData(14, 1, "NCHAR")
        tdSql.checkData(15, 1, "NCHAR")
        tdSql.checkData(16, 1, "NCHAR")
        tdSql.checkData(17, 1, "NCHAR")
        tdSql.checkData(18, 1, "NCHAR")
        tdSql.checkData(19, 1, "NCHAR")
        tdSql.checkData(20, 1, "NCHAR")
        tdSql.checkData(21, 1, "NCHAR")
        tdSql.checkData(22, 1, "NCHAR")
        tdSql.checkData(23, 1, "NCHAR")
        tdSql.checkData(24, 1, "NCHAR")
        tdSql.checkData(25, 1, "NCHAR")
        tdSql.checkData(26, 1, "NCHAR")
    
        print("do sml_insert_alltypes_json ........... [passed]")

    #
    # ------------------- test_sml_interlace.py ----------------
    #
    def do_sml_interlace(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_interlace.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb1)")
        else:
            tdSql.query("select count(tbname) from db.stb1")
        tdSql.checkData(0, 0, 8)
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb2)")
        else:
            tdSql.query("select count(tbname) from db.stb2")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb1")
        result = tdSql.getData(0, 0)
        assert result <= 160, "result is %s > expect: 160" % result
        tdSql.query("select count(*) from db.stb2")
        result = tdSql.getData(0, 0)
        assert result <= 160, "result is %s > expect: 160" % result
    
        print("do sml interlace ...................... [passed]")

    #
    # ------------------- test_sml_json_alltypes_interlace.py ----------------
    #
    def do_sml_json_alltypes_interlace(self):

        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_json_alltypes-interlace.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb1")
        tdSql.checkData(1, 1, "BOOL")
        tdSql.query("describe db.stb2")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb3")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb4")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb5")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb6")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb7")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb8")
        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 1, "NCHAR")
            tdSql.checkData(1, 2, 8)

        tdSql.query("describe db.stb9")
        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 2, 8)
            tdSql.checkData(1, 1, "NCHAR")

        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb5")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb6")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb7")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb8")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb9")
        tdSql.checkData(0, 0, 160)
    
        print("do sml all data-types interlace ....... [passed]")

    #
    # ------------------- test_sml_json_alltypes.py ----------------
    #
    def do_sml_json_alltypes(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_json_alltypes.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb1")
        tdSql.checkData(1, 1, "BOOL")
        tdSql.query("describe db.stb2")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb3")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb4")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb5")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb6")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb7")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb8")
        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 1, "NCHAR")
            tdSql.checkData(1, 2, 8)

        tdSql.query("describe db.stb9")
        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 1, "NCHAR")
            tdSql.checkData(1, 2, 8)

        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb5")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb6")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb7")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb8")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb9")
        tdSql.checkData(0, 0, 160)
    
        print("do sml all data-types  ................ [passed]")

    #
    # ------------------- test_sml_json_insert_alltypes_same_min_max.py ----------------
    #
    def do_sml_json_insert_alltypes_same_min_max(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f %s/json/sml_json_insert_alltypes-same-min-max.json"
            % (binPath, os.path.dirname(__file__))
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.stb")
        rows = tdSql.queryResult[0]
        tdSql.query("select * from db.stb")
        for row in range(rows[0]):
            if major_ver == "3":
                tdSql.checkData(row, 1, 1.0)
                tdSql.checkData(row, 2, 1.0)
                tdSql.checkData(row, 4, 1.0)
                tdSql.checkData(row, 5, 1.0)
                tdSql.checkData(row, 6, 1.0)
                tdSql.checkData(row, 7, 1.0)
                tdSql.checkData(row, 11, 30.0)
                tdSql.checkData(row, 12, 60000.0)
            else:
                tdSql.checkData(row, 1, 1.0)
                tdSql.checkData(row, 2, 1.0)
                tdSql.checkData(row, 3, 60000.0)
                tdSql.checkData(row, 5, 1.0)
                tdSql.checkData(row, 6, 1.0)
                tdSql.checkData(row, 7, 1.0)
                tdSql.checkData(row, 8, 1.0)
                tdSql.checkData(row, 12, 30.0)
    
        print("do sml all data-types min==max ........ [passed]")

    #
    # ------------------- test_sml_taosjson_alltypes.py ----------------
    #
    def do_sml_taosjson_alltypes(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_taosjson_alltypes.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb1")
        tdSql.checkData(1, 1, "BOOL")
        tdSql.query("describe db.stb2")
        tdSql.checkData(1, 1, "TINYINT")
        tdSql.query("describe db.stb3")
        tdSql.checkData(1, 1, "SMALLINT")
        tdSql.query("describe db.stb4")
        tdSql.checkData(1, 1, "INT")
        tdSql.query("describe db.stb5")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.query("describe db.stb6")
        tdSql.checkData(1, 1, "FLOAT")
        tdSql.query("describe db.stb7")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb8")
        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 1, "BINARY")
            tdSql.checkData(1, 2, 8)

        tdSql.query("describe db.stb9")
        tdSql.checkData(1, 1, "NCHAR")
        if major_ver == "3":
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 2, 8)

        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb5")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb6")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb7")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb8")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb9")
        tdSql.checkData(0, 0, 160)
    
        print("do sml taosjson alltypes .............. [passed]")

    #
    # ------------------- test_sml_taosjson_insert_alltypes_same_min_max.py ----------------
    #
    def do_sml_taosjson_insert_alltypes_same_min_max(self):
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f %s/json/sml_taosjson_insert_alltypes-same-min-max.json"
            % (binPath, os.path.dirname(__file__))
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.stb")
        rows = tdSql.queryResult[0]
        tdSql.query("select * from db.stb")
        for row in range(rows[0]):
            tdSql.checkData(row, 3, 1)
            tdSql.checkData(row, 4, 3000000000)
            tdSql.checkData(row, 5, 1.0)
            tdSql.checkData(row, 6, 1.0)
            tdSql.checkData(row, 7, 1)
            tdSql.checkData(row, 8, 1)
            tdSql.checkData(row, 9, True)

        tdLog.success("%s successfully executed" % __file__)
    
        print("do sml taosjson all data-types ........ [passed]")

    #
    # ------------------- test_sml_telnet_alltypes.py ----------------
    #
    def do_sml_telnet_alltypes(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_telnet_alltypes.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb1")
        tdSql.checkData(1, 1, "BOOL")
        tdSql.query("describe db.stb2")
        tdSql.checkData(1, 1, "TINYINT")
        tdSql.query("describe db.stb3")
        tdSql.checkData(1, 1, "TINYINT UNSIGNED")
        tdSql.query("describe db.stb4")
        tdSql.checkData(1, 1, "SMALLINT")
        tdSql.query("describe db.stb5")
        tdSql.checkData(1, 1, "SMALLINT UNSIGNED")
        tdSql.query("describe db.stb6")
        tdSql.checkData(1, 1, "INT")
        tdSql.query("describe db.stb7")
        tdSql.checkData(1, 1, "INT UNSIGNED")
        tdSql.query("describe db.stb8")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.query("describe db.stb9")
        tdSql.checkData(1, 1, "BIGINT UNSIGNED")
        tdSql.query("describe db.stb10")
        tdSql.checkData(1, 1, "FLOAT")
        tdSql.query("describe db.stb11")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb12")

        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(
                1, 2, 8
            )  # 3.0 will use a bit more space for schemaless create table
        else:
            tdSql.checkData(1, 1, "BINARY")
            tdSql.checkData(1, 2, 8)

        tdSql.query("describe db.stb13")
        tdSql.checkData(1, 1, "NCHAR")
        if major_ver == "3":
            tdSql.checkData(1, 2, 8)
        else:
            tdSql.checkData(1, 2, 8)

        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb5")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb6")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb7")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb8")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb9")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb11")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb12")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb13")
        tdSql.checkData(0, 0, 160)
        print("do sml telnet all data-types .......... [passed]")
        
    #
    # ------------------- test_sml_telnet_insert_alltypes_same_min_max.py ----------------
    #
    def do_sml_telnet_insert_alltypes_same_min_max(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f %s/json/sml_telnet_insert_alltypes-same-min-max.json"
            % (binPath, os.path.dirname(__file__))
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.stb")
        rows = tdSql.queryResult[0]
        tdSql.query("select * from db.stb")
        for row in range(rows[0]):
            if major_ver == "3":
                tdSql.checkData(row, 1, 1)
                tdSql.checkData(row, 2, "1i32")
                tdSql.checkData(row, 3, "3000000000i64")
                tdSql.checkData(row, 4, "1.000000f32")
                tdSql.checkData(row, 5, "1.000000f64")
                tdSql.checkData(row, 6, "1i16")
                tdSql.checkData(row, 7, "1i8")
                tdSql.checkData(row, 8, "true")
                tdSql.checkData(row, 9, "4000000000u32")
                tdSql.checkData(row, 10, "5000000000u64")
                tdSql.checkData(row, 11, "30u8")
                tdSql.checkData(row, 12, "60000u16")
            else:
                tdSql.checkData(row, 1, 1)
                tdSql.checkData(row, 2, "1i32")
                tdSql.checkData(row, 3, "60000u16")
                tdSql.checkData(row, 4, "3000000000i64")
                tdSql.checkData(row, 5, "1.000000f32")
                tdSql.checkData(row, 6, "1.000000f64")
                tdSql.checkData(row, 7, "1i16")
                tdSql.checkData(row, 8, "1i8")
                tdSql.checkData(row, 9, "true")
                tdSql.checkData(row, 10, "4000000000u32")
                tdSql.checkData(row, 11, "5000000000u64")
                tdSql.checkData(row, 12, "30u8")

        print("do sml telnet all data-types min==max ...... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_sml(self):
        """taosBenchmark schemaless insert

        1. sml auto create table with json
        2. sml insert alltypes with json
        3. sml interlace
        4. sml json alltypes interlace
        5. sml json alltypes
        6. sml json insert alltypes same min max
        7. sml taosjson alltypes
        8. sml taosjson insert alltypes same min max
        9. sml telnet alltypes
        10.sml telnet insert alltypes same min max
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_auto_create_table_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_json_alltypes.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_json_insert_alltypes_same_min_max.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_taosjson_alltypes.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_telnet_alltypes.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_telnet_insert_alltypes_same_min_max.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_json_alltypes_interlace.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_insert_alltypes_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_interlace.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_sml_taosjson_insert_alltypes_same_min_max.py

        """
        self.do_sml_auto_create_table_json()
        self.do_sml_insert_alltypes_json()
        self.do_sml_interlace()
        self.do_sml_json_alltypes_interlace()
        self.do_sml_json_alltypes()
        self.do_sml_json_insert_alltypes_same_min_max()
        self.do_sml_taosjson_alltypes()
        self.do_sml_taosjson_insert_alltypes_same_min_max()
        self.do_sml_telnet_alltypes()
        self.do_sml_telnet_insert_alltypes_same_min_max() 