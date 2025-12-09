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
import time


class TestCommandlineSmlRest:
    def caseDescription(self):
        """
        [TD-22334] taosBenchmark sml rest test cases
        """
    #
    # ------------------- test_commandline_sml_rest.py ----------------
    #
    def do_commandline_sml_rest(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -I sml-rest -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-line -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-telnet -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-json -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-taosjson -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -N -I sml-rest -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0
        print("do sml rest ........................... [passed]")

    #
    # ------------------- test_rest_insert_alltypes_json.py ----------------
    #
    def do_rest_insert_alltypes_json(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -f %s/json/rest_insert_alltypes.json" % (binPath, os.path.dirname(__file__))
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

        print("do rest insert all types .............. [passed]")

    #
    # ------------------- test_telnet_tcp.py ----------------
    #
    def do_telnet_tcp(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_telnet_tcp.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(5)
        tdSql.execute("reset query cache")

        if major_ver == "3":
            tdSql.query(
                "select count(*) from (select distinct(tbname) from opentsdb_telnet.stb1)"
            )
        else:
            tdSql.query("select count(tbname) from opentsdb_telnet.stb1")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from opentsdb_telnet.stb1")
        tdSql.checkData(0, 0, 160)
        if major_ver == "3":
            tdSql.query(
                "select count(*) from (select distinct(tbname) from opentsdb_telnet.stb2)"
            )
        else:
            tdSql.query("select count(tbname) from opentsdb_telnet.stb2")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from opentsdb_telnet.stb2")
        tdSql.checkData(0, 0, 160)
        print("do telnet-tcp ......................... [passed]")

    #
    # ------------------- main ----------------
    #
    def do_taosadapter_json(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_rest_telnet.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f %s/json/sml_rest_line.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db2")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db2.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db2.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f %s/json/sml_rest_json.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db3")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db3.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db3.stb2")
        tdSql.checkData(0, 0, 160)

        print("do taosadapter support fun ............ [passed]")

    #
    # ------------------- main ----------------
    #
    def test_commandline_sml_rest(self):
        """taosBenchmark run with rest connect

        1. Verify "-I sml-rest -t 1 -n 1 -y"
        2. Verify "-I sml-rest-line -t 1 -n 1 -y"
        3. Verify "-I sml-rest-telnet -t 1 -n 1 -y"
        4. Verify "-I sml-rest-json -t 1 -n 1 -y"
        5. Verify "-I sml-rest-taosjson -t 1 -n 1 -y"
        6. Verify "-N -I sml-rest -y" negative case
        7. Run with rest insert all data-types
        8. Connect with telnet_tcp protocol
        9. Verify taosadapter with sml rest json/line/telnet

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_sml_rest.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_rest_insert_alltypes_json.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_telnet_tcp.py           
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosadapter_json.py

        """
        self.do_commandline_sml_rest()
        self.do_rest_insert_alltypes_json()
        self.do_telnet_tcp()
        self.do_taosadapter_json()