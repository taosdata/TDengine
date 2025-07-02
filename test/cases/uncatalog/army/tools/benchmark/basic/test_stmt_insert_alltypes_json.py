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

class TestStmtInsertAlltypesJson:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """



    def test_stmt_insert_alltypes_json(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -f ./tools/benchmark/basic/json/stmt_insert_alltypes.json" % binPath
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

        tdLog.success("%s successfully executed" % __file__)


