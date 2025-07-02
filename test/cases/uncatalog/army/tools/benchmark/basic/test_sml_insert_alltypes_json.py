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

class TestSmlInsertAlltypesJson:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def test_sml_insert_alltypes_json(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -f ./tools/benchmark/basic/json/sml_insert_alltypes.json" % binPath
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
        # sml nchar and binary length will auto changed in 3.0
        # tdSql.checkData(8, 2, 32)
        tdSql.checkData(9, 1, "INT UNSIGNED")
        tdSql.checkData(10, 1, "BIGINT UNSIGNED")
        tdSql.checkData(11, 1, "TINYINT UNSIGNED")
        tdSql.checkData(12, 1, "SMALLINT UNSIGNED")
        # binary/varchar diff in 2.x/3.x
        # tdSql.checkData(13, 1, "BINARY")
        # sml nchar and binary length will auto changed in 3.0
        # tdSql.checkData(13, 2, 32)
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

        tdLog.success("%s successfully executed" % __file__)


