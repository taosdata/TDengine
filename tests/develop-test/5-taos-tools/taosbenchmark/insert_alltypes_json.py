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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        '''
        [TD-11510] taosBenchmark test cases
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/taosc_insert_alltypes.json"
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
        tdSql.checkData(14, 1, "BINARY")
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
        tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(28, 2, 19)

        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/sml_insert_alltypes.json"
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
        tdSql.checkData(8, 2, 29)
        tdSql.checkData(9, 1, "INT UNSIGNED")
        tdSql.checkData(10, 1, "BIGINT UNSIGNED")
        tdSql.checkData(11, 1, "TINYINT UNSIGNED")
        tdSql.checkData(12, 1, "SMALLINT UNSIGNED")
        tdSql.checkData(13, 1, "BINARY")
        tdSql.checkData(13, 2, 23)
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

        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/rest_insert_alltypes.json"
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
        tdSql.checkData(14, 1, "BINARY")
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
        tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(28, 2, 19)

        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/stmt_insert_alltypes.json"
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
        tdSql.checkData(14, 1, "BINARY")
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
        tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(28, 2, 19)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())