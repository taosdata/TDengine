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

class TestSmlTelnetInsertAlltypesSameMinMax:
    def caseDescription(self):
        """
        [TD-23292] taosBenchmark test cases
        """

    def test_sml_telnet_insert_alltypes_same_min_max(self):
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
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
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
        rows = tdSql.res[0]
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

        tdLog.success("%s successfully executed" % __file__)


