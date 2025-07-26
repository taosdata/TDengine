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

class TestStreamFunctionTest:
    def caseDescription(self):
        """
        [TD-21047] taosBenchmark stream test cases
        """



    def test_stream_function_test(self):
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
        cmd = "%s -f %s/json/stream_exist_stb_tag_prepare.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        cmd = "%s -f %s/json/stream_exist_stb_tag_insert_partition.json " % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from stream_test.stb")
        tdSql.checkData(0, 0, 100000)
        tdSql.query("select count(*) from stream_test.output_streamtb;")
        tdSql.checkEqual(tdSql.queryResult[0][0] >= 0, True)

        tdLog.success("%s successfully executed" % __file__)


