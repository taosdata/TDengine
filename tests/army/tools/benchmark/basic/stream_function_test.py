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
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        [TD-21047] taosBenchmark stream test cases
        """



    def run(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/stream_exist_stb_tag_prepare.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        cmd = "%s -f ./tools/benchmark/basic/json/stream_exist_stb_tag_insert_partition.json " % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from stream_test.stb")
        tdSql.checkData(0, 0, 100000)
        tdSql.query("select count(*) from stream_test.output_streamtb;")
        tdSql.checkEqual(tdSql.res[0][0] >= 0, True)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
