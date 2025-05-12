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
import subprocess

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
        [TD-19449] taosBenchmark creating table interval test cases
        """



    def run(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f ./tools/benchmark/basic/json/taosc_insert_table-creating-interval.json -g 2>&1| grep sleep | wc -l"
            % binPath
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

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
