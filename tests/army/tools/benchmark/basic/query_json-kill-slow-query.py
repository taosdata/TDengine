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
        [TD-11510] taosBenchmark test cases
        """



    def run(self):
        binPath = etool.benchMarkFile()

        cmd = (
            "%s -b binary,nchar,binary,nchar,binary,nchar,binary,nchar,binary,nchar,binary,nchar -t 2 -n 50000 -I stmt -y > /dev/null"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        cmd = 'taos -s "select * from test.meters limit 1000000 offset 0" > /dev/null &'
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        cmd = "%s -f ./tools/benchmark/basic/json/taosc_query-kill-slow-query.json" % binPath
        tdLog.info("%s" % cmd)
        output = subprocess.check_output(cmd, shell=True).decode("utf-8")
        if "KILL QUERY" not in output:
            tdLog.info(output)
            tdLog.exit("KILL QUERY failed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
