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
        [TD-11510] taosBenchmark test cases
        """



    def run(self):
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -F abc -P abc -I abc -T abc -i abc -S abc -B abc -r abc -t abc -n abc -l abc -w abc -w 16385 -R abc -O abc -a abc -n 2 -t 2 -r 1 -y"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 4)

        cmd = "%s non_exist_opt" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -f non_exist_file -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -h non_exist_host -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -p non_exist_pass -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -u non_exist_user -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
