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
        cmd = "taosBenchmark -F abc -P abc -I abc -T abc -i abc -S abc -B abc -r abc -t abc -n abc -l abc -w abc -w 16385 -R abc -O abc -a abc -n 2 -t 2 -r 1 -y"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 4)

        cmd = "taosBenchmark non_exist_opt"
        tdLog.info("%s" % cmd)
        assert (os.system("%s" % cmd) != 0)

        cmd = "taosBenchmark -f non_exist_file"
        tdLog.info("%s" % cmd)
        assert (os.system("%s" % cmd) != 0)

        cmd = "taosBenchmark -h non_exist_host"
        tdLog.info("%s" % cmd)
        assert (os.system("%s" % cmd) != 0)

        cmd = "taosBenchmark -p non_exist_pass"
        tdLog.info("%s" % cmd)
        assert (os.system("%s" % cmd) != 0)

        cmd = "taosBenchmark -u non_exist_user"
        tdLog.info("%s" % cmd)
        assert (os.system("%s" % cmd) != 0)

        cmd = "taosBenchmark -c non_exist_dir -n 1 -t 1 -o non_exist_path -y"
        tdLog.info("%s" % cmd)
        assert (os.system("%s" % cmd) == 0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())