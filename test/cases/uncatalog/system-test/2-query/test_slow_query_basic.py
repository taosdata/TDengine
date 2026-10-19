# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql, etool
from new_test_framework.utils.pathFinding import find_proj_path

import string
import platform
import os

class TestSlowQueryBasic:
    updatecfgDict = {'slowLogThresholdTest': ''}
    updatecfgDict["slowLogThresholdTest"]  = 0
    
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        projPath = find_proj_path(selfPath)

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def taosBenchmark(self, param):
        binPath = self.getPath()
        cmd = f"{binPath} {param}"
        tdLog.info(cmd)
        os.system(cmd)
        
    def checkSlowQuery(self):
        etool.benchMark(command =" -d db -t 2 -v 2 -n 1000000 -y")
        sql = "select count(*) from db.meters"
        for i in range(10):            
            tdSql.query(sql)
            tdSql.checkData(0, 0, 2 * 1000000)

    def test_slow_query_basic(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: common,ci,integration,functional
        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        self.checkSlowQuery()

        #tdSql.close()

