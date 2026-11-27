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

from new_test_framework.utils import tdLog, tdSql
import time


class TestTimezoneConf:
    updatecfgDict = {"clientCfg" : {'timezone': 'UTC', 'charset': 'gbk'}, 'timezone': 'UTC-8', 'charset': 'gbk'}
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def timezone_check(self, timezone, sql):
        tdSql.query(sql)
        rows = tdSql.getRows()
        for i in range(rows):
            if tdSql.getData(i, 0) == "timezone" :
                if tdSql.getData(i, 1).find(timezone) == -1:
                    tdLog.exit("show timezone:%s != %s"%(tdSql.getData(i, 1), timezone))

    def charset_check(self, charset, sql):
        tdSql.query(sql)
        rows = tdSql.getRows()
        for i in range(rows):
            if tdSql.getData(i, 0) == "charset" :
                if tdSql.getData(i, 1).find(charset) == -1:
                    tdLog.exit("show charset:%s != %s"%(tdSql.getData(i, 1), charset))

    def test_timezone_conf(self):
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

        self.charset_check('gbk', "show local variables")
        self.charset_check('gbk', "show dnode 1 variables")
        self.timezone_check('UTC', "show local variables")
        # time.sleep(50)
        self.timezone_check('UTC-8', "show dnode 1 variables")

        #tdSql.close()

