# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPassword:
    updatecfgDict = {'encryptPassAlgorithm':'sm4'}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_password(self):
        """Password: crypted password

        Create encrypt_key and test to create user and login with crypted password

        Catalog:
            - User

        Since: v3.3.7.0

        Labels: common,ci,integration,functional,security
        Jira: TS-5619

        History:
            - 2025-6-27 init 

        """
        time.sleep(3)
        tdSql.query(f"show dnodes")
        tdSql.checkRows(1, True)
        tdSql.checkData(0, 4, "ready")

        tdLog.info(f"============= step1")
        tdSql.execute("create encrypt_key '12345678'", queryTimes = 1, show = True)
        time.sleep(3)
        tdSql.query(f"show encryptions")
        tdSql.checkRows(1, True)
        tdSql.checkData(0, 1, "loaded")

        tdLog.info(f"============= step2")
        tdSql.execute(f"create user u_test pass 'AAbb1122'", queryTimes = 1, show = True)

        tdSql.query(f"show users")
        tdSql.checkRows(2, True)

        tdLog.info(f"============= step3")
        tdLog.info(f"user u_test login")
        tdSql.connect("u_test", "AAbb1122")