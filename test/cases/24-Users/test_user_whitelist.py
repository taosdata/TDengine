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


class TestUserWhiteList:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_whitelist(self):
        """Whitelist: basic test

        Verify basic usage of whitelist functionality, including creation and display operations.

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci,integration,functional,security
        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/whitelist.sim

        """

        tdLog.info(f"============= step1")
        tdSql.execute(
            f"create user u_read pass 'taosdata1xad@#' host '127.0.0.1/24','192.168.1.0/24'"
        )
        tdSql.execute(
            f"create user u_write pass 'taosdata1TadBD' host '127.0.0.1','192.168.1.0'"
        )

        tdSql.execute(f"alter user u_read add host '3.3.3.4/24'")
        tdSql.execute(f"alter user u_write drop host '4.4.4.5/25'")

        tdSql.query(f"show users")
        tdSql.checkRows(3)

        tdLog.info(f"============= step2")
        tdSql.error(f"create user read1 pass 'taosdata1XR' host '127.0.0/24'")
        tdSql.error(f"create user write1 pass 'TZtaosdata1' host '4.4.4.4/33'")

        tdSql.query(f"show users")
        tdSql.checkRows(3)
