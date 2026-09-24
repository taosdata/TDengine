# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""
XNode restart command error-path coverage.

Verifies that RESTART XNODE reaches the mnode handler and reports the
credential-missing error when no xnoded credentials have been configured.
"""

import uuid

from new_test_framework.utils import tdLog, tdSql

TSDB_CODE_MND_XNODE_USER_PASS_NOT_EXIST = 0x800E


class TestXnodeRestart:
    """XNode restart command error-path tests"""

    replicaVar = 1

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.suffix = uuid.uuid4().hex[:6]
        tdSql.prepare(
            dbname=f"xnode_restart_db_{cls.suffix}",
            drop=True,
            replica=cls.replicaVar,
        )

    def test_restart_xnode_without_credentials(self):
        """RESTART XNODE fails cleanly when no xnoded credentials exist

        1. Execute RESTART XNODE as superuser on a fresh cluster
        2. Verify the credential-missing error is returned instead of a syntax error

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-21 Created
        """
        tdSql.error(
            "RESTART XNODE",
            expectedErrno=TSDB_CODE_MND_XNODE_USER_PASS_NOT_EXIST,
        )
        tdLog.success("Test 1 passed: RESTART XNODE reports missing xnoded credentials")
