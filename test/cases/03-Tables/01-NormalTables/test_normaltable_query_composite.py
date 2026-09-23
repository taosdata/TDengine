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

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCompositeKeyLoad:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_composite_key_load(self):
        """Normal table composite key

        Error Reading Composite Key Data from Memory and STT Files

        Root Cause:
        The minKeyused a shallow copy. During backward iteration in STT, this accidentally modified the minKeyvalue. When merging keys from memory and STT, inconsistent key states caused data corruption.

        Reproduction Steps:
        1. Create table with timestamp column and composite key string column. Insert 4 records into STT files and 1 record into memory.
        2. Execute query → Trigger failure

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: TS-6787

        History:
            - 2025-07-04 Haojun Liao

        """

        dbPrefix = "m_si_db"

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table t(ts timestamp, k varchar(12) primary key)")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00001')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00003')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00005')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00007')")
        tdSql.execute(f"flush database {db}; ")

        time.sleep(5)
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00005')")

        tdLog.info("insert completed, check results")
        tdSql.query("select * from t")

        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 'abcd-00001')
        tdSql.checkData(1, 1, 'abcd-00003')
        tdSql.checkData(2, 1, 'abcd-00005')
        tdSql.checkData(3, 1, 'abcd-00007')

        print(f"=============== {tdSql.getData(1,1)}")

