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

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import os


class TestInsertBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_basic(self):
        """insert use ns precision

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/basic.sim

        """

        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table tb(ts timestamp,c1 double);")

        csvPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv")
        tdLog.debug(f"csvPath: {csvPath}")

        csv1 = os.path.join(csvPath, "1.csv").replace("\\", "/")
        csv2 = os.path.join(csvPath, "2.csv").replace("\\", "/")
        csv3 = os.path.join(csvPath, "3.csv").replace("\\", "/")

        tdSql.error(f"insert into tb file '{csv1}';", expectErrInfo="syntax err", fullMatched=False)
        tdSql.error(f"insert into tb file '{csv2}';", expectErrInfo="syntax err", fullMatched=False)
        tdSql.execute(f"insert into tb file '{csv3}';")
        tdSql.query("select * from tb;")
        tdSql.checkRows(10)
