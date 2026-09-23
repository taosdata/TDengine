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

from new_test_framework.utils import tdLog, tdSql


TSDB_CODE_SNODE_NO_AVAILABLE_NODE = 0x042C


class TestTsmaNoSnode:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_data(self):
        tdSql.execute("drop database if exists tsma_no_snode")
        tdSql.execute("create database tsma_no_snode")
        tdSql.execute("create table tsma_no_snode.meters(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table tsma_no_snode.t1 using tsma_no_snode.meters tags(1)")
        tdSql.execute("insert into tsma_no_snode.t1 values(now, 1)")

    def do_create_tsma_without_snode(self):
        self.prepare_data()
        tdSql.error(
            "create tsma tsma1 on tsma_no_snode.meters function(avg(c1)) interval(1m)",
            TSDB_CODE_SNODE_NO_AVAILABLE_NODE,
        )
        tdLog.info("create tsma without snode reports snode error")

    def test_tsma_no_snode(self):
        """Create TSMA without SNODE reports the SNODE error

        1. Create a database and stable without creating any SNODE.
        2. Verify CREATE TSMA fails with No Snode is available.

        Catalog:
            - TSMA

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-26 Simon Created

        """
        self.do_create_tsma_without_snode()
