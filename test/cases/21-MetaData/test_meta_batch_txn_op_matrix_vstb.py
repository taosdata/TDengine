# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
"""Batch meta txn: every DDL on a virtual super table, seen through every view (m13, part 1/3).

13 scenarios: ALTER of a virtual super table (columns, tags, comment) and of a virtual super table that
inherits from another one. Each is checked inside the txn, after COMMIT and after ROLLBACK.
The shared part (fixture, views, the scenario loop) is in meta_txn_matrix_common.py.
"""

import os
import sys

from new_test_framework.utils import tdLog, tdSql, tdCom

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from meta_txn_matrix_common import M13Common  # noqa: E402


class TestBatchMetaTxnOpMatrixVstb(M13Common):

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_mx_db")
        tdSql.execute("create database txn_mx_db vgroups 2 keep 36500")
        tdSql.execute("use txn_mx_db")

    # name, DDLs run in the txn, tables created by them, super tables created by them
    SCENARIOS = [
        # ---- virtual super table ----
        ("vstb: add column", ["alter table vstb add column x1 int"], [], []),
        ("vstb: drop a column all children bind", ["alter table vstb drop column c1"], [], []),
        ("vstb: drop a column only one child binds", ["alter table vstb drop column c2"], [], []),
        ("vstb: rename a bound column", ["alter table vstb rename column c1 c1n"], [], []),
        ("vstb: modify column length", ["alter table vstb modify column v varchar(50)"], [], []),
        ("vstb: modify column compress",
         ["alter table vstb modify column c1 encode 'simple8b' compress 'zlib' level 'high'"], [], []),
        ("vstb: add tag", ["alter table vstb add tag t3 int"], [], []),
        ("vstb: drop tag", ["alter table vstb drop tag t1"], [], []),
        ("vstb: rename tag", ["alter table vstb rename tag t2 t2n"], [], []),
        ("vstb: modify tag length", ["alter table vstb modify tag tv varchar(50)"], [], []),
        ("vstb: comment", ["alter table vstb comment 'hello'"], [], []),
        ("vstb: several ALTERs", ["alter table vstb add column x1 int", "alter table vstb drop column c2",
                                  "alter table vstb rename tag t2 t2n", "alter table vstb add tag t3 int",
                                  "alter table vstb drop tag t1", "alter table vstb modify tag tv varchar(50)"],
         [], []),
        # ---- inheritance ----
        ("inherit: DROP BASE ON", ["alter stable chi drop base on txn_mx_db.pa"], [], []),
    ]

    def test_meta_batch_txn_op_matrix_vstb(self):
        """Batch meta txn: virtual super table DDL, every view matches COMMIT (m13 part 1)

        Every ALTER of a virtual super table / inheritance: DESCRIBE, SHOW CREATE, SHOW TAGS, ins_columns /
        ins_tags / ins_tables / ins_stables / ins_virtual_tables_referencing (by table name and as full scans) and
        SELECT show the txn's own change inside the txn, identical to the state after COMMIT; ROLLBACK restores the
        state before the txn.

        Since: v3.3.6.0

        Labels: common,ci
        """
        self.run_scenarios(self.SCENARIOS)
        tdLog.info("m13 (vstb) PASSED")

    @classmethod
    def teardown_class(cls):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("drop database if exists txn_mx_db")
        tdLog.debug("%s teardown done" % __file__)
