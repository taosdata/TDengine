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
"""Batch meta txn: every DDL on a virtual child / virtual normal table, seen through every view (m13, part 2/3).

24 scenarios: ALTER / DROP of a virtual child table and of a virtual normal table (columns, tags,
references). Each is checked inside the txn, after COMMIT and after ROLLBACK.
The shared part (fixture, views, the scenario loop) is in meta_txn_matrix_common.py.
"""

import os
import sys

from new_test_framework.utils import tdLog, tdSql, tdCom

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from meta_txn_matrix_common import M13Common  # noqa: E402


class TestBatchMetaTxnOpMatrixVchild(M13Common):

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_mx_db")
        tdSql.execute("create database txn_mx_db vgroups 2 keep 36500")
        tdSql.execute("use txn_mx_db")

    # name, DDLs run in the txn, tables created by them, super tables created by them
    SCENARIOS = [
        # ---- virtual child table ----
        ("vchild: SET TAG literal", ["alter table vc2 set tag t1=9"], [], []),
        ("vchild: SET TAG varchar", ["alter table vc1 set tag tv='zz'"], [], []),
        ("vchild: SET TAG to a reference", ["alter table vc1 set tag t1 = txn_mx_db.sc1.cnt"], [], []),
        ("vchild: ALTER COLUMN SET reference", ["alter table vc1 alter column c1 set txn_mx_db.src.c2"], [], []),
        ("vchild: ALTER COLUMN SET NULL", ["alter table vc1 alter column c2 set null"], [], []),
        ("vchild: rebind an own column", ["alter table vc2 alter column own_c set txn_mx_db.src.c1"], [], []),
        ("vchild: unbind an own column", ["alter table vc2 alter column own_c set null"], [], []),
        ("vchild: drop", ["drop vtable vc3"], [], []),
        ("vchild: drop one, ALTER the super table",
         ["drop vtable vc2", "alter table vstb add column x1 int"], [], []),
        ("vchild: drop one, create another under a different name",
         ["drop vtable vc3", "create vtable vc3b (c1 from txn_mx_db.src.c2) using vstb tags(60, 70, 'cc')"],
         ["vc3b"], []),
        # ---- virtual normal table ----
        ("vntb: add column and bind it",
         ["alter table vn1 add column c3 int", "alter table vn1 alter column c3 set txn_mx_db.src.c3"], [], []),
        ("vntb: drop column", ["alter table vn1 drop column c2"], [], []),
        ("vntb: rename column", ["alter table vn1 rename column c1 c1n"], [], []),
        ("vntb: rebind column", ["alter table vn1 alter column c1 set txn_mx_db.src.c3"], [], []),
        ("vntb: unbind column", ["alter table vn1 alter column c2 set null"], [], []),
        ("vntb: add tag", ["alter table vn1 add tag t9 int"], [], []),
        ("vntb: add tag with a reference", ["alter table vn1 add tag rt2 int from txn_mx_db.sc1.cnt"], [], []),
        ("vntb: drop literal tag", ["alter table vn1 drop tag own"], [], []),
        ("vntb: drop reference tag", ["alter table vn1 drop tag rt1"], [], []),
        ("vntb: rename tags", ["alter table vn1 rename tag own own_n", "alter table vn1 rename tag rt1 rt1n"], [], []),
        ("vntb: SET TAG literal", ["alter table vn1 set tag own=7"], [], []),
        ("vntb: SET TAG to another reference", ["alter table vn1 set tag rt1 = txn_mx_db.sc2.loc"], [], []),
        ("vntb: drop", ["drop vtable vn1"], [], []),
        ("vntb: drop one, create another under a different name",
         ["drop vtable vn1", "create vtable vn1b (ts timestamp, c1 int from txn_mx_db.src.c3) tags(own int = 1)"],
         ["vn1b"], []),
    ]

    def test_meta_batch_txn_op_matrix_vchild(self):
        """Batch meta txn: virtual child / normal table DDL, every view matches COMMIT (m13 part 2)

        Every DDL of a virtual child table and a virtual normal table: DESCRIBE, SHOW CREATE, SHOW TAGS,
        ins_columns / ins_tags / ins_tables / ins_stables / ins_virtual_tables_referencing (by table name and as
        full scans) and SELECT show the txn's own change inside the txn, identical to the state after COMMIT;
        ROLLBACK restores the state before the txn.

        Since: v3.3.6.0

        Labels: common,ci
        """
        self.run_scenarios(self.SCENARIOS)
        tdLog.info("m13 (vchild) PASSED")

    @classmethod
    def teardown_class(cls):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("drop database if exists txn_mx_db")
        tdLog.debug("%s teardown done" % __file__)
