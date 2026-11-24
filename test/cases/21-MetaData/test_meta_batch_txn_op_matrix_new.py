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
"""Batch meta txn: objects created in the txn then changed; refused DDL (m13 part 3, m14, m15).

5 scenarios of objects created inside the txn and then changed, plus m14 (options refused on virtual
tables, parent ALTER refused) and m15 (re-creating a dropped name refused).
The shared part (fixture, views, the scenario loop) is in meta_txn_matrix_common.py.
"""

import os
import sys

from new_test_framework.utils import tdLog, tdSql, tdCom

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from meta_txn_matrix_common import M13Common  # noqa: E402


class TestBatchMetaTxnOpMatrixNew(M13Common):

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.execute("drop database if exists txn_mx_db")
        tdSql.execute("create database txn_mx_db vgroups 2 keep 36500")
        tdSql.execute("use txn_mx_db")

    # name, DDLs run in the txn, tables created by them, super tables created by them
    SCENARIOS = [
        # ---- created inside the txn, then changed ----
        ("new vstb + child, then ALTER the vstb",
         ["create table ne_vstb (ts timestamp, c1 int, c2 int) tags(t1 int) virtual 1",
          "create vtable ne_v1 (c1 from txn_mx_db.src.c1) using ne_vstb tags(1)",
          "alter table ne_vstb add tag t2 int", "alter table ne_vstb add column x1 int",
          "alter table ne_vstb rename column c1 c1n"], ["ne_v1"], ["ne_vstb"]),
        ("new vntb, then change its columns and tags",
         ["create vtable ne_vn (ts timestamp, c1 int from txn_mx_db.src.c1) tags(own int = 1)",
          "alter table ne_vn add column c2 int", "alter table ne_vn alter column c2 set txn_mx_db.src.c2",
          "alter table ne_vn add tag t2 int", "alter table ne_vn rename column c1 c1n",
          "alter table ne_vn set tag own=2", "alter table ne_vn alter column c2 set null"], ["ne_vn"], []),
        ("new vchild with an own column and tag, then change it",
         ["create vtable ne_v1 (c1 from txn_mx_db.src.c1, own_c int from txn_mx_db.src.c3) using vstb "
          "(t1, t2, tv, own_t int) tags(1, 2, 'x', 3)",
          "alter table ne_v1 alter column own_c set txn_mx_db.src.c2", "alter table ne_v1 alter column c1 set null",
          "alter table ne_v1 set tag t1=5"], ["ne_v1"], []),
        ("two new vchildren in one statement, then ALTER the vstb",
         ["create vtable ne_a (c1 from txn_mx_db.src.c1) using vstb tags(11, 12, 'a') "
          "ne_b (c1 from txn_mx_db.src.c2) using vstb tags(13, 14, 'b')",
          "alter table vstb add column x1 int", "alter table vstb drop tag t1"], ["ne_a", "ne_b"], []),
        ("new vchild, ALTER the vstb, drop the vstb",
         ["create vtable ne_v1 (c1 from txn_mx_db.src.c1) using vstb tags(1, 2, 'a')",
          "alter table vstb add column x1 int", "drop table vstb"], ["ne_v1"], []),
    ]

    def m14_virtual_options_rejected_cleanly(self):
        """COMMENT / TTL are table options of normal and child tables only: on a virtual child or virtual normal
        table the vnode refuses them ("Invalid table action"). MODIFY TAG is refused on a virtual normal table by
        the parser. Inside a txn the refusal must come at once and must leave the txn usable: a later DDL of the
        same txn still commits and nothing of the rejected ones shows."""
        tdLog.info("m14: COMMENT / TTL on virtual child / virtual normal tables are rejected without poisoning the txn")
        self._m13_fixture()
        before = self._m13_snap(self.M13_BASE_TABLES, self.M13_BASE_STABLES)
        tdSql.execute("BEGIN")
        for ddl in ("alter table vc1 comment 'c'", "alter table vc1 ttl 100",
                    "alter table vn1 comment 'n'", "alter table vn1 ttl 50",
                    # MODIFY TAG is for super tables; a virtual normal table does not take it (parser: 0x2649)
                    "alter table vn1 modify tag lab nchar(30)",
                    # a parent VST (chi inherits from pa) cannot be reshaped: TSDB_CODE_MND_VST_HAS_CHILDREN
                    "alter table pa add column p2 int", "alter table pa add tag pt2 int",
                    "alter table pa drop column p1", "alter table pa drop tag pt"):
            tdSql.error(ddl)
        tdSql.execute("alter table vstb add column x1 int")
        tdSql.execute("COMMIT")
        assert any(r[0] == "x1" for r in self._m13_q("describe vc1")), "the accepted DDL of the txn must commit"
        after = self._m13_snap(self.M13_BASE_TABLES, self.M13_BASE_STABLES)
        for key in ("ins_tables vc1", "ins_tables vn1"):
            assert after[key] == before[key], f"{key}: a rejected COMMENT / TTL changed something: {after[key]}"
        tdLog.info("m14 PASSED: rejected virtual options / parent ALTER leave the txn usable")

    def m15_virtual_recreate_same_name_rejected(self):
        """An object dropped in a txn cannot be re-created under the same name in that txn (0x3320, the message
        says to pick another name or COMMIT/ROLLBACK first). The refusal must not poison the txn: the drop stays,
        and a different name works."""
        tdLog.info("m15: dropping then re-creating the same virtual table name is refused, a new name is fine")
        for drop, create, name in (
            ("drop vtable vc3", "create vtable vc3 (c1 from txn_mx_db.src.c2) using vstb tags(60, 70, 'cc')", "vc3"),
            ("drop vtable vn1", "create vtable vn1 (ts timestamp, c1 int from txn_mx_db.src.c3) tags(own int = 1)",
             "vn1"),
        ):
            tdLog.info(f"m15: {drop} then {create.split(' (')[0]}")
            self._m13_fixture()
            tdSql.execute("BEGIN")
            tdSql.execute(drop)
            tdSql.error(create, expectedErrno=0x3320)
            assert self._m13_q(f"describe {name}") == "ERR", f"{name} must stay dropped inside the txn"
            tdSql.execute("COMMIT")
            assert self._m13_q(f"describe {name}") == "ERR", f"{name} must be gone after COMMIT"
            # and after the txn the name is free again
            tdSql.execute(create)
            assert self._m13_q(f"describe {name}") != "ERR", f"{name} must be creatable after the txn"
        tdLog.info("m15 PASSED: re-creating a dropped virtual table name in the same txn is refused")

    def test_meta_batch_txn_op_matrix_new(self):
        """Batch meta txn: objects created in the txn then changed, refused DDL (m13 part 3, m14, m15)

        m13. A virtual super table / child / normal table created in the txn and then changed: every view shows
             the txn's own change, identical to the state after COMMIT; ROLLBACK restores the state before.
        m14. COMMENT / TTL / MODIFY TAG refused on virtual child / normal tables, and ALTER of a parent virtual
             super table refused, without poisoning the txn
        m15. Drop then re-create the same virtual table name in one txn is refused (0x3320), the drop stays, a new
             name works

        Since: v3.3.6.0

        Labels: common,ci
        """
        self.run_scenarios(self.SCENARIOS)
        self.m14_virtual_options_rejected_cleanly()
        self.m15_virtual_recreate_same_name_rejected()

    @classmethod
    def teardown_class(cls):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("drop database if exists txn_mx_db")
        tdLog.debug("%s teardown done" % __file__)
