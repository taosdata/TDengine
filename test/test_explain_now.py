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

"""Tiny test that just runs EXPLAIN VERBOSE TRUE and prints."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "new_test_framework"))

from utils.sql import tdSql
import taos

class TestExplainNow:
    def test_explain(self):
        conn = taos.connect(config="/tmp/sim_fq_warm/dnode1/cfg")
        cur = conn.cursor()

        for db in ["fq_parity_local", "fq_parity_src_m"]:
            sql = "EXPLAIN VERBOSE TRUE SELECT NOW() FROM %s.parity_t LIMIT 1" % db
            print("\n" + "=" * 70)
            print(sql)
            print("=" * 70)
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                for row in rows:
                    print(row[0] if len(row) == 1 else " | ".join(str(c) for c in row))
            except Exception as e:
                print("ERROR: %s" % e)
        cur.close()
        conn.close()
