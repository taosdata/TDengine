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

"""PostgreSQL bytea and UUID window ordering with local and remote sorts.

Catalog: - Query:FederatedResultParity

Since: v3.4.4.0

Labels: common,ci

Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7174122374

History:
    - 2026-11-20 Add PostgreSQL binary ordering coverage
"""
from contextlib import ExitStack
from uuid import uuid4

from federated_query_common import ExtSrcEnv, FederatedWindowOrderTestMixin


class TestFqPgWindowOrder(FederatedWindowOrderTestMixin):
    PROVIDER = "pg"

    def test_bytea_window_sort(self):
        """Preserve bytea ordering and rank when sorting in PostgreSQL.

        Include byte prefixes, repeated UUIDs, and NULL and non-NULL partitions.
        Check the remote ORDER clause and row ranks with pushdown on and off.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.4.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7174122374

        History:
            - 2026-11-20 Add bytea window sort regression
        """
        database = f"fq_pg_window_{uuid4().hex}"
        ExtSrcEnv.pg_exec("postgres", [f'CREATE DATABASE "{database}"'])
        with ExitStack() as cleanup:
            cleanup.callback(ExtSrcEnv.pg_drop_db, database)
            ExtSrcEnv.pg_exec(database, [
                "CREATE TABLE window_keys (ts TIMESTAMP(3) PRIMARY KEY, b BYTEA NOT NULL, part BYTEA, "
                "u UUID NOT NULL, m MACADDR NOT NULL, m8 MACADDR8 NOT NULL)",
                "INSERT INTO window_keys VALUES "
                "('2024-01-01 00:00:00', decode('4100', 'hex'), NULL, 'a0000000-0000-0000-0000-000000000001', "
                "'0a:00:00:00:00:01', '0a:00:00:00:00:00:00:01'), "
                "('2024-01-01 00:00:01', decode('410061', 'hex'), decode('61','hex'), '90000000-0000-0000-0000-000000000001', "
                "'09:ff:ff:ff:ff:ff', '09:ff:ff:ff:ff:ff:ff:ff'), "
                "('2024-01-01 00:00:02', decode('41', 'hex'), NULL, 'A0000000-0000-0000-0000-000000000001', "
                "'0a:00:00:00:00:01', '0a:00:00:00:00:00:00:01'), "
                "('2024-01-01 00:00:03', decode('4200','hex'), decode('61','hex'), 'a0000000-0000-0000-0000-000000000002', "
                "'0a:00:00:00:00:02', '0a:00:00:00:00:00:00:02')",
            ])
            self._create_window_source(cleanup, self._pg_cfg(), database=database)
            expected = [(0, 2), (1, 3), (2, 1), (3, 4)]
            for window in ("ROW_NUMBER() OVER (ORDER BY b, ts)", "DENSE_RANK() OVER (ORDER BY b)"):
                self._check_window(f"SELECT ts, {window} FROM {self.source}.window_keys", expected, True)
            table = f"{self.source}.window_keys"
            self._check_window(f"SELECT ts, ROW_NUMBER() OVER (PARTITION BY part ORDER BY ts) FROM {table}",
                               [(0, 1), (1, 1), (2, 2), (3, 2)], True)
            for key in ("u", "m", "m8"):
                self._check_window(f"SELECT ts, ROW_NUMBER() OVER (ORDER BY {key}, ts) FROM {table}",
                                   [(0, 2), (1, 1), (2, 3), (3, 4)], True)
                self._check_window(f"SELECT ts, DENSE_RANK() OVER (ORDER BY {key}) FROM {table}",
                                   [(0, 2), (1, 1), (2, 2), (3, 3)], True)
