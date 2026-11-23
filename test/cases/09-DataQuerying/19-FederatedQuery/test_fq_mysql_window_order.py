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

"""MySQL window ranking with local and remote sort keys.

Catalog: - Query:FederatedResultParity

Since: v3.4.4.0

Labels: common,ci

Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7174122374

History:
    - 2026-11-20 Add embedded-NUL window ordering coverage
"""
from contextlib import ExitStack
from uuid import uuid4

from federated_query_common import ExtSrcEnv, FederatedWindowOrderTestMixin


class TestFqMysqlWindowOrder(FederatedWindowOrderTestMixin):
    PROVIDER = "mysql"

    def test_mysql_window_sort(self):
        """Preserve bytewise window order with MySQL ORDER enabled or disabled.

        Use a case-insensitive key with bytes after NUL and a timestamp tie
        breaker. Check each row number and the remote SQL for both settings.
        Compare indexable native binary keys with character keys and derived
        integer keys. Check right-hand JOIN keys and NULL placement for a
        nullable native binary column.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.4.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7174122374

        History:
            - 2026-11-20 Add MySQL window sort regression
        """
        database = f"fq_mysql_window_{uuid4().hex}"
        ExtSrcEnv.mysql_exec(None, [f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4"])
        with ExitStack() as cleanup:
            cleanup.callback(ExtSrcEnv.mysql_drop_db, database)
            ExtSrcEnv.mysql_exec(database, [
                "CREATE TABLE window_keys (ts DATETIME(3) PRIMARY KEY, id INT, "
                "k VARCHAR(8) CHARACTER SET ascii COLLATE ascii_general_ci, "
                "b VARBINARY(8) NOT NULL, payload BLOB, fixed_key BINARY(8) NOT NULL, "
                "f FLOAT NOT NULL, d DOUBLE NOT NULL, INDEX key_order (b, ts))",
                "INSERT INTO window_keys VALUES "
                "('2024-01-01 00:00:00', 2, 0x410061, 0x410061, 0x410061, 0x4100, 1.25, 1.25), "
                "('2024-01-01 00:00:01', 1, 0x410041, 0x410041, 0x410041, 0x410061, 0, 0), "
                "('2024-01-01 00:00:02', 1, 0x41004100, 0x41, 0x41, 0x41, 0, 0)",
                "CREATE TABLE window_peers (ts DATETIME(3) PRIMARY KEY, only_key VARBINARY(8) NOT NULL, "
                "b BLOB, n VARBINARY(8))",
                "INSERT INTO window_peers SELECT ts, b, IF(id = 2, 0x40, b), IF(id = 2, NULL, b) FROM window_keys",
            ])
            self._create_window_source(cleanup, self._mysql_cfg(), database=database)
            # Expected ranks are listed by timestamp, independent of result row order.
            cases = (
                ("ROW_NUMBER() OVER (ORDER BY k, ts)", (3, 1, 2), False),
                ("ROW_NUMBER() OVER (ORDER BY id, ts)", (3, 1, 2), True),
                ("ROW_NUMBER() OVER (ORDER BY b, ts)", (3, 2, 1), True),
                # BINARY(8) pads 0x4100 and 0x41 to the same eight bytes.
                ("ROW_NUMBER() OVER (ORDER BY fixed_key, ts)", (1, 3, 2), True),
                ("DENSE_RANK() OVER (ORDER BY fixed_key)", (1, 2, 1), True),
                ("ROW_NUMBER() OVER (ORDER BY payload, ts)", (3, 2, 1), False),
                ("DENSE_RANK() OVER (ORDER BY CASE id WHEN 1 THEN 2 ELSE 1 END)", (1, 2, 2), False),
            )
            for window, expected, pushable in cases:
                sql = f"SELECT ts, {window} FROM {self.source}.window_keys"
                self._check_window(sql, list(enumerate(expected)), pushable)

            for key in ("f", "d"):
                table = f"{self.source}.window_keys"
                self._check_window(f"SELECT ts, ROW_NUMBER() OVER (ORDER BY {key}, ts) FROM {table}",
                                   [(0, 3), (1, 1), (2, 2)], True)
                self._check_window(f"SELECT ts, DENSE_RANK() OVER (ORDER BY {key}) FROM {table}",
                                   [(0, 2), (1, 1), (2, 1)], True)
                sql = f"SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) FROM {table} PARTITION BY {key}"
                for enabled in (False, True):
                    remote, ranks = self._read_window(sql, enabled)
                    assert ranks == [(0, 1), (1, 1), (2, 2)] and len(remote) == 1, (sql, remote, ranks)
                    # The scan may still order its primary timestamp.
                    assert f"`{key}`" not in remote[0].partition("order by")[2], (sql, remote)

            peers = f"{self.source}.window_peers"
            joined = f"{self.source}.window_keys a JOIN {peers} r ON a.ts = r.ts"
            for key, expected, pushable in (("only_key", (3, 2, 1), True), ("b", (1, 3, 2), False)):
                sql = f"SELECT r.ts, ROW_NUMBER() OVER (ORDER BY r.{key}, r.ts) FROM {joined}"
                self._check_window(sql, list(enumerate(expected)), pushable)

            sql = f"SELECT ts, ROW_NUMBER() OVER (ORDER BY n, ts) FROM {peers} WHERE n IS NOT NULL"
            self._check_window(sql, [(1, 2), (2, 1)], True)
            for ordering, expected in (("ASC NULLS FIRST", (1, 3, 2)), ("ASC NULLS LAST", (3, 2, 1)),
                                       ("DESC NULLS FIRST", (1, 2, 3)), ("DESC NULLS LAST", (3, 1, 2))):
                sql = f"SELECT ts, ROW_NUMBER() OVER (ORDER BY n {ordering}, ts) FROM {peers}"
                self._check_window(sql, list(enumerate(expected)), True)
