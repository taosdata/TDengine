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

"""Oracle RAW window ordering with local and remote sorts.

Catalog: - Query:FederatedResultParity

Since: v3.4.4.0

Labels: common,ci

Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7174122374

History:
    - 2026-11-20 Add Oracle binary ordering coverage
"""
from contextlib import ExitStack
from uuid import uuid4

from federated_query_common import ExtSrcEnv, FederatedWindowOrderTestMixin


class TestFqOracleWindowOrder(FederatedWindowOrderTestMixin):
    PROVIDER = "oracle"

    def test_raw_window_sort(self):
        """Preserve RAW prefixes and peers when sorting in Oracle.

        Compare the remote plans and per-row results for both settings of ORDER.
        Include a shorter prefix, bytes following NUL, and repeated keys.

        Catalog: - Query:FederatedResultParity

        Since: v3.4.4.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7174122374

        History:
            - 2026-11-20 Add RAW window sort regression
        """
        cfg = self._oracle_cfg()
        table = f"fq_window_{uuid4().hex[:16]}"
        ExtSrcEnv.oracle_exec_cfg(cfg, [
            f'CREATE TABLE "{table}" ("ts" TIMESTAMP(3) PRIMARY KEY, "k" RAW(8) NOT NULL)',
        ])
        with ExitStack() as cleanup:
            cleanup.callback(ExtSrcEnv.oracle_drop_table_cfg, cfg, table)
            inserts = [
                f"""INSERT INTO "{table}" VALUES (TIMESTAMP '2024-01-01 00:00:0{sec}', HEXTORAW('{key}'))"""
                for sec, key in enumerate(("4100", "410061", "41", "4100"))
            ]
            ExtSrcEnv.oracle_exec_cfg(cfg, [*inserts, "COMMIT"])
            self._create_window_source(cleanup, cfg, database=cfg.service, schema=cfg.user.upper())
            sql = f"FROM {self.source}.{table}"
            self._check_window(f"SELECT ts, ROW_NUMBER() OVER (ORDER BY k, ts) {sql}",
                               [(0, 2), (1, 4), (2, 1), (3, 3)], True)
            self._check_window(f"SELECT ts, DENSE_RANK() OVER (ORDER BY k) {sql}",
                               [(0, 2), (1, 3), (2, 1), (3, 2)], True)
