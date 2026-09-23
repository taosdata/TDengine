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

import os
import taosrest


class CloudSql:
    def __init__(self):
        self._conn = None

    def connect(self):
        if self._conn is None:
            url = os.environ["TDENGINE_CLOUD_URL"]
            token = os.environ["TDENGINE_CLOUD_TOKEN"]
            self._conn = taosrest.connect(url=url, token=token)
        return self._conn

    def count(self, database, table):
        conn = self.connect()
        res = conn.query(f"select count(*) from {database}.`{table}`")
        if res.rows > 0:
            return res.data[0][0]
