# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

import taos
host="localhost"
port=6030
conn = None
try:
    conn = taos.connect(host=host,
                        port=port,
                        user="root",
                        password="taosdata")
    sql = "SELECT ts, current, location FROM power.meters limit 100"
    result = conn.query(sql)
    # Get data from result as list of tuple
    data = result.fetch_all()
    for row in data:
        print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to query data from power.meters, sql: {sql}, ErrMessage:{err}")
    raise err
finally:
    if conn:
        conn.close()
