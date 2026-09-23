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

import taosws

conn = None
reqId = 3
host="localhost"
port=6041
try:
    conn = taosws.connect(
        user="root",
        password="taosdata",
        host=host,
        port=port,
    )

    result = conn.query_with_req_id("SELECT ts, current, location FROM power.meters limit 100", req_id=3)
    # Get data from result as list of tuple
    for row in result:
        print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to execute sql with reqId:{reqId}, ErrMessage:{err}")
    raise err
finally:
    if conn:
        conn.close()
