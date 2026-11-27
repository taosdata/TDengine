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

import taosrest

client = None
url="http://localhost:6041"
reqId = 3
try:
    client = taosrest.RestClient(url=url,
                   user="root",
                   password="taosdata",
                   timeout=30)

    result = client.sql(f"SELECT ts, current, location FROM power.meters limit 100", reqId)
    if result["data"]:
        for row in result["data"]:
            print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to execute sql with reqId:{reqId}, ErrMessage:{err}")
    raise err
