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

conn = None
url="http://localhost:6041"
try:
    conn = taosrest.connect(url=url,
                   user="root",
                   password="taosdata",
                   timeout=30)

    sql = """
        INSERT INTO 
        power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
            VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
            (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
        power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
            VALUES (NOW + 1a, 10.30000, 218, 0.25000)
        """
    affectedRows = conn.execute(sql)
    print(f"Successfully inserted {affectedRows} rows to power.meters.")

except Exception as err:
    print(f"Failed to insert data to power.meters, sql:{sql}, ErrMessage:{err}.")
    raise err
finally:
    if conn:
        conn.close()
