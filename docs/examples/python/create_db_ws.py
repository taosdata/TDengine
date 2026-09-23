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
host = "localhost"
port = 6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
    print(f"Create database power successfully, rowsAffected: {rowsAffected}");
    
    # create super table
    rowsAffected = conn.execute(
        "CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )
    print(f"Create stable power.meters successfully, rowsAffected: {rowsAffected}");

except Exception as err:
    print(f"Failed to create database power or stable meters, ErrMessage:{err}") 
    raise err
finally:
    if conn:
        conn.close()
