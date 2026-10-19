# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("DROP DATABASE IF EXISTS test")
cursor.execute("CREATE DATABASE test keep 36500")
cursor.execute("USE test")
cursor.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")

for i in range(1000):
    location = str(i % 10)
    tb = "t" + location
    cursor.execute(f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)")

cursor.execute("SELECT count(*) FROM weather")
data = cursor.fetchall()
print("count:", data[0][0])
cursor.execute("SELECT tbname, * FROM weather LIMIT 2")
col_names = [meta[0] for meta in cursor.description]
print(col_names)
rows = cursor.fetchall()
print(rows)

cursor.close()
conn.close()

# output:
# count: 2000
# ['tbname', 'ts', 'temperature', 'location']
# row_count: -1
# [('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 392000), 23.5, 0), ('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 393000), 23.5, 0)]
