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

import pandas
from sqlalchemy import create_engine, text

engine = create_engine("taosrest://root:taosdata@localhost:6041")
conn = engine.connect()
df: pandas.DataFrame = pandas.read_sql(text("SELECT * FROM power.meters"), conn)
conn.close()

# print index
print(df.index)
# print data type  of element in ts column
print(type(df.ts[0]))
print(df.head(3))

# output:
# RangeIndex(start=0, stop=8, step=1)
# <class 'pandas._libs.tslibs.timestamps.Timestamp'>
#                                 ts  current  ...               location  groupid
# 0 2018-10-03 06:38:05.500000+00:00     11.8  ...  california.losangeles        2
# 1 2018-10-03 06:38:16.600000+00:00     13.4  ...  california.losangeles        2
# 2        2018-10-03 06:38:05+00:00     10.8  ...  california.losangeles        3
