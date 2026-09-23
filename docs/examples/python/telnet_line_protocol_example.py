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
from taos import SmlProtocol, SmlPrecision

# format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
lines = ["meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
         "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
         ]


# create connection use firstEp in taos.cfg.
def get_connection():
    return taos.connect()


def create_database(conn):
    conn.execute("CREATE DATABASE test keep 36500")
    conn.execute("USE test")


def insert_lines(conn):
    affected_rows = conn.schemaless_insert(
        lines, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
    print(affected_rows)  # 8


if __name__ == '__main__':
    connection = get_connection()
    try:
        create_database(connection)
        insert_lines(connection)
    finally:
        connection.close()
