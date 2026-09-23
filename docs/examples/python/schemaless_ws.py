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

host = "localhost"
port = 6041
def prepare():
    conn = None
    try:
        conn = taosws.connect(user="root",
                            password="taosdata",
                            host=host,
                            port=port)

        # create database
        rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
        assert rowsAffected == 0

    except Exception as err:
        print(f"Failed to create db and table, db addrr:{host}:{port} ; ErrMessage:{err}")
        raise err
    finally:
        if conn:
            conn.close()

def schemaless_insert():

    conn = None

    lineDemo = [
        "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
    ]

    telnetDemo = ["metric_telnet 1707095283260 4 host=host0 interface=eth0"]

    jsonDemo = [
        '{"metric": "metric_json","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}'
    ]

    try:
        conn = taosws.connect(user="root",
                              password="taosdata",
                              host=host,
                              port=port,
                              database='power')

        conn.schemaless_insert(
            lines = lineDemo,
            protocol = taosws.PySchemalessProtocol.Line,
            precision = taosws.PySchemalessPrecision.Millisecond,
            ttl=1,
            req_id=1,
        )

        conn.schemaless_insert(
            lines=telnetDemo,
            protocol=taosws.PySchemalessProtocol.Telnet,
            precision=taosws.PySchemalessPrecision.Microsecond,
            ttl=1,
            req_id=2,
        )

        conn.schemaless_insert(
            lines=jsonDemo,
            protocol=taosws.PySchemalessProtocol.Json,
            precision=taosws.PySchemalessPrecision.Millisecond,
            ttl=1,
            req_id=3,
        )
        print("Inserted data with schemaless successfully.");
    except Exception as err:
        print(f"Failed to insert data with schemaless, ErrMessage:{err}")
        raise err
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    prepare()
    schemaless_insert()
  
