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

import taosws
from sqlalchemy import create_engine
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
import time

# Create a SQLAlchemy engine with WebSocket connection
# If using native connection, use `taos` instead of `taosws`
engine = create_engine(url="taosws://root:taosdata@?hosts=localhost:6041,127.0.0.1:6041&timezone=Asia/Shanghai", pool_size=10, max_overflow=20)

def init_db():
    try:
        with engine.begin() as conn:
            # create database
            conn.execute(text("DROP DATABASE IF EXISTS power"))
            conn.execute(text("CREATE DATABASE IF NOT EXISTS power PRECISION 'ms'"))
            print(f"Create database power successfully")

            # create super table
            conn.execute(
                text("CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(64))")
            )
            print(f"Create stable power.meters successfully")

    except Exception as err:
        print(f"Failed to create db and table; ErrMessage:{err}")
        raise

def ws_insert_sql(i: int, base_ts: int):
    try:
        with engine.begin() as conn:
            sql = text(f"""
                INSERT INTO
                power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
                    VALUES ({base_ts+i+1}, 10.30000, 219, 0.31000)
                    ({base_ts+i+2}, 12.60000, 218, 0.33000) ({base_ts+i+3}, 12.30000, 221, 0.31000)
                power.d1002 USING power.meters (groupid, location)  TAGS(3, 'California.SanFrancisco')
                    VALUES ({base_ts+i+1}, 10.30000, 218, 0.25000)
                """)
            affectedRows = conn.execute(sql)
            print(f"Successfully inserted {affectedRows.cursor.row_count} rows to power.meters.")

    except Exception as err:
        print(f"Failed to insert data to power.meters; ErrMessage:{err}")
        raise

# Use connection pool to execute queries
def ws_query(sql: str):
    try:
        # Get connection from pool
        with engine.begin() as conn:
            # Execute SQL
            result = conn.execute(text(sql))
            # Get results
            data = result.fetchall()
            print(f"Query result: {data}")
            return data
    except Exception as e:
        print(f"TDengine query failed: {e}")
        raise

def main():
    try:
        init_db()
        # Use one millisecond base for all writers. Per-query NOW values can
        # make different writers collide on the same table/timestamp key.
        base_ts = time.time_ns() // 1_000_000
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = []
            for i in range(5):
                futures.append(pool.submit(ws_insert_sql, i * 10, base_ts))
                futures.append(pool.submit(ws_query, "SELECT * FROM power.meters"))
            # Propagate worker errors to the main thread and wait for all writes
            # before checking the final count.
            for future in futures:
                future.result()

        data = ws_query("SELECT count(*) FROM power.meters")
        if data[0][0] != 20:
            raise RuntimeError(f"Expected 20 rows in power.meters, got {data[0][0]}")
        print("All sub-threads completed, main thread ending")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
