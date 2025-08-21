import threading
from dbutils.pooled_db import PooledDB
import taosws

def init_websocket_pool():
    return PooledDB(
        # Set connector driver. If using native mode, please replace taosws with taos
        creator=taosws,
        # Maximum number of connections  
        maxconnections=10,  
        # TDengine connection parameters (modify according to actual environment)
        host="localhost",
        port=6041,
        user="root",
        password="taosdata",
        charset="UTF-8"
    )

websocket_pool = init_websocket_pool()

def init_db():
    try:
        conn = websocket_pool.connection()
        cursor = conn.cursor()
        # create database
        cursor.execute(f"Drop DATABASE IF EXISTS power")
        rowsAffected = cursor.execute(f"CREATE DATABASE IF NOT EXISTS power")
        print(f"Create database power successfully, rowsAffected: {rowsAffected}")
        assert rowsAffected == 0

        # create super table
        rowsAffected = cursor.execute(
            "CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(64))"
        )
        print(f"Create stable power.meters successfully, rowsAffected: {rowsAffected}");

    except Exception as err:
        print(f"Failed to create db and table; ErrMessage:{err}")
    finally:
        if conn:
            conn.close()

def ws_insert_sql(i: int):
    conn = None
    try:
        conn = websocket_pool.connection()
        cursor = conn.cursor()
        sql = f"""
            INSERT INTO 
            power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
                VALUES (NOW + {i+1}a, 10.30000, 219, 0.31000) 
                (NOW + {i+2}a, 12.60000, 218, 0.33000) (NOW + {i+3}a, 12.30000, 221, 0.31000)
            power.d1002 USING power.meters (groupid, location)  TAGS(3, 'California.SanFrancisco') 
                VALUES (NOW + {i+1}a, 10.30000, 218, 0.25000)
            """
        affectedRows = cursor.execute(sql)
        print(f"Successfully inserted {affectedRows} rows to power.meters.")

    except Exception as err:
        print(f"Failed to insert data to power.meters; ErrMessage:{err}")
    finally:
        if conn:
            conn.close()

# Execute queries using connection pool
def ws_query(sql: str):
    conn = None
    cursor = None
    try:
        # Get connection from pool
        conn = websocket_pool.connection()
        # Create cursor
        cursor = conn.cursor()
        # Execute SQL
        cursor.execute(sql)
        # Get results
        data = cursor.fetchall()
        print(data)
        return data
    except Exception as e:
        print(f"TDengine query failed: {e}")
        raise
    finally:
        # Close cursor
        if cursor:
            cursor.close()
        # Return connection to pool (not actually closed, just marked as idle)
        if conn:
            conn.close()


if __name__ == "__main__":
    init_db()  # Initialize database and tables
    threads = []
    for i in range(5):
        t1 = threading.Thread(target=ws_insert_sql, args=(i*10,))
        t2 = threading.Thread(target=ws_query, args=("SELECT * FROM power.meters",))
        threads.extend([t1, t2])
        t1.start()
        t2.start()

    for t in threads:
        t.join()

    data = ws_query("SELECT count(*) FROM power.meters")
    assert data[0][0] == 20, "Expected 20 rows in power.meters"
    print("All sub-threads completed, main thread ending")