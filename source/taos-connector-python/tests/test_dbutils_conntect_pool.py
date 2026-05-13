import taos
import threading
import utils
from dbutils.pooled_db import PooledDB
from utils import PORT


# Initialize TDengine connection pool
def init_native_pool():
    return PooledDB(
        creator=taos.connect,  # Fix: Use taos.connect instead of taos
        maxconnections=10,  # Maximum number of connections
        # TDengine connection parameters (modify according to actual environment)
        host="localhost",
        port=PORT,
        user=utils.test_username(),
        password=utils.test_password(),
    )


# Global connection pool instance
native_pool = init_native_pool()


def init_db():
    conn = None
    host = "localhost"
    port = PORT
    try:
        conn = native_pool.connection()
        cursor = conn.cursor()
        # create database
        cursor.execute("DROP DATABASE IF EXISTS power")  # Fix: DROP not Drop
        rowsAffected = cursor.execute("CREATE DATABASE IF NOT EXISTS power")
        print(f"Create database power successfully, rowsAffected: {rowsAffected}")
        assert rowsAffected == 0

        # create super table
        rowsAffected = cursor.execute(
            "CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(64))"
        )
        print(f"Create stable power.meters successfully, rowsAffected: {rowsAffected}")  # Fix: Remove semicolon

    except Exception as err:
        print(f"Failed to create db and table, db addr:{host}:{port} ; ErrMessage:{err}")
    finally:
        if conn:
            conn.close()


def taos_insert_sql(i: int):
    conn = None
    host = "localhost"
    port = PORT
    try:
        conn = native_pool.connection()
        cursor = conn.cursor()
        sql = f"""
            INSERT INTO 
            power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
                VALUES (NOW + {i + 1}a, 10.30000, 219, 0.31000) 
                (NOW + {i + 2}a, 12.60000, 218, 0.33000) (NOW + {i + 3}a, 12.30000, 221, 0.31000)
            power.d1002 USING power.meters (groupid, location)  TAGS(3, 'California.SanFrancisco') 
                VALUES (NOW + {i + 1}a, 10.30000, 218, 0.25000)
            """
        affectedRows = cursor.execute(sql)
        print(f"Successfully inserted {affectedRows} rows to power.meters.")

    except Exception as err:
        print(f"Failed to insert data to power.meters, db addr:{host}:{port} ; ErrMessage:{err}")
        raise
    finally:
        if conn:
            conn.close()


# Use connection pool to execute queries
def query_native(sql: str):
    conn = None
    cursor = None
    try:
        # Get connection from connection pool
        conn = native_pool.connection()
        # Create cursor
        cursor = conn.cursor()
        # Execute SQL
        cursor.execute(sql)
        # Get results
        data = cursor.fetchall()
        print(f"Query result: {data}")
        return data
    except Exception as e:
        print(f"TDengine query failed: {e}")
        raise "TDengine query failed!"
    finally:
        # Close cursor
        if cursor:
            cursor.close()
        # Return connection to pool (not actually closed, just marked as idle)
        if conn:
            conn.close()


# Example: Query tables in TDengine
def test_connection_pool():
    """Test basic functionality of connection pool"""
    init_db()  # Initialize database and tables

    # Query database list
    result = query_native("SHOW DATABASES")
    print("TDengine database list:", result)

    # Multithreaded concurrent test
    threads = []
    for i in range(5):
        t1 = threading.Thread(target=taos_insert_sql, args=(i * 10,), name=f"Insert-{i}")
        # Fix: Use COUNT(*) instead of SELECT * to avoid memory issues
        t2 = threading.Thread(target=query_native, args=("SELECT COUNT(*) FROM power.meters",), name=f"Query-{i}")
        threads.extend([t1, t2])
        t1.start()
        t2.start()

    # Main thread waits for all sub-threads to complete
    for t in threads:
        t.join()
        print(f"Thread {t.name} completed")

    # Final verification
    data = query_native("SELECT COUNT(*) FROM power.meters")
    if data and len(data) > 0:
        count = data[0][0]
        print(f"Total records: {count}")
        assert count == 20, f"Expected 20 rows in power.meters, but got {count}"

    print("All sub-threads completed, main thread ending")


if __name__ == "__main__":
    test_connection_pool()
