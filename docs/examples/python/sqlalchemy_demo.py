import taosws
from sqlalchemy import create_engine
from sqlalchemy import text
import threading

# Create a SQLAlchemy engine with WebSocket connection
# If using native connection, use `taos` instead of `taosws`
engine = create_engine(url="taosws://root:taosdata@localhost:6041?timezone=Asia/Shanghai", pool_size=10, max_overflow=20)

def init_db():
    try:
        with engine.begin() as conn:
            # create database
            conn.execute(text("DROP DATABASE IF EXISTS power"))
            conn.execute(text("CREATE DATABASE IF NOT EXISTS power"))
            print(f"Create database power successfully")

            # create super table
            conn.execute(
                text("CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(64))")
            )
            print(f"Create stable power.meters successfully")

    except Exception as err:
        print(f"Failed to create db and table; ErrMessage:{err}")
        raise

def ws_insert_sql(i: int):
    try:
        with engine.begin() as conn:
            sql = text(f"""
                INSERT INTO 
                power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
                    VALUES (NOW + {i+1}a, 10.30000, 219, 0.31000) 
                    (NOW + {i+2}a, 12.60000, 218, 0.33000) (NOW + {i+3}a, 12.30000, 221, 0.31000)
                power.d1002 USING power.meters (groupid, location)  TAGS(3, 'California.SanFrancisco') 
                    VALUES (NOW + {i+1}a, 10.30000, 218, 0.25000)
                """)
            affectedRows = conn.execute(sql)
            print(f"Successfully inserted {affectedRows} rows to power.meters.")

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
    