import warnings
from sqlalchemy.exc import SAWarning

# Suppress specific SQLAlchemy warnings
warnings.filterwarnings(
    "ignore",
    category=SAWarning,
    message="Dialect taosws:taosws will not make use of SQL compilation caching"
)

# ANCHOR: connect
import taosws
from sqlalchemy import create_engine
from sqlalchemy import text

def create_connection_with_sqlalchemy():
    

    engine = create_engine(url="taosws://root:taosdata@?hosts=localhost:6041,127.0.0.1:6041&timezone=Asia/Shanghai")
    conn = engine.connect()
    return conn
# ANCHOR_END: connect_sqlalchemy

def create_db_table(conn):
# ANCHOR: create_db
    try:
        conn.execute(text("DROP DATABASE IF EXISTS power"))
        conn.execute(text("CREATE DATABASE IF NOT EXISTS power"))
        conn.execute(text("USE power"))
        conn.execute(text("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"))
        conn.execute(text("CREATE TABLE  IF NOT EXISTS power.`d0` USING power.`meters` (location, groupId) TAGS('Los Angles', 0)"))
    except Exception as err:
        print(f'Exception {err}')
        raise err
# ANCHOR_END: create_db

def insert(conn):
# ANCHOR: insert
    sql = text("""
    INSERT INTO 
    power.d1001 USING power.meters (location, groupId) TAGS('California.SanFrancisco', 2)
        VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
        (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
    power.d1002 USING power.meters (location, groupId) TAGS('California.SanFrancisco', 3)
        VALUES (NOW + 1a, 10.30000, 218, 0.25000)
    """)
    try:
        affectedRows = conn.execute(sql)
        assert affectedRows.cursor.row_count == 4
    except Exception as err:
        print(f'Exception {err}')
        raise err
# ANCHOR_END: insert

def query(conn):
# ANCHOR: query
    try:
        result = conn.execute(text("select * from power.meters"))
        for row in result:
            print(row)    
    except Exception as err:
        print(f'Exception {err}')
        raise err
# ANCHOR_END: query

if __name__ == "__main__":
    conn = create_connection_with_sqlalchemy()
    create_db_table(conn)
    insert(conn)
    query(conn)
    if conn:
        conn.close()
