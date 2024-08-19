# ANCHOR: connect
import taosws

def create_connection():
    conn = None
    host = "localhost"
    port = 6041
    try:
        conn = taosws.connect(
            user="root",
            password="taosdata",
            host=host,
            port=port,
        )
        print(f"Connected to {host}:{port} successfully.");
    except Exception as err:
        print(f"Failed to connect to {host}:{port} , ErrMessage:{err}")
   
    return conn
 # ANCHOR_END: connect

def create_db_table(conn):
# ANCHOR: create_db
    try:
        conn.execute("CREATE DATABASE IF NOT EXISTS power")
        conn.execute("USE power")
        conn.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
        conn.execute("CREATE TABLE  IF NOT EXISTS `d0` USING `meters` (groupId, location) TAGS(0, 'Los Angles')")
    except Exception as err:
        print(f'Exception {err}')
# ANCHOR_END: create_db

def insert(conn):
# ANCHOR: insert
    sql = """
    INSERT INTO 
    power.d1001 USING power.meters TAGS('California.SanFrancisco', 2)
        VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
        (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3)
        VALUES (NOW + 1a, 10.30000, 218, 0.25000)
    """
    try:
        inserted = conn.execute(sql)
        assert inserted == 8
    except Exception as err:
        print(f'Exception111 {err}')
# ANCHOR_END: insert

def query(conn):
# ANCHOR: query
    try:
        result = conn.query("select * from meters")
        num_of_fields = result.field_count
        print(num_of_fields)

        for row in result:
            print(row)
    except Exception as err:
        print(f'Exception {err}')
# ANCHOR_END: query

if __name__ == "__main__":
    conn = create_connection()
    create_db_table(conn)
    insert(conn)
    query(conn)
    if conn:
        conn.close()
