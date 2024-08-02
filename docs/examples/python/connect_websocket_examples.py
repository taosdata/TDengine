import taosws

def create_connection():
    conn = None
# ANCHOR: connect
    try:
        conn = taosws.connect(
            user="root",
            password="taosdata",
            host="localhost",
            port=6041,
        )
    except Exception as err:
        print(f'Exception {err}')
# ANCHOR_END: connect
    return conn

def create_db_table(conn):
# ANCHOR: create_db
    try:
        conn.execute("CREATE DATABASE IF NOT EXISTS power")
        conn.execute("USE power")
        conn.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
        conn.execute("CREATE TABLE `d0` USING `meters` TAGS(0, 'Los Angles')")
    except Exception as err:
        print(f'Exception {err}')
# ANCHOR_END: create_db

def insert(conn):    
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
        print(f'Exception {err}')   

def query(conn):
    result = conn.query("select * from stb")
    num_of_fields = result.field_count
    print(num_of_fields)

    for row in result:
        print(row)

# output:
# 3
# ('2023-02-28 15:56:13.329 +08:00', 1, 1)
# ('2023-02-28 15:56:13.333 +08:00', 2, 1)
# ('2023-02-28 15:56:13.337 +08:00', 3, 1)
