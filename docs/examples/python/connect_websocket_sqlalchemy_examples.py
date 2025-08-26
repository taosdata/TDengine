import taosws

# ANCHOR: connect_sqlalchemy
def create_connection_with_sqlalchemy():
    from sqlalchemy import create_engine

    engine = create_engine("taosws://root:taosdata@?hosts=localhost:6041,127.0.0.1:6041")
    conn = engine.connect()
    return conn
# ANCHOR_END: connect_sqlalchemy

def create_db_table(conn):
# ANCHOR: create_db
    try:
        conn.execute("CREATE DATABASE IF NOT EXISTS power")
        conn.execute("USE power")
        conn.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
        conn.execute("CREATE TABLE  IF NOT EXISTS `d0` USING `meters` (groupId, location) TAGS(0, 'Los Angles')")
    except Exception as err:
        print(f'Exception {err}')
        raise err
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
        assert inserted == 4
    except Exception as err:
        print(f'Exception111 {err}')
        raise err
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
        raise err
# ANCHOR_END: query

if __name__ == "__main__":
    conn = create_connection_with_sqlalchemy()
    create_db_table(conn)
    insert(conn)
    if conn:
        conn.close()

    query(conn)
    if conn:
        conn.close()
