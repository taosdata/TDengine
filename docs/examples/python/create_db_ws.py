import taosws

conn = None
host = "localhost"
port = 6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    db = "power"
    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    assert rowsAffected == 0

    # change database. 
    rowsAffected = conn.execute(f"USE {db}")
    assert rowsAffected == 0
    
    # create super table
    rowsAffected = conn.execute(
        "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )
    assert rowsAffected == 0

    # create table
    rowsAffected = conn.execute("CREATE TABLE IF NOT EXISTS `d0` USING `meters` (groupid, location) TAGS(0, 'Los Angles')")
    assert rowsAffected == 0

except Exception as err:
    print(f"Failed to create db and table, db addrr:{host}:{port} err:{err}") 
finally:
    if conn:
        conn.close()
