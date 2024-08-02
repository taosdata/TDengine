import taos

conn = None
try:
    conn = taos.connect(host="localhost",
                        user="root",
                        password="taosdata",
                        port=6030)

    db = "power"
    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    assert rowsAffected == 0

    # change database. same as execute "USE db"
    rowsAffected = conn.select_db(db)
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
    print(err) 
finally:
    if conn:
        conn.close()       
