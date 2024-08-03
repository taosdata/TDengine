import taosrest

conn = None
url = "http://localhost:6041"
try:
    conn = taosrest.connect(url=url,
                   user="root",
                   password="taosdata",
                   timeout=30)

    db = "power"
    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    assert rowsAffected == 0

    # create super table
    rowsAffected = conn.execute(
        f"CREATE TABLE IF NOT EXISTS `{db}`.`meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )
    assert rowsAffected == 0
    # create table
    rowsAffected = conn.execute(f"CREATE TABLE IF NOT EXISTS `{db}`.`d0` USING `{db}`.`meters` (groupid, location) TAGS(0, 'Los Angles')")
    assert rowsAffected == 0

except Exception as err:
    print(f"Failed to create db and table, url:{url} err:{err}") 
finally:
    if conn:
        conn.close()
