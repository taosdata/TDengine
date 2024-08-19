import taosrest

conn = None
url = "http://localhost:6041"
try:
    conn = taosrest.connect(url=url,
                   user="root",
                   password="taosdata",
                   timeout=30)

    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
    print(f"Create database power successfully, rowsAffected: {rowsAffected}");

    # create super table
    rowsAffected = conn.execute(
        f"CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )
    print(f"Create stable power.meters successfully, rowsAffected: {rowsAffected}");

except Exception as err:
    print(f"Failed to create database power or stable meters, ErrMessage:{err}") 
finally:
    if conn:
        conn.close()
