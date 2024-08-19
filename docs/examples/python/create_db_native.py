import taos

conn = None
host = "localhost"
port = 6030
try:
    conn = taos.connect(host=host,
                        port=port,
                        user="root",
                        password="taosdata")

    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
    print(f"Create database power successfully, rowsAffected: {rowsAffected}");
    
    # create super table
    rowsAffected = conn.execute(
        "CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )
    print(f"Create stable power.meters successfully, rowsAffected: {rowsAffected}");

except Exception as err:
    print(f"Failed to create database power or stable meters, ErrMessage:{err}") 
finally:
    if conn:
        conn.close()       
