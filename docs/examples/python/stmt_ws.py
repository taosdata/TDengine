from datetime import datetime
import random
import taosws

numOfSubTable = 10

numOfRow = 10

conn = None
stmt = None
host="localhost"
port=6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    conn.execute("USE power")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )

    # ANCHOR: stmt
    sql = "INSERT INTO ? USING meters (groupid, location) TAGS(?,?) VALUES (?,?,?,?)"
    stmt = conn.statement()
    stmt.prepare(sql)

    for i in range(numOfSubTable):
        tbname = f"d_bind_{i}"

        tags = [
            taosws.int_to_tag(i),
            taosws.varchar_to_tag(f"location_{i}"),
        ]
        stmt.set_tbname_tags(tbname, tags)
        current = int(datetime.now().timestamp() * 1000)
        timestamps = []
        currents = []
        voltages = []
        phases = []
        for j in range (numOfRow):
            timestamps.append(current + i)
            currents.append(random.random() * 30)
            voltages.append(random.randint(100, 300))
            phases.append(random.random())

        stmt.bind_param(
            [
                taosws.millis_timestamps_to_column(timestamps),
                taosws.floats_to_column(currents),
                taosws.ints_to_column(voltages),
                taosws.floats_to_column(phases),
            ]
        )

        stmt.add_batch()
        stmt.execute()
        
        print(f"Successfully inserted to power.meters.")
        
except Exception as err:
    print(f"Failed to insert to table meters using stmt, ErrMessage:{err}") 
finally:
    if stmt:
        stmt.close()
    if conn:    
        conn.close()
