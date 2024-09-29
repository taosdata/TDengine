import taos
from datetime import datetime
import random

numOfSubTable = 10
numOfRow = 10

conn = None
stmt = None
host="localhost"        
port=6030
try:
    conn = taos.connect(
        user="root",
        password="taosdata",
        host=host,        
        port=port,
    )

    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    conn.execute("USE power")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )

    # ANCHOR: stmt
    sql = "INSERT INTO ? USING meters (groupid, location) TAGS(?,?) VALUES (?,?,?,?)"
    stmt = conn.statement(sql)
    for i in range(numOfSubTable):
        tbname = f"d_bind_{i}"

        tags = taos.new_bind_params(2)
        tags[0].int([i])
        tags[1].binary([f"location_{i}"])
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

        params = taos.new_bind_params(4)
        params[0].timestamp(timestamps)
        params[1].float(currents)
        params[2].int(voltages)
        params[3].float(phases)
        stmt.bind_param_batch(params)
        stmt.execute()
        print(f"Successfully inserted to power.meters.")

except Exception as err:
    print(f"Failed to insert to table meters using stmt, ErrMessage:{err}") 
    raise err
finally:
    if stmt:
        stmt.close()
    if conn:    
        conn.close()
