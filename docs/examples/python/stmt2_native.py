import taos
from datetime import datetime
import random

numOfSubTable = 10
numOfRow = 10

conn = None
stmt2 = None
host="localhost"        
port=6030
try:
    # 1 connect
    conn = taos.connect(
        user="root",
        password="taosdata",
        host=host,        
        port=port,
    )

    # 2 create db and table
    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    conn.execute("USE power")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )

    # 3 prepare
    sql = "INSERT INTO ? USING meters (groupid, location) TAGS(?,?) VALUES (?,?,?,?)"
    stmt2 = conn.statement2(sql)

    tbnames = []
    tags    = []
    datas   = []
    
    for i in range(numOfSubTable):
        # tbnames
        tbnames.append(f"d_bind_{i}")
        # tags
        tags.append([i, f"location_{i}"])
        # datas
        current = int(datetime.now().timestamp() * 1000)
        timestamps = []
        currents = []
        voltages = []
        phases = []
        for j in range (numOfRow):
            timestamps.append(current + i*1000 + j)
            currents.append(float(random.random() * 30))
            voltages.append(random.randint(100, 300))
            phases.append(float(random.random()))
        data = [timestamps, currents, voltages, phases]
        datas.append(data)

    # 4 bind param
    stmt2.bind_param(tbnames, tags, datas)

    # 5 execute
    stmt2.execute()

    # show 
    print(f"Successfully inserted with stmt2 to power.meters. child={numOfSubTable} rows={numOfRow} \n")

except Exception as err:
    print(f"Failed to insert to table meters using stmt2, ErrMessage:{err}") 
    raise err
finally:
    if stmt2:
        stmt2.close()
    if conn:    
        conn.close()
