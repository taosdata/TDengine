from datetime import datetime
import random
import taosws

numOfSubTable = 10

numOfRow = 10

conn = None
stmt2 = None
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

    sql = "INSERT INTO ? USING meters (groupid, location) TAGS(?,?) VALUES (?,?,?,?)"
    stmt2 = conn.stmt2_statement()
    stmt2.prepare(sql)

    for i in range(numOfSubTable):        
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

        pyStmt2Param = taosws.stmt2_bind_param_view(
            table_name=f"d_bind_{i}", 
            tags=[taosws.int_to_tag(i),
                  taosws.varchar_to_tag(f"location_{i}")
            ], 
            columns=[  
                taosws.millis_timestamps_to_column(timestamps),
                taosws.floats_to_column(currents),
                taosws.ints_to_column(voltages),
                taosws.floats_to_column(phases)
            ]
        )    
        
        stmt2.bind([pyStmt2Param])
        rows = stmt2.execute()
        print(f"Successfully inserted to power.meters.")
        
except Exception as err:
    print(f"Failed to insert to table meters using stmt, ErrMessage:{err}") 
    raise err
finally:
    if stmt2:
        stmt2.close()
    if conn:    
        conn.close()
