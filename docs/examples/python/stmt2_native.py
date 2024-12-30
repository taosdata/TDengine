import taos
from datetime import datetime
import random

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

    # table name array
    tbnames = ["d0","d1","d2"]
    # tag data array
    tags    = [
        [1, "BeiJing"],
        [2, None],
        [3, "ShangHai"]
    ]
    # column data array
    datas   = [
        # d0 tabled
        [
            [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004,1601481600005],
            [10.1,         10.2,         10.3,         10.4,         10.5         ,None         ],
            [98,           None,           60,           100,          99           ,128          ],
            [0,            1,            0,            0,            1           ,0             ]
        ],
        # d1 tabled
        [
            [1601481700000,1601481700001,1601481700002,1601481700003,1601481700004,1601481700005],
            [10.1,         10.2,         10.3,         10.4,         10.5         ,11.2         ],
            [98,           80,           60,           100,          99           ,128          ],
            [0,            1,            0,            0,            1           ,0             ]
        ],
        # d2 tabled
        [
            [1601481800000,1601481800001,1601481800002,1601481800003,1601481800004,1601481800005],
            [10.1,         10.2,         10.3,         10.4,         10.5         ,13.4         ],
            [98,           80,           60,           100,          99           ,128          ],
            [0,            1,            0,            None,            1           ,0          ]
        ],
    ]

    # 4 bind param
    stmt2.bind_param(tbnames, tags, datas)

    # 5 execute
    stmt2.execute()

    # show 
    print(f"Successfully inserted with stmt2 to power.meters.")

except Exception as err:
    print(f"Failed to insert to table meters using stmt2, ErrMessage:{err}") 
    raise err
finally:
    if stmt2:
        stmt2.close()
    if conn:    
        conn.close()
