import pyodbc
import os
import sys

if __name__ == "__main__":
    # Using a DSN, but providing a password as well

    # server = os.getenv('DB_SERVER', '127.0.0.1:6030')
    # driver = os.getenv('DB_DRIVER', 'TAOS_ODBC_DRIVER')
    # database = os.getenv('DB_DATABASE', 'information_schema')
    # uid = os.getenv('DB_UID', 'root')
    # pwd = os.getenv('DB_PWD', 'taosdata')
    #
    # info = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}'
    # print(info)
    # cnxn = pyodbc.connect(info)

    cnxn = pyodbc.connect('DSN=TAOS_ODBC_WS_DSN;PWD=taosdata')

    # Create a cursor from the connection
    cursor = cnxn.cursor()

    cursor.execute("drop database if exists meter")
    cursor.execute("create database if not exists meter")
    cursor.execute("use meter")

    cursor.execute("drop table if exists d0")
    cursor.execute("create table d0 (ts timestamp, current FLOAT, voltage INT)")
    cursor.execute("insert into d0(ts, current, voltage) values (now(), 1.5, 220)")
    cursor.execute("insert into d0(ts, current, voltage) values (?, ?, ?)", 1682565350033, 1.52, 221)

    data = cursor.execute("select current,voltage from d0").fetchall()
    print(data, "\n")

    cursor.execute("drop stable if exists meters")
    cursor.execute(
        "create stable meters (ts timestamp, current FLOAT, voltage INT, phase FLOAT) tags (groupid INT, location BINARY(24))")
    cursor.execute(
        "insert into 'd000' using meters tags (0, 'California.SanFrancisco') values (1665226861289, 2, 200, 1.3)")
    data = cursor.execute("select ts, current, voltage, phase from meters").fetchall()
    print(data, "\n")

    cursor.execute("insert into ? using meters tags (?, ?) values (?, ?, ?, ?)", 'd001', 1, 'California.LosAngles',
                   1665226861289, 2.1, 201, 1.2)
    data = cursor.execute("select ts, current, voltage, phase from meters").fetchall()
    print(data, "\n")

    params = [('d002', 2, 'California.SanFrancisco', 1665226861289, 1.21, 202, 1.2),
              ('d003', 3, 'California.LosAngles', 1665226861299, 1.22, 203, 1.3)]
    cursor.fast_executemany = False
    cursor.executemany("insert into ? using meters tags (?, ?) values (?, ?, ?, ?)", params)

    data = cursor.execute("select ts, current, voltage, phase from meters").fetchall()
    print(data, "\n")

    cursor.close()
    cnxn.close()
