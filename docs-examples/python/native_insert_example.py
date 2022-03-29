import taos

lines = ["d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,Beijing.Chaoyang,2",
         "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,Beijing.Haidian,3",
         "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,Beijing.Haidian,2",
         "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,Beijing.Haidian,3",
         "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,Beijing.Chaoyang,3",
         "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,Beijing.Chaoyang,2",
         "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,Beijing.Chaoyang,2",
         "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,Beijing.Haidian,2"]


def get_connection() -> taos.TaosConnection:
    return taos.connect()


def create_stable(conn: taos.TaosConnection):
    conn.execute("CREATE DATABASE power")
    conn.execute("USE power")
    conn.execute("CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                 "TAGS (location BINARY(64), groupId INT)")


def insert_data(conn: taos.TaosConnection):
    pass


if __name__ == '__main__':
    connection = get_connection()
    create_stable(connection)
    insert_data(connection)
    connection.close()
