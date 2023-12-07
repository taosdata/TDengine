import taos
from datetime import datetime

# note: lines have already been sorted by table name
lines = [('d1001', '2018-10-03 14:38:05.000', 10.30000, 219, 0.31000, 'California.SanFrancisco', 2),
         ('d1001', '2018-10-03 14:38:15.000', 12.60000, 218, 0.33000, 'California.SanFrancisco', 2),
         ('d1001', '2018-10-03 14:38:16.800', 12.30000, 221, 0.31000, 'California.SanFrancisco', 2),
         ('d1002', '2018-10-03 14:38:16.650', 10.30000, 218, 0.25000, 'California.SanFrancisco', 3),
         ('d1003', '2018-10-03 14:38:05.500', 11.80000, 221, 0.28000, 'California.LosAngeles', 2),
         ('d1003', '2018-10-03 14:38:16.600', 13.40000, 223, 0.29000, 'California.LosAngeles', 2),
         ('d1004', '2018-10-03 14:38:05.000', 10.80000, 223, 0.29000, 'California.LosAngeles', 3),
         ('d1004', '2018-10-03 14:38:06.500', 11.50000, 221, 0.35000, 'California.LosAngeles', 3)]


def get_ts(ts: str):
    dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
    return int(dt.timestamp() * 1000)


def create_stable():
    conn = taos.connect()
    try:
        conn.execute("CREATE DATABASE power keep 36500")
        conn.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                     "TAGS (location BINARY(64), groupId INT)")
    finally:
        conn.close()


def bind_row_by_row(stmt: taos.TaosStmt):
    tb_name = None
    for row in lines:
        if tb_name != row[0]:
            tb_name = row[0]
            tags: taos.TaosBind = taos.new_bind_params(2)  # 2 is count of tags
            tags[0].binary(row[5])  # location
            tags[1].int(row[6])  # groupId
            stmt.set_tbname_tags(tb_name, tags)
        values: taos.TaosBind = taos.new_bind_params(4)  # 4 is count of columns
        values[0].timestamp(get_ts(row[1]))
        values[1].float(row[2])
        values[2].int(row[3])
        values[3].float(row[4])
        stmt.bind_param(values)


def insert_data():
    conn = taos.connect(database="power")
    try:
        stmt = conn.statement("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
        bind_row_by_row(stmt)
        stmt.execute()
        stmt.close()
    finally:
        conn.close()


if __name__ == '__main__':
    create_stable()
    insert_data()
