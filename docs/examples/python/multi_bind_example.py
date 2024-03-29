import taos
from datetime import datetime

# ANCHOR: bind_batch
table_tags = {
    "d1001": ('California.SanFrancisco', 2),
    "d1002": ('California.SanFrancisco', 3),
    "d1003": ('California.LosAngeles', 2),
    "d1004": ('California.LosAngeles', 3)
}

table_values = {
    "d1001": [
        ['2018-10-03 14:38:05.000', '2018-10-03 14:38:15.000', '2018-10-03 14:38:16.800'],
        [10.3, 12.6, 12.3],
        [219, 218, 221],
        [0.31, 0.33, 0.32]
    ],
    "d1002": [
        ['2018-10-03 14:38:16.650'], [10.3], [218], [0.25]
    ],
    "d1003": [
        ['2018-10-03 14:38:05.500', '2018-10-03 14:38:16.600'],
        [11.8, 13.4],
        [221, 223],
        [0.28, 0.29]
    ],
    "d1004": [
        ['2018-10-03 14:38:05.500', '2018-10-03 14:38:06.500'],
        [10.8, 11.5],
        [223, 221],
        [0.29, 0.35]
    ]
}


def bind_multi_rows(stmt: taos.TaosStmt):
    """
    batch bind example
    """
    for tb_name in table_values.keys():
        tags = table_tags[tb_name]
        tag_params = taos.new_bind_params(2)
        tag_params[0].binary(tags[0])
        tag_params[1].int(tags[1])
        stmt.set_tbname_tags(tb_name, tag_params)

        values = table_values[tb_name]
        value_params = taos.new_multi_binds(4)
        value_params[0].timestamp([get_ts(t) for t in values[0]])
        value_params[1].float(values[1])
        value_params[2].int(values[2])
        value_params[3].float(values[3])
        stmt.bind_param_batch(value_params)


def insert_data():
    conn = taos.connect(database="power")
    try:
        stmt = conn.statement("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
        bind_multi_rows(stmt)
        stmt.execute()
        stmt.close()
    finally:
        conn.close()


# ANCHOR_END: bind_batch


def create_stable():
    conn = taos.connect()
    try:
        conn.execute("CREATE DATABASE power keep 36500")
        conn.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                     "TAGS (location BINARY(64), groupId INT)")
    finally:
        conn.close()


def get_ts(ts: str):
    dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
    return int(dt.timestamp() * 1000)


if __name__ == '__main__':
    create_stable()
    insert_data()
