import taos

conn = taos.connect()


def print_all():
    result = conn.query()
