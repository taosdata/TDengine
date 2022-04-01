import taos


def test_connection():
    # all parameters are optional.
    # if database is specified,
    # then it must exist.
    # we use database log here, because it always exists.
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata",
                        database="log")
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    conn.close()


if __name__ == "__main__":
    test_connection()
