import taos

def test_connection():
    # all parameters are optional.
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata")
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    conn.close()

if __name__ == "__main__":
    test_connection()
