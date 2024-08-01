import taos

def create_connection():
    # all parameters are optional.
    conn = None
    try:
        conn = taosws.connect(
            user="root",
            password="taosdata",
            host="192.168.1.98",
            port=6041,
        )
    except Exception as err:
        print(f'Exception {err}')
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_connection()
