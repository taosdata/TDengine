import taos

def create_connection():
    # all parameters are optional.
    conn = None
    try:
        conn = taos.connect(
            user="root",
            password="taosdata",
            host="localhost",
            port=6041,
        )
    except Exception as err:
        print(err)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_connection()
