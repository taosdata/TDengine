import taos

def create_connection():
    # all parameters are optional.
    conn = None
    host = "localhost"
    port = 6031
    try:
        conn = taos.connect(
            user="root",
            password="taosdata",
            host=host,
            port=port,
        )
        print(f"Connected to {host}:{port} successfully.");
    except Exception as err:
        print(f"Failed to connect to {host}:{port} , ErrMessage:{err}")
        raise err
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_connection()
