import taos


def test_invalid_dbname():
    conn: taos.TaosConnection = None
    try:
        conn = taos.connect(database="notexist")
    except taos.Error as e:
        print("\n=============================")
        print(e)
        print("exception class: ", e.__class__.__name__)  # ConnectionError
        print("error number:", e.errno)  # 65535
        print("error message:", e.msg)  # connect to TDengine failed
    except BaseException as other:
        print("exception occur")
        print(other)
    finally:
        if conn is not None:
            conn.close()
