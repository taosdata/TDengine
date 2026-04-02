import taos

host = "127.0.0.1"
port = 6030


def json_tag_example():
    conn = None
    try:
        conn = taos.connect(host=host, port=port, user="root", password="taosdata")
        # create database
        rows_affected: int = conn.execute(f"CREATE DATABASE IF NOT EXISTS example_json_tag")
        print(f"Create database power successfully")
        assert rows_affected == 0

        rows_affected = conn.execute(
            "create table if not exists example_json_tag.stb (ts timestamp, v int) tags(jt json)"
        )
        print(f"Create stable example_json_tag.stb successfully")
        assert rows_affected == 0

        rows_affected = conn.execute(
            'insert into example_json_tag.tb1 using example_json_tag.stb tags(\'{"name":"value"}\') values(now, 1)'
        )
        print(f"Successfully inserted {rows_affected} rows to example_json_tag.tb1.")

        result = conn.query("SELECT ts, v, jt FROM example_json_tag.stb limit 100", req_id=1)
        for row in result:
            print(f"ts: {row[0]}, v: {row[1]}, jt:  {row[2]}")

    except Exception as err:
        print(f"Failed to execute json_tag_example, db addrr:{host}:{port} ; ErrMessage:{err}")
    finally:
        if conn:
            conn.close()


def all_type_example():
    conn = None
    try:
        conn = taos.connect(host=host, port=port, user="root", password="taosdata")
        # create database
        rows_affected: int = conn.execute(f"CREATE DATABASE IF NOT EXISTS all_type_example")
        print(f"Create database power successfully")
        assert rows_affected == 0

        cols = [
            "ts timestamp",
            "int_col INT",
            "double_col DOUBLE",
            "bool_col BOOL",
            "binary_col BINARY(100)",
            "nchar_col NCHAR(100)",
            "varbinary_col VARBINARY(100)",
            "geometry_col GEOMETRY(100)",
        ]
        tags = [
            "int_tag INT",
            "double_tag DOUBLE",
            "bool_tag BOOL",
            "binary_tag BINARY(100)",
            "nchar_tag NCHAR(100)",
            "varbinary_tag VARBINARY(100)",
            "geometry_tag GEOMETRY(100)",
        ]

        str_cols = ",".join(cols)
        str_tags = ",".join(tags)
        print(str_cols, str_tags)
        conn.execute("create table IF NOT EXISTS all_type_example.stb(%s) tags(%s)" % (str_cols, str_tags))
        print(f"Create stable all_type_example.stb successfully")

        rows_affected = conn.execute(
            "INSERT INTO all_type_example.tb1 using all_type_example.stb tags(1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)') "
            + "values(now, 1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)')"
        )
        print(f"Successfully inserted {rows_affected} rows to all_type_example.tb1.")

        result = conn.query("SELECT * FROM all_type_example.stb limit 100", req_id=1)

        data_dict = result.fetch_all_into_dict()
        print(data_dict)

    except Exception as err:
        print(f"Failed to create db and table, db addrr:{host}:{port} ; ErrMessage:{err}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    json_tag_example()
    all_type_example()
