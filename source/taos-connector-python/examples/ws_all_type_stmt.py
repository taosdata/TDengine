import taosws

host = "127.0.0.1"
port = 6041


def json_tag_example():
    conn = None
    try:
        conn = taosws.connect(host=host, port=port, user="root", password="taosdata")
        # create database
        rows_affected: int = conn.execute(f"CREATE DATABASE IF NOT EXISTS example_json_tag")
        print(f"Create database power successfully")
        assert rows_affected == 0

        rows_affected = conn.execute(
            "create table if not exists example_json_tag.stb (ts timestamp, v int) tags(jt json)"
        )
        print(f"Create stable example_json_tag.stb successfully")
        assert rows_affected == 0

        conn.execute("use example_json_tag")

        stmt = conn.statement()
        stmt.prepare("INSERT INTO ? using stb tags(?) VALUES (?,?)")
        stmt.set_tbname("tb1")
        stmt.set_tags([taosws.json_to_tag('{"name":"value"}')])

        stmt.bind_param([taosws.millis_timestamps_to_column([1686844800000]), taosws.ints_to_column([1])])

        stmt.add_batch()
        rows = stmt.execute()
        assert rows == 1
        stmt.close()

        result = conn.query("SELECT ts, v, jt FROM example_json_tag.stb limit 100")
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
        conn = taosws.connect(host=host, port=port, user="root", password="taosdata")
        cursor = conn.cursor()
        # create database
        rows_affected: int = cursor.execute(f"CREATE DATABASE IF NOT EXISTS all_type_example")
        print(f"Create database power successfully")
        assert rows_affected == 0
        conn.execute("use all_type_example")

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
        tags = ["int_tag INT", "double_tag DOUBLE", "bool_tag BOOL", "binary_tag BINARY(100)", "nchar_tag NCHAR(100)"]

        str_cols = ",".join(cols)
        str_tags = ",".join(tags)
        print(str_cols, str_tags)
        cursor.execute("create table IF NOT EXISTS all_type_example.stb(%s) tags(%s)" % (str_cols, str_tags))
        print(f"Create stable all_type_example.stb successfully")

        varbinary = bytes([0x01, 0x02, 0x03, 0x04])

        geometry = bytearray(
            [
                0x01,
                0x01,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x59,
                0x40,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x59,
                0x40,
            ]
        )

        stmt = conn.statement()
        stmt.prepare("INSERT INTO ? using stb tags(?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)")
        stmt.set_tbname("tb1")

        stmt.set_tags(
            [
                taosws.int_to_tag(1),
                taosws.double_to_tag(1.1),
                taosws.bool_to_tag(True),
                taosws.varchar_to_tag("hello"),
                taosws.nchar_to_tag("stmt"),
            ]
        )

        stmt.bind_param(
            [
                taosws.millis_timestamps_to_column([1686844800000]),
                taosws.ints_to_column([1]),
                taosws.doubles_to_column([1.1]),
                taosws.bools_to_column([True]),
                taosws.varchar_to_column(["hello"]),
                taosws.nchar_to_column(["stmt"]),
                taosws.varbinary_to_column([b"0x7661726332"]),
                taosws.geometry_to_column([geometry]),
            ]
        )

        stmt.add_batch()
        rows = stmt.execute()
        assert rows == 1
        stmt.close()

        cursor.execute("SELECT * FROM all_type_example.stb limit 100")

        data_dict = cursor.fetchallintodict()
        print(data_dict)
        cursor.close()
    except Exception as err:
        print(f"Failed to create db and table, db addrr:{host}:{port} ; ErrMessage:{err}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    json_tag_example()
    all_type_example()
