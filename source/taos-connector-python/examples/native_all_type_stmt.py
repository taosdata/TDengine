import taos
from taos import new_bind_params, PrecisionEnum

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

        conn.select_db("example_json_tag")

        rows_affected = conn.execute("create table if not exists stb (ts timestamp, v int) tags(jt json)")
        print(f"Create stable example_json_tag.stb successfully")
        assert rows_affected == 0

        stmt = conn.statement("INSERT INTO ? using stb tags(?) VALUES (?,?)")

        tags = new_bind_params(1)
        tags[0].json('{"name":"value"}')
        stmt.set_tbname_tags("tb1", tags)

        params = new_bind_params(2)
        params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        params[1].int(7)

        stmt.bind_param(params)
        stmt.execute()

        assert stmt.affected_rows == 1

        result = conn.query("SELECT ts, v, jt FROM stb limit 100", req_id=1)
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
        conn.select_db("all_type_example")

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
        conn.execute("create table IF NOT EXISTS stb(%s) tags(%s)" % (str_cols, str_tags))
        print(f"Create stable all_type_example.stb successfully")

        stmt = conn.statement("INSERT INTO ? using stb tags(?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)")
        tags = new_bind_params(7)
        tags[0].int(1)
        tags[1].double(1.1)
        tags[2].bool(True)
        tags[3].binary("hello")
        tags[4].nchar("stmt")
        string_data = "Hello, world!"
        byte_array = bytearray(string_data, "utf-8")
        tags[5].varbinary(byte_array)
        binary_list = bytearray(
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
        tags[6].geometry(binary_list)
        stmt.set_tbname_tags("tb1", tags)

        params = new_bind_params(8)
        params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        params[1].int(1)
        params[2].double(1.1)
        params[3].bool(True)
        params[4].binary("hello")
        params[5].nchar("stmt")
        string_data = "Hello, world!"
        byte_array = bytearray(string_data, "utf-8")
        params[6].varbinary(byte_array)
        binary_list = bytearray(
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
        params[7].geometry(binary_list)
        stmt.bind_param(params)
        stmt.execute()

        assert stmt.affected_rows == 1
        result = conn.query("SELECT * FROM stb limit 100", req_id=1)

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
