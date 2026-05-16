#! encoding=utf-8
import taos
from utils import tear_down_database


def test_get_table_vgroup_id():
    conn = taos.connect()
    res = conn.query("select server_version()")
    data = res.fetch_all()
    res.close()
    if data[0][0] < "3.0.2.0":
        return

    conn.execute("drop database if exists test_get_table_vgroup_id")
    conn.execute("create database if not exists test_get_table_vgroup_id")
    conn.execute(
        "create stable test_get_table_vgroup_id.meters (ts timestamp, current float, voltage int, phase float)"
        " tags (location binary(64), groupId int)"
    )
    conn.execute(
        "create table test_get_table_vgroup_id.d0 using test_get_table_vgroup_id.meters "
        "tags ('California.SanFrancisco', 1)"
    )
    conn.execute(
        "create table test_get_table_vgroup_id.d1 using test_get_table_vgroup_id.meters "
        "tags ('California.LosAngles', 2)"
    )

    vg_id = conn.get_table_vgroup_id("test_get_table_vgroup_id", "d0")
    print(vg_id)
    print("\n")
    vg_id = conn.get_table_vgroup_id("test_get_table_vgroup_id", "d1")
    print(vg_id)
    print("\n")
    db_name = "test_get_table_vgroup_id"
    tear_down_database(conn, db_name)
    conn.close()


if __name__ == "__main__":
    test_get_table_vgroup_id()
