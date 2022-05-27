import taos


# ANCHOR: iter
def query_api_demo(conn: taos.TaosConnection):
    result: taos.TaosResult = conn.query("SELECT tbname, * FROM meters LIMIT 2")
    print("field count:", result.field_count)
    print("meta of fields[1]:", result.fields[1])
    print("======================Iterate on result=========================")
    for row in result:
        print(row)


# field count: 7
# meta of files[1]: {name: ts, type: 9, bytes: 8}
# ======================Iterate on result=========================
# ('d1001', datetime.datetime(2018, 10, 3, 14, 38, 5), 10.300000190734863, 219, 0.3100000023841858, 'California.SanFrancisco', 2)
# ('d1001', datetime.datetime(2018, 10, 3, 14, 38, 15), 12.600000381469727, 218, 0.33000001311302185, 'California.SanFrancisco', 2)
# ANCHOR_END: iter

# ANCHOR: fetch_all
def fetch_all_demo(conn: taos.TaosConnection):
    result: taos.TaosResult = conn.query("SELECT ts, current FROM meters LIMIT 2")
    rows = result.fetch_all_into_dict()
    print("row count:", result.row_count)
    print("===============all data===================")
    print(rows)


# row count: 2
# ===============all data===================
# [{'ts': datetime.datetime(2018, 10, 3, 14, 38, 5), 'current': 10.300000190734863},
# {'ts': datetime.datetime(2018, 10, 3, 14, 38, 15), 'current': 12.600000381469727}]
# ANCHOR_END: fetch_all

if __name__ == '__main__':
    connection = taos.connect(database="power")
    try:
        query_api_demo(connection)
        fetch_all_demo(connection)
    finally:
        connection.close()
