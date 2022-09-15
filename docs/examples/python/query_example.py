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
# meta of fields[1]: {name: ts, type: 9, bytes: 8}
# ======================Iterate on result=========================
# ('d1003', datetime.datetime(2018, 10, 3, 14, 38, 5, 500000), 11.800000190734863, 221, 0.2800000011920929, 'california.losangeles', 2)
# ('d1003', datetime.datetime(2018, 10, 3, 14, 38, 16, 600000), 13.399999618530273, 223, 0.28999999165534973, 'california.losangeles', 2)
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
# [{'ts': datetime.datetime(2018, 10, 3, 14, 38, 5, 500000), 'current': 11.800000190734863},
# {'ts': datetime.datetime(2018, 10, 3, 14, 38, 16, 600000), 'current': 13.399999618530273}]
# ANCHOR_END: fetch_all

if __name__ == '__main__':
    connection = taos.connect(database="power")
    try:
        query_api_demo(connection)
        fetch_all_demo(connection)
    finally:
        connection.close()
