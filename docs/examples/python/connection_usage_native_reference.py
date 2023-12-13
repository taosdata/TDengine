import taos

# ANCHOR: insert
conn = taos.connect()
# Execute a sql, ignore the result set, just get affected rows. It's useful for DDL and DML statement.
conn.execute("DROP DATABASE IF EXISTS test")
conn.execute("CREATE DATABASE test keep 36500")
# change database. same as execute "USE db"
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
affected_row = conn.execute("INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+1m, 23.5) (now+2m, 24.4)")
print("affected_row", affected_row)
# output:
# affected_row 3
# ANCHOR_END: insert

# ANCHOR: query
# Execute a sql and get its result set. It's useful for SELECT statement
result = conn.query("SELECT * from weather")

# Get fields from result
fields = result.fields
for field in fields:
    print(field)  # {name: ts, type: 9, bytes: 8}

# output:
# {name: ts, type: 9, bytes: 8}
# {name: temperature, type: 6, bytes: 4}
# {name: location, type: 4, bytes: 4}

# Get data from result as list of tuple
data = result.fetch_all()
print(data)
# output:
# [(datetime.datetime(2022, 4, 27, 9, 4, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 5, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 6, 25, 367000), 24.399999618530273, 1)]

# Or get data from result as a list of dict
# map_data = result.fetch_all_into_dict()
# print(map_data)
# output:
# [{'ts': datetime.datetime(2022, 4, 27, 9, 1, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 2, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 3, 15, 343000), 'temperature': 24.399999618530273, 'location': 1}]
# ANCHOR_END: query


conn.close()