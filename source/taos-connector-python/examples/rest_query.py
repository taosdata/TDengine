"""
Query example for RestConnection
"""

from taosrest import connect, TaosRestConnection, Result

conn: TaosRestConnection = connect()
result: Result = conn.query("show databases")

num_of_fields = result.field_count
for field in result.fields:
    print(field)

for row in result:
    print(row)

# {'name': 'name', 'type': 8, 'bytes': 32}
# {'name': 'created_time', 'type': 9, 'bytes': 8}
# {'name': 'ntables', 'type': 4, 'bytes': 4}
# {'name': 'vgroups', 'type': 4, 'bytes': 4}
# {'name': 'replica', 'type': 3, 'bytes': 2}
# {'name': 'quorum', 'type': 3, 'bytes': 2}
# {'name': 'days', 'type': 3, 'bytes': 2}
# {'name': 'keep', 'type': 8, 'bytes': 24}
# {'name': 'cache(MB)', 'type': 4, 'bytes': 4}
# {'name': 'blocks', 'type': 4, 'bytes': 4}
# {'name': 'minrows', 'type': 4, 'bytes': 4}
# {'name': 'maxrows', 'type': 4, 'bytes': 4}
# {'name': 'wallevel', 'type': 2, 'bytes': 1}
# {'name': 'fsync', 'type': 4, 'bytes': 4}
# {'name': 'comp', 'type': 2, 'bytes': 1}
# {'name': 'cachelast', 'type': 2, 'bytes': 1}
# {'name': 'precision', 'type': 8, 'bytes': 3}
# {'name': 'update', 'type': 2, 'bytes': 1}
# {'name': 'status', 'type': 8, 'bytes': 10}
# ['test', datetime.datetime(2022, 6, 13, 9, 38, 15, 290000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 1, 1, 1, 1, 10, '3650', 16, 6, 100, 4096, 1, 3000, 2, 0, 'ms', 0, 'ready']
# ['log', datetime.datetime(2022, 3, 26, 15, 54, 26, 997000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 274, 1, 1, 1, 10, '30', 1, 3, 100, 4096, 1, 3000, 2, 0, 'us', 0, 'ready']
# ['test2', datetime.datetime(2022, 4, 21, 9, 34, 52, 236000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 0, 0, 1, 1, 10, '3650', 16, 6, 100, 4096, 1, 3000, 2, 0, 'ms', 0, 'ready']
# ['power', datetime.datetime(2022, 6, 7, 13, 3, 10, 198000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 4, 1, 1, 1, 10, '3650', 16, 6, 100, 4096, 1, 3000, 2, 0, 'ms', 0, 'ready']
