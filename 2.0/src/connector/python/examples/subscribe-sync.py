import taos
import random

conn = taos.connect()
dbname = "pytest_taos_subscribe"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s" % dbname)
conn.select_db(dbname)
conn.execute("create table if not exists log(ts timestamp, n int)")
for i in range(10):
    conn.execute("insert into log values(now, %d)" % i)

sub = conn.subscribe(False, "test", "select * from log", 1000)
print("# consume from begin")
for ts, n in sub.consume():
    print(ts, n)

print("# consume new data")
for i in range(5):
    conn.execute("insert into log values(now, %d)(now+1s, %d)" % (i, i))
    result = sub.consume()
    for ts, n in result:
        print(ts, n)

sub.close(True)
print("# keep progress consume")
sub = conn.subscribe(False, "test", "select * from log", 1000)
result = sub.consume()
rows = result.fetch_all()
# consume from latest subscription needs root privilege(for /var/lib/taos).
assert result.row_count == 0
print("## consumed ", len(rows), "rows")

print("# consume with a stop condition")
for i in range(10):
    conn.execute("insert into log values(now, %d)" % random.randint(0, 10))
    result = sub.consume()
    try:
        ts, n = next(result)
        print(ts, n)
        if n > 5:
            result.stop_query()
            print("## stopped")
            break
    except StopIteration:
        continue

sub.close()

# sub.close()

conn.execute("drop database if exists %s" % dbname)
# conn.close()
