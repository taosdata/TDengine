import taos
from taos import SmlProtocol, SmlPrecision

conn = taos.connect()
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us' keep 36500" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
]
conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=1)
print("inserted")

conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=2)

result = conn.query("show tables")
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)
