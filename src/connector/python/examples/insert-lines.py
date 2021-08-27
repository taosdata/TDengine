import taos

conn = taos.connect()
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000ns',
]
conn.insert_lines(lines)
print("inserted")

conn.insert_lines(lines)

result = conn.query("show tables")
for row in result:
    print(row)


conn.execute("drop database if exists %s" % dbname)
