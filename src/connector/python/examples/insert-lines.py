import taos

conn = taos.connect()
dbname = "pytest_line"
conn.exec("drop database if exists %s" % dbname)
conn.exec("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000ns',
    'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000ns',
    'stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000ns',
]
conn.insert_lines(lines)
print("inserted")

lines = [
    'stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000ns',
]
conn.insert_lines(lines)

result = conn.query("show tables")
for row in result:
    print(row)
result.close()


conn.exec("drop database if exists %s" % dbname)
conn.close()
