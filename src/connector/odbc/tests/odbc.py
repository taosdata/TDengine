import pyodbc
# cnxn = pyodbc.connect('DSN={TAOS_DSN};UID={ root };PWD={ taosdata };HOST={ localhost:6030 }', autocommit=True)
cnxn = pyodbc.connect('DSN={TAOS_DSN}; UID=root;PWD=taosdata; HOST=localhost:6030', autocommit=True)
cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
#cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
#cnxn.setencoding(encoding='utf-8')

#cursor = cnxn.cursor()
#cursor.execute("SELECT * from db.t")
#row = cursor.fetchone()
#while row:
#    print(row)
#    row = cursor.fetchone()
#cursor.close()

#cursor = cnxn.cursor()
#cursor.execute("""
#INSERT INTO db.t values (?,?,?,?,?,?,?,?,?,?)
#""",
#"2020-12-12 00:00:00",
#1,
#27,
#32767,
#147483647,
#223372036854775807,
#23.456,
#899.999999,
#"foo",
#"bar")

cursor = cnxn.cursor()
cursor.execute("drop database if exists db");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create database db");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create table db.mt (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), blob nchar(10))");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("insert into db.mt values('2020-10-13 06:44:00', 1, 127, 32767, 32768, 32769, 123.456, 789.987, 'hello', 'world')")
cursor.close()

cursor = cnxn.cursor()
cursor.execute("insert into db.mt values(?,?,?,?,?,?,?,?,?,?)", "2020-10-13 07:06:00", 0, 127, 32767, 32768, 32769, 123.456, 789.987, "hel后lo", "wo哈rld");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("SELECT * from db.mt")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
cursor.close()

#cursor = cnxn.cursor()
#cursor.execute("drop database if exists db");
#cursor.close()
#
#cursor = cnxn.cursor()
#cursor.execute("create database db");
#cursor.close()

cursor = cnxn.cursor()
cursor.execute("create table db.t (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(4), blob nchar(4))");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("insert into db.t values('2020-10-13 06:44:00', 1, 127, 32767, 32768, 32769, 123.456, 789.987, 'hell', 'worl')")
cursor.close()

cursor = cnxn.cursor()
cursor.execute("SELECT * from db.t")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create table db.v (ts timestamp, v1 tinyint)")
cursor.close()

params = [ ('A', 1), ('B', 2), ('C', 3) ]
params = [ ('A', 1), ('B', 2), ('C', 3) ]
params = [ ('2020-10-16 00:00:00', 1),
           ('2020-10-16 00:00:01', 4),
           ('2020-10-16 00:00:02', 5),
           ('2020-10-16 00:00:03.009', 6) ]
cursor = cnxn.cursor()
cursor.fast_executemany = True
cursor.executemany("insert into db.v values (?, ?)", params)
cursor.close()

cursor = cnxn.cursor()
cursor.execute("SELECT * from db.v")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
cursor.close()

cursor = cnxn.cursor()
cursor.execute("SELECT * from db.v where v1 > ?", 4)
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
cursor.close()

cursor = cnxn.cursor()
cursor.execute("SELECT * from db.v where v1 > ?", '5')
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create table db.f (ts timestamp, v1 float)")
cursor.close()

params = [ ('2020-10-20 00:00:10', '123.3') ]
cursor = cnxn.cursor()
cursor.fast_executemany = True
cursor.executemany("insert into db.f values (?, ?)", params)
cursor.close()

