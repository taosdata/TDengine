import pyodbc
cnxn = pyodbc.connect('DSN=TAOS_DSN;UID=root;PWD=taosdata', autocommit=True)
cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
#cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
#cnxn.setencoding(encoding='utf-8')

cursor = cnxn.cursor()
cursor.execute("SELECT * from db.t")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
cursor.close()

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
cursor.execute("create table db.t (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), blob nchar(10))");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("insert into db.t values('2020-10-13 06:44:00', 1, 127, 32767, 32768, 32769, 123.456, 789.987, 'hello', 'world')")
cursor.close()

cursor = cnxn.cursor()
cursor.execute("insert into db.t values(?,?,?,?,?,?,?,?,?,?)", "2020-10-13 07:06:00", 0, 127, 32767, 32768, 32769, 123.456, 789.987, "hel后lo", "wo哈rld");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("SELECT * from db.t")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
cursor.close()

cursor = cnxn.cursor()
cursor.execute("drop database if exists db");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create database db");
cursor.close()

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

