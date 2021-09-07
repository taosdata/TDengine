import pyodbc
import argparse
import sys

parser = argparse.ArgumentParser(description='Access TDengine via ODBC.')
parser.add_argument('--DSN', help='DSN to use')
parser.add_argument('--UID', help='UID to use')
parser.add_argument('--PWD', help='PWD to use')
parser.add_argument('--Server', help='Server to use')
parser.add_argument('-C', metavar='CONNSTR', help='Connection string to use')

args = parser.parse_args()

a = 'DSN=%s'%args.DSN if args.DSN else None
b = 'UID=%s'%args.UID if args.UID else None
c = 'PWD=%s'%args.PWD if args.PWD else None
d = 'Server=%s'%args.Server if args.Server else None
conn_str = ';'.join(filter(None, [a,b,c,d])) if args.DSN else None
conn_str = conn_str if conn_str else args.C
if not conn_str:
  parser.print_help(file=sys.stderr)
  exit()

print('connecting: [%s]' % conn_str)
cnxn = pyodbc.connect(conn_str, autocommit=True)
cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')

cursor = cnxn.cursor()
cursor.execute("drop database if exists db");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create database db");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create table db.mt (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(10), blob nchar(10))");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("insert into db.mt values('2020-10-13 06:44:00.123', 1, 127, 32767, 2147483647, 32769, 123.456, 789.987, 'hello', 'helloworld')")
cursor.close()

cursor = cnxn.cursor()
cursor.execute("insert into db.mt values(?,?,?,?,?,?,?,?,?,?)", "2020-10-13 07:06:00.234", 0, 127, 32767, 32768, 32769, 123.456, 789.987, "hel后lo".encode('utf-8'), "wo哈rlxd129")
##cursor.execute("insert into db.mt values(?,?,?,?,?,?,?,?,?,?)", 1502535178128, 9223372036854775807, 127, 32767, 32768, 32769, 123.456, 789.987, "hel后lo".encode('utf-8'), "wo哈rlxd123");
cursor.close()

cursor = cnxn.cursor()
cursor.execute("""
INSERT INTO db.mt (ts,b,v1,v2,v4,v8,f4,f8,bin,blob) values (?,?,?,?,?,?,?,?,?,?)
""",
"2020-12-12 00:00:00",
'true',
'-127',
'-32767',
'-2147483647',
'-9223372036854775807',
'-1.23e10',
'-11.23e6',
'abcdefghij'.encode('utf-8'),
"人啊大发测试及abc")
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
cursor.execute("insert into db.t values('2020-10-13 06:44:00', 1, 127, 32767, 32768, 32769, 123.456, 789.987, 'hell', 'w我你z')")
cursor.close()

cursor = cnxn.cursor()
cursor.execute("create table db.v (ts timestamp, v1 tinyint, v2 smallint, name nchar(10), ts2 timestamp)")
cursor.close()

params = [ ('2020-10-16 00:00:00.123', 19, '2111-01-02 01:02:03.123'),
           ('2020-10-16 00:00:01',     41, '2111-01-02 01:02:03.423'),
           ('2020-10-16 00:00:02',     57, '2111-01-02 01:02:03.153'),
           ('2020-10-16 00:00:03.009', 26, '2111-01-02 01:02:03.623') ]
cursor = cnxn.cursor()
cursor.fast_executemany = True
print('py:...................')
cursor.executemany("insert into db.v (ts, v1, ts2) values (?, ?, ?)", params)
print('py:...................')
cursor.close()

## cursor = cnxn.cursor()
## cursor.execute("SELECT * from db.v where v1 > ?", 4)
## row = cursor.fetchone()
## while row:
##     print(row)
##     row = cursor.fetchone()
## cursor.close()
## 
## cursor = cnxn.cursor()
## cursor.execute("SELECT * from db.v where v1 > ?", '5')
## row = cursor.fetchone()
## while row:
##     print(row)
##     row = cursor.fetchone()
## cursor.close()

