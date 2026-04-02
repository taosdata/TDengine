###############################################################################
# MIT License
#
# Copyright (c) 2022-2023 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

import pyodbc
import os
import sys

if False:
  cnxn = pyodbc.connect('DSN=SQLSERVER_ODBC_DSN')
  cursor = cnxn.cursor()
  x = cursor.execute("select name,mark from x").fetchall()
  cursor.close()
  cnxn.close()
  print(x, file=sys.stderr)

if False:
  cnxn = pyodbc.connect('DSN=TAOS_ODBC_DSN')
  cursor = cnxn.cursor()
  x = cursor.execute("select name,mark from foo.x").fetchall()
  cursor.close()
  cnxn.close()
  print(x, file=sys.stderr)


def test_case0():
  # Specifying the ODBC driver, server name, database, etc. directly
  server = os.getenv('DB_SERVER', '127.0.0.1:6030')
  driver = os.getenv('DB_DRIVER', 'TAOS_ODBC_DRIVER')
  database = os.getenv('DB_DATABASE', 'information_schema')
  uid = os.getenv('DB_UID', 'root')
  pwd = os.getenv('DB_PWD', 'taosdata')

  info = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}'
  print(info)
  cnxn = pyodbc.connect(info)
  cnxn.close()

  # Using a DSN, but providing a password as well
  cnxn = pyodbc.connect('DSN=TAOS_ODBC_DSN;PWD=taosdata')
  cnxn.setencoding(encoding='utf-8')

  # Create a cursor from the connection
  cursor = cnxn.cursor()

  cursor.execute("drop database if exists foo")
  cursor.execute("create database if not exists foo")
  cursor.execute("use foo")

  cursor.execute("select * from information_schema.ins_tables")
  while True:
    row = cursor.fetchone()
    if not row:
      break
    print(row, file=sys.stderr)

  cursor.execute("drop table if exists x")
  cursor.execute("create table x (ts timestamp, name varchar(20), mark nchar(20))")
  cursor.execute("insert into x(ts, name, mark) values (now(), '测试', '试验')")
  # cnxn.commit()
  cursor.execute("insert into x(ts, name, mark) values (?, ?, ?)", 1682565350033, 'xes', 'no')
  # cnxn.commit()
  x = cursor.execute("select name,mark from x").fetchall()
  print(x, file=sys.stderr)
  y = [('xes', 'no'), ('测试', '试验')]
  assert str(x) == str(y), "{0} != {1}".format(x, y)

  cursor.execute("drop stable if exists st")
  cursor.execute("create stable st (ts timestamp, age int) tags (name varchar(20))")
  cursor.execute("insert into 'suzhou' using st tags ('jiangsu') values (1665226861289, 100)")
  x = cursor.execute("select name,age from st").fetchall()
  print(x, file=sys.stderr)
  y = [('jiangsu', 100)]
  assert str(x) == str(y), "{0} != {1}".format(x, y)

  cursor.execute("drop stable if exists st")
  cursor.execute("create stable st (ts timestamp, age int) tags (name varchar(20))")
  cursor.execute("insert into ? using st tags (?) values (?, ?)", 'suzhou', 'jiangsu', 1665226861289, 100)
  x = cursor.execute("select age,name from st").fetchall()
  print(x, file=sys.stderr)
  y = [(100, 'jiangsu')]
  assert str(x) == str(y), "{0} != {1}".format(x, y)

  cursor.execute("drop stable if exists st")
  cursor.execute("create stable st (ts timestamp, age int) tags (name varchar(20))")
  params = [ ('suzhou', 'jiangsu', 1665226861289, 101), ('shanghai', 'Shanghai', 1665226861299, 202) ]
  cursor.fast_executemany = False
  cursor.executemany("insert into ? using st tags (?) values (?, ?)", params)

  x = cursor.execute("select name,age from st").fetchall()
  print(x, file=sys.stderr)
  y = [('jiangsu', 101), ('Shanghai', 202)]
  assert str(x) == str(y), "{0} != {1}".format(x, y)

  cursor.execute("drop table if exists x")
  cursor.execute("create table x (ts timestamp, name varchar(20), mark nchar(20))")
  cursor.execute("insert into x(ts, name, mark) values (now(), '测试', '试验')")
  # cnxn.commit()
  cursor.execute("insert into x(ts, name, mark) values (?, ?, ?)", 1682565350033, '试验', '测试')
  # cnxn.commit()
  x = cursor.execute("select name,mark from x").fetchall()
  print(x, file=sys.stderr)
  y = [('试验', '测试'), ('测试', '试验')]
  assert str(x) == str(y), "{0} != {1}".format(x, y)

  cursor.execute("drop table if exists x")
  cursor.execute("create table x (ts timestamp, dbl double)")
  cursor.execute("insert into x(ts, dbl) values (now(), 1.23)")
  # cnxn.commit()
  cursor.execute("insert into x(ts, dbl) values (?, ?)", 1682565350033, 2.34)
  # cnxn.commit()
  x = cursor.execute("select dbl from x").fetchall()
  print(x, file=sys.stderr)
  y = [(2.34,), (1.23,)]
  assert str(x) == str(y), "{0} != {1}".format(x, y)

  cursor.execute("drop table if exists x")
  cursor.execute("create table x (ts timestamp, flt float)")
  cursor.execute("insert into x(ts, flt) values (now(), 1.23)")
  # cnxn.commit()
  cursor.execute("insert into x(ts, flt) values (?, ?)", 1682565350033, 2.34)
  # cnxn.commit()
  x = cursor.execute("select flt from x").fetchall()
  print(x, file=sys.stderr)
  y = [(2.34,), (1.23,)]
  assert str(round(x[0][0],2)) == str(y[0][0]), "1: {0} != {1}".format(x[0][0], y[0][0])
  assert str(round(x[1][0],2)) == str(y[1][0]), "2: {0} != {1}".format(x[1][0], y[1][0])

  cursor.close()
  cnxn.close()

def check_with_values(cnxn, sql, nr_rows, nr_cols, *vals):
  cursor = cnxn.execute(sql)
  assert len(cursor.description) == nr_cols, f"expected {nr_cols} columns, but got =={len(cursor.description)}=="
  row = cursor.fetchone()
  i_row = 0
  idx = 0
  while row:
    assert i_row < nr_rows, "expected {0} rows, but got ==more rows==".format(nr_rows)
      
    for i_col in range(0, nr_cols):
      assert row[i_col] == vals[idx], f"[{i_row+1,i_col+1}]:expected [{vals[idx]}], but got =={row[i_col]}=="
      idx += 1
    i_row += 1
    row = cursor.fetchone()

  cursor.close()

  assert i_row == nr_rows, "expected {0} rows, but got =={1}==".format(nr_rows, i_row)

  pass

def test_charsets():
  cnxn = pyodbc.connect('DSN=TAOS_ODBC_DSN;PWD=taosdata')
  cnxn.execute("drop database if exists foo")
  cnxn.execute("create database if not exists foo")
  cnxn.execute("create table foo.t (ts timestamp, name varchar(20), mark nchar(20))")
  cnxn.execute("insert into foo.t (ts, name, mark) values (now(), 'name', 'mark')")
  cnxn.execute("insert into foo.t (ts, name, mark) values (now()+1s, '测试', '检验')")
  check_with_values(cnxn, "select name from foo.t where name='name'", 1, 1, "name")
  check_with_values(cnxn, "select mark from foo.t where mark='mark'", 1, 1, "mark")
  check_with_values(cnxn, "select name from foo.t where name='测试'", 1, 1, "测试")
  check_with_values(cnxn, "select mark from foo.t where mark='检验'", 1, 1, "检验")
  cnxn.close()


def find_case(test_cases, name):
  for t in test_cases:
    if t.__name__ == name:
      return t
  return None

def run_case(t):
  r = t()
  if r is None:
    return 0
  if r == 0:
    return 0
  print(f"{t.__name__} => {r}:failed")
  exit(1)

def usage(arg0):
  print(f"{arg0} -h\n"
        f"  show this help page\n"
        f"{arg0} [name]...\n"
        f"  running test case `name`\n"
        f"{arg0} -l\n"
        f"  list all test cases")

def run(test_cases):
  if len(sys.argv) == 1:
    for t in test_cases:
      run_case(t)
    return 0

  for i, arg in enumerate(sys.argv):
    if i == 0:
      continue
    if arg == "-h":
      usage(sys.argv[0])
      return 0
    if arg == "-l":
      print("supported test cases:")
      for t in test_cases:
        print(f"  {t.__name__}")
      return 0
    t = find_case(test_cases, arg)
    if t is None:
      print(f"test case `{arg}`:not found")
      return 1
    run_case(t)

if __name__ == "__main__":
  test_cases = [test_case0, test_charsets]

  r = run(test_cases)
  if (r is None) or (r == 0):
    print("==success==")
    exit(0)
  else:
    print("==failure==")
    exit(1)

