package main

import (
    _ "github.com/alexbrainman/odbc"
    "database/sql"
    "log"
    "fmt"
    "os"
    "reflect"
    "runtime"
    "strconv"
    )

func test_sql_server() {
  db, err := sql.Open("odbc", "DSN=SQLSERVER_ODBC_DSN")
  if err != nil {
    log.Fatal(err)
  }

  var (
      name string
      mark string
      )

  // rows, err := db.Query("SELECT * FROM x WHERE name = ?", "测试")
  rows, err := db.Query("SELECT * FROM x")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()
  for rows.Next() {
    err := rows.Scan(&name, &mark)
    if err != nil {
      log.Fatal(err)
    }
    fmt.Println(name, mark)
  }
  err = rows.Err()
  if err != nil {
    log.Fatal(err)
  }

  defer db.Close()
}

func test_case0() int {
  if false {
    test_sql_server()
    log.Fatal("==success==")
  }

  db0, err := sql.Open("odbc", "DSN=TAOS_ODBC_DSN;CHARSET_ENCODER_FOR_COL_BIND=UTF-8")
  // db0, err := sql.Open("odbc", "Driver={TAOS_ODBC_DRIVER}")
  if err != nil {
    log.Fatal(err)
  }
  defer db0.Close()

  var (
      ts string
      name string
      mark string
      )

  res, err := db0.Exec("drop database if exists xyz")
  if err != nil { log.Fatal(err) }
  n, err := res.RowsAffected()
  if err != nil { log.Fatal(err) }
  fmt.Println(n)

  // nn, err := res.LastInsertId()
  // if err != nil { log.Fatal(err) }
  // fmt.Println(nn)

  db, err := sql.Open("odbc", "DSN=TAOS_ODBC_DSN;CHARSET_ENCODER_FOR_COL_BIND=UTF-8")
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  sqls := [...]string {
    "drop database if exists foo",
    "create database if not exists foo",
    "create table foo.x (ts timestamp, name varchar(20), mark nchar(20))",
    "insert into foo.x (ts, name, mark) values (now(), 'name', 'mark')",
    "insert into foo.x (ts, name, mark) values (now()+1s, '人a', '检验')"}
  exec_sqls(db, sqls[:])

  rows, err := db.Query("SELECT * FROM foo.x WHERE name = ?", "人a")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()
  for rows.Next() {
    err := rows.Scan(&ts, &name, &mark)
    if err != nil {
      log.Fatal(err)
    }
    fmt.Println(ts, name, mark)
  }
  err = rows.Err()
  if err != nil {
    log.Fatal(err)
  }

  return 0
}

func exec_sqls(db *sql.DB, sqls []string) {
  for _,sql := range sqls {
    _, err := db.Exec(sql)
    if err != nil {
      log.Fatal(err)
    }
  }
}

func check_with_values(db *sql.DB, sql string, nr_rows int, nr_cols int, values []string) {
  if nr_cols > 10 {
    log.Fatal(fmt.Sprintf("columns %d more than %d:not supported yet", nr_cols, 10))
  }
  rows, err := db.Query(sql)
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()

  cols, err := rows.Columns()
  if err != nil {
    log.Fatal(err)
  }
  _nr_cols := len(cols)

  if _nr_cols != nr_cols {
    s := fmt.Sprintf("expected %d columns, but got =={%d}==", nr_cols, _nr_cols)
    log.Fatal(s)
  }
  i_row := 0
  idx := 0
  var (
    v1  string
    v2  string
    v3  string
    v4  string
    v5  string
    v6  string
    v7  string
    v8  string
    v9  string
    v10 string
  )
  for rows.Next() {
    if i_row >= nr_rows {
      log.Fatal(fmt.Sprintf("expected %d rows, but got ==more rows==", nr_rows))
    }
    if nr_cols == 1 {
      err = rows.Scan(&v1)
    } else if nr_cols == 2 {
      err = rows.Scan(&v1, &v2)
    } else if nr_cols == 3 {
      err = rows.Scan(&v1, &v2, &v3)
    } else if nr_cols == 4 {
      err = rows.Scan(&v1, &v2, &v3, &v4)
    } else if nr_cols == 5 {
      err = rows.Scan(&v1, &v2, &v3, &v4, &v5)
    } else if nr_cols == 6 {
      err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6)
    } else if nr_cols == 7 {
      err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6, &v7)
    } else if nr_cols == 8 {
      err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8)
    } else if nr_cols == 9 {
      err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9)
    } else {
      err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10)
    }
    if err != nil {
      log.Fatal(err)
    }
    v := [...]string {v1, v2, v3, v4, v5, v6, v7, v8, v9, v10}
    for i:=0; i<nr_cols; i++ {
      if v[i] != values[idx+i] {
        log.Fatal(fmt.Sprintf("[%d,%d]:expected [%s], but got ==%s==", i_row+1, i+1, values[idx+i], v[i]))
      }
    }
    i_row += 1
    idx += nr_cols
  }

  if i_row != nr_rows {
    log.Fatal(fmt.Sprintf("expected %d rows, but got ==%d==", nr_rows, i_row))
  }
}

func test_charsets() int {
  var db *sql.DB
  db, err := sql.Open("odbc", "DSN=TAOS_ODBC_DSN;CHARSET_ENCODER_FOR_COL_BIND=UTF-8")
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  sqls := [...]string {
    "drop database if exists foo",
    "create database if not exists foo",
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20))",
    "insert into foo.t (ts, name, mark) values (now(), 'name', 'mark')",
    "insert into foo.t (ts, name, mark) values (now()+1s, '测试', '检验')"}

  exec_sqls(db, sqls[:])
  check_with_values(db, "select name from foo.t where name='name'", 1, 1, []string {"name"})
  check_with_values(db, "select mark from foo.t where mark='mark'", 1, 1, []string {"mark"})
  check_with_values(db, "select name from foo.t where name='测试'", 1, 1, []string {"测试"})
  check_with_values(db, "select mark from foo.t where mark='检验'", 1, 1, []string {"检验"})

  return 0
}


func usage(arg0 string) {
  fmt.Fprintf(os.Stderr,
      "%v -h\n"                           +
      "  show this help page\n"           +
      "%v [name]...\n"                    +
      "  running test case `name`\n"      +
      "%v -l\n"                           +
      "  list all test cases\n",
      arg0, arg0, arg0)
}

func find_case(cases []func() int, name string) func() int {
  for _,c := range cases {
    if runtime.FuncForPC(reflect.ValueOf(c).Pointer()).Name() == "main." + name {
      return c
    }
  }
  return nil
}

func list_cases(cases []func() int) {
  fmt.Println("supported test cases:")
  for _,c := range cases {
    fmt.Println("  " + runtime.FuncForPC(reflect.ValueOf(c).Pointer()).Name()[5:])
  }
}

func run_case(c func() int) {
  r := c()
  if r != 0 {
    log.Fatal(runtime.FuncForPC(reflect.ValueOf(c).Pointer()).Name() + " => " + strconv.Itoa(r) + ":failed")
  }
}

func run_cases(cases []func () int) {
  for _,c := range cases {
    run_case(c)
  }
}

func main() {
  cases := []func () int { test_case0, test_charsets }
  nr_args := len(os.Args)
  nr_cases := 0
  if nr_args > 1 {
    for i:=1; i<nr_args; i+=1 {
      arg := os.Args[i]
      if arg == "-h" {
        usage(os.Args[0])
        os.Exit(0)
      }
      if arg == "-l" {
        list_cases(cases)
        os.Exit(0)
      }
      c := find_case(cases, arg)
      if c == nil {
        log.Fatal("test case `" + arg + "`:not found")
      }
      run_case(c)
      nr_cases += 1
    }
  }

  if nr_cases == 0 {
    run_cases(cases)
  }

  fmt.Println("==success==")
}

