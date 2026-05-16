/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#![deny(warnings)]

extern crate odbc;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;

extern crate json;

use odbc::*;
use odbc_safe::AutocommitOn;
use odbc_safe::Odbc3;
use std::env;

fn main() {

  env_logger::init();

  // FIXME: better approach?
  let encode = match env::consts::OS {
    "windows" => "GB18030",
    _         => "UTF-8"
  };

  // let env: Environment<Odbc3> = create_environment_v3().unwrap();
  let env: Environment<Odbc3> = create_environment_v3_with_os_db_encoding(encode, encode).unwrap();
  _do_test_cases_in_env(&env);

  println!("==success==")
}

fn _test_connect(env: &Environment<Odbc3>, conn_str: &str) -> bool {
  match env.connect_with_connection_string(conn_str) {
    Ok(_) => { true }
    _     => { false }
  }
}

fn _exec_direct(conn: &Connection<'_, AutocommitOn>, sql: &str) -> Result<bool> {
  let stmt = Statement::with_parent(conn)?;
  let result = stmt.exec_direct(sql)?;
  match result {
    Data(_)   => { Ok(true) }
    NoData(_) => { Ok(false) }
  }
}

fn _test_exec_direct(conn: &Connection<'_, AutocommitOn>, sql: &str) -> bool {
  match _exec_direct(conn, sql) {
    Ok(_) => { true }
    _ => { false }
  }
}

macro_rules! _stmt_to_col_names {
  ( $( $x:expr )+ ) => {
    {
      let stmt = $($x)+;
      let cols = stmt.num_result_cols().unwrap();
      let mut names = Vec::new();
      for i in 1..(cols + 1) {
        names.push(stmt.describe_col(i as u16).unwrap().name);
      }
      names
    }
  };
}

macro_rules! _stmt_collect_rows {
  ( $( $x:expr)+ ) => {
    {
      let stmt = $($x)+;
      let mut jv = json::JsonValue::new_array();
      let names = _stmt_to_col_names!(&stmt);
      while let Some(mut cursor) = stmt.fetch()? {
        let mut row = json::JsonValue::new_object();
        for i in 1..(names.len() + 1) {
          let _k = &names[(i-1) as usize];
          match cursor.get_data::<String>(i as u16)? {
            Some(_val) => {
              row.insert(_k, _val).unwrap();
            }
            None => {
              row.insert(_k, json::JsonValue::Null).unwrap();
            }
          }
        }
        jv.push(row).unwrap();
      };
      jv
    }
  };
}

macro_rules! _rs_to_rows {
  ( $( $x:expr )+ ) => {
    {
      let rs = $($x)+;
      match rs? {
        Data(mut stmt) => {
          let jv = _stmt_collect_rows!(&mut stmt);
          Ok((stmt.close_cursor()?, Some(jv)))
        }
        NoData(stmt) => {
          Ok((stmt, None))
        }
      }
    }
  };
}

fn _stmt_query<'a>(stmt: Statement<'a,'a, Allocated, NoResult, safe::AutocommitOn>, sql: &str) ->
Result<(Statement<'a, 'a, Allocated, NoResult, safe::AutocommitOn>, Option<json::JsonValue>)>
{
  let rs = stmt.exec_direct(sql);
  _rs_to_rows!(rs)
}

fn _stmt_execute<'a>(stmt: Statement<'a,'a, Prepared, NoResult, safe::AutocommitOn>) ->
Result<(Statement<'a, 'a, Prepared, NoResult, safe::AutocommitOn>, Option<json::JsonValue>)>
{
  let rs = stmt.execute();
  _rs_to_rows!(rs)
}

fn _conn_query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> Result<Option<json::JsonValue>> {
  let stmt = Statement::with_parent(conn)?;
  let rs = stmt.exec_direct(sql);
  let (_, jv) = _rs_to_rows!(rs)?;
  Ok(jv)
}

fn _test_query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> bool {
  match _conn_query(conn, sql) {
    Ok(_) => { true }
    _     => { false }
  }
}

fn _case1(conn: &Connection<'_, AutocommitOn>) -> Result<bool> {
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);

  let _parsed = json::array!(
    {ts:r#"2022-09-11 09:57:28.752"#, name:r#"name1"#, age:"20", sex:r#"male"#, text:r#"中国人"#},
    {ts:r#"2022-09-11 09:57:29.753"#, name:r#"name2"#, age:"30", sex:r#"female"#, text:r#"苏州人"#},
    {ts:r#"2022-09-11 09:57:30.754"#, name:r#"name3"#, age:null, sex:null, text:null},
  );

  let _rs = _conn_query(&conn, r#"select * from t"#).unwrap();

  assert_eq!(_rs, Some(_parsed));

  Ok(true)
}

fn _test_case1(conn: &Connection<'_, AutocommitOn>) -> bool {
  _case1(conn).unwrap()
}

macro_rules! _stmt_bind_query_check {
  ( $( $x:expr, $y:expr, $z:expr )+ ) => {
    {
      let stmt = $($x)+;
      let name = $($y)+;
      let expected = $($z)+;
      let stmt = stmt.bind_parameter(1, name).unwrap();
      let (stmt, jv) = _stmt_execute(stmt).unwrap();
      assert_eq!(jv.unwrap(), json::parse(&expected).unwrap());
      stmt
    }
  };
}

fn _test_case2(conn: &Connection<'_, AutocommitOn>) {
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861451755, "3245", null, null, null)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861451756, "测试", null, null, null)"#), true);

  let _parsed = json::array!(
    {ts:r#"2022-09-11 09:57:28.752"#, name:r#"name1"#, age:"20", sex:r#"male"#, text:r#"中国人"#},
    {ts:r#"2022-09-11 09:57:29.753"#, name:r#"name2"#, age:"30", sex:r#"female"#, text:r#"苏州人"#},
    {ts:r#"2022-09-11 09:57:30.754"#, name:r#"name3"#, age:null, sex:null, text:null},
    {ts:r#"2022-09-11 09:57:31.755"#, name:r#"3245"#, age:null, sex:null, text:null},
    {ts:r#"2022-09-11 09:57:31.756"#, name:r#"测试"#, age:null, sex:null, text:null},
  );

  let stmt = Statement::with_parent(conn).unwrap();

  let stmt = stmt.prepare("select * from foo.t where name = ?").unwrap();

  let stmt = _stmt_bind_query_check!(stmt, &"name2", format!("[{}]", _parsed[1].dump()));
  let stmt = _stmt_bind_query_check!(stmt, &"name1", format!("[{}]", _parsed[0].dump()));
  let stmt = _stmt_bind_query_check!(stmt, &"name3", format!("[{}]", _parsed[2].dump()));
  _stmt_bind_query_check!(stmt, &"测试", format!("[{}]", _parsed[4].dump()));

  let stmt = Statement::with_parent(conn).unwrap();
  let stmt = stmt.prepare("select * from foo.t where name <> ?").unwrap();

  _stmt_bind_query_check!(stmt, &"name", _parsed.dump());

  let stmt = Statement::with_parent(conn).unwrap();
  let stmt = stmt.prepare("select * from foo.t where age = ?").unwrap();
  _stmt_bind_query_check!(stmt, &30, format!("[{}]", _parsed[1].dump()));


  let stmt = Statement::with_parent(conn).unwrap();
  let stmt = stmt.prepare("select * from foo.t where name = ?").unwrap();
  _stmt_bind_query_check!(stmt, &3245, format!("[{}]", _parsed[3].dump()));
}

fn _test_case3(conn: &Connection<'_, AutocommitOn>) {
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, age bigint)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861448752, 1662861448752)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861449753, 1662861449753)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861450754, 1662861450754)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861451755, 1662861451755)"#), true);

  let _parsed = json::array!(
    {ts:r#"2022-09-11 09:57:28.752"#, age:r#"1662861448752"#},
    {age:r#"1662861449753"#, ts:r#"2022-09-11 09:57:29.753"#},
    {ts:r#"2022-09-11 09:57:30.754"#, age:r#"1662861450754"#},
    {ts:r#"2022-09-11 09:57:31.755"#, age:r#"1662861451755"#},
  );

  let stmt = Statement::with_parent(conn).unwrap();

  let stmt = stmt.prepare("select * from foo.t where age = ?").unwrap();
  _stmt_bind_query_check!(stmt, &1662861449753i64, format!("[{}]", _parsed[1].dump()));
}

fn do_test_cases_in_conn(conn: &Connection<'_, AutocommitOn>)
{
  assert_eq!(_test_exec_direct(&conn, "xshow databases"), false);
  assert_eq!(_test_exec_direct(&conn, "show databases"), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select "abc" union all select "bcd" union all select "cde""#), true);
  assert_eq!(_test_query(&conn, r#"select "abc" union all select "bcd" union all select "cde""#), true);
  assert_eq!(_test_query(&conn, r#"select * from t"#), true);
  assert_eq!(_test_case1(&conn), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t where name = ?"#), false);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name) values (now, ?)"#), false);
}

fn _do_test_cases_in_env(env: &Environment<Odbc3>) {
  assert_eq!(_test_connect(env, "DSN=xTAOS_ODBC_DSN"), false);
  assert_eq!(_test_connect(env, "DSN=TAOS_ODBC_DSN"), true);
  // NOTE: await taosws's upgrade
  // assert_eq!(_test_connect(env, "DSN=TAOS_ODBC_WS_DSN"), true);

  let conn = env.connect_with_connection_string("DSN=TAOS_ODBC_DSN").unwrap();
  do_test_cases_in_conn(&conn);
  _test_case2(&conn);
  _test_case3(&conn);

  // NOTE: await taosws's upgrade
  // let conn = env.connect_with_connection_string("DSN=TAOS_ODBC_WS_DSN").unwrap();
  do_test_cases_in_conn(&conn);

  // assert_eq!(0, 1);
}

#[cfg(test)]
mod tests {
    #[test]
    fn exploration() {
        assert_eq!(2 + 2, 4);
    }
}

