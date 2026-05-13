#!/usr/bin/env node

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

const assert = require('assert');
const odbc = require('odbc');


async function connectToDatabase(conn_str) {
  try {
    const conn = await odbc.connect(conn_str);
    await conn.close();
    return 0;
  } catch (error) {
    console.error(error);
    return -1;
  }
}

async function execute(conn_str, sql) {
  var r = -1;
  const conn = await odbc.connect(conn_str);
  try {
    const result = await conn.query(sql);
    // console.log(result);
    // const cursor = await conn.query(sql, {cursor: true, fetchSize: 1});
    // const result = await cursor.fetch();
    // console.log(result);
    // await cursor.close();
    r = 0;
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

function stringify(v) {
  // NOTE: https://github.com/GoogleChromeLabs/jsbi/issues/30
  //       `Do not know how to serialize a BigInt`
  var result = JSON.stringify(v, (key, value) =>
      typeof value === 'bigint'
      ? value.toString()
      : value);

  return result;
}

function stringify_by_key(v, ks) {
  var result = JSON.stringify(v, (key, value) =>
      ks.includes(key)
      ? value.toString()
      : value);
  return result;
}

async function cursor_collect(cursor, fetch_once) {
  var rows = [];
  while (!cursor.noData) {
    var result = await cursor.fetch();
    var i = 0;
    while (result[i]) {
      rows.push(result[i]);
      ++i;
    }
    if (fetch_once) break;
  }

  return rows;
}

async function rs_collect(rs) {
  var rows = [];
  var i = 0;
  while (rs[i]) {
    rows.push(rs[i]);
    ++i;
  }

  return rows;
}

async function conn_exec_check(conn, sql, fetch_size, exp, fetch_once) {
  var rows;
  if (fetch_size > 0) {
    var cursor = await conn.query(sql, {cursor: true, fetchSize: fetch_size});
    rows = await cursor_collect(cursor, fetch_once);
    await cursor.close();
  } else {
    var result = await conn.query(sql);
    rows = await rs_collect(result);
  }
  assert.equal(stringify(rows), exp);
  return 0;
}

async function conn_ins_check(conn, sql, binds) {
  var r = -1;
  const stmt = await conn.createStatement();

  try {
    await stmt.prepare(sql);
    await stmt.bind(binds);
    await stmt.execute();

    r = 0;
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await stmt.close();
  return r;
}

async function stmt_bind_exec_check(stmt, params, result) {
  var r = -1;
  try {
    await stmt.bind(params);
    var rs = await stmt.execute();
    if (result && rs) result.push(rs);
    r = 0;
  } catch (error) {
    throw (error);
  }

  return r;
}

async function conn_prepare_ins_check(conn, sql, params) {
  var r = -1;
  var stmt = await conn.createStatement();

  try {
    await stmt.prepare(sql);
    r = await stmt_bind_exec_check(stmt, params);
    await stmt.close();
  } catch (error) {
    await stmt.close();
    throw(error);
  }

  return r;
}

async function conn_prepare_exec_check(conn, sql, params, expx) {
  var r = -1;
  var stmt = await conn.createStatement();

  try {
    await stmt.prepare(sql);
    await stmt.bind(params);
    var result = await stmt.execute();
    {
      var resultx = stringify(result);
      assert.equal(resultx, expx);
    }
    r = 0;
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await stmt.close();

  return r;
}


async function case1(conn_str, ws) {
  var r = -1;
  const conn = await odbc.connect(conn_str);
  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)');

    var exp = [
        {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人'},
        {ts:'2022-09-11 09:57:29.753', name:'name2', age:30, sex:'female', text:'苏州人'},
        {ts:'2022-09-11 09:57:30.754', name:'name3', age:null, sex:null, text:null},
    ];

    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 3, stringify_by_key(exp, [])), 0);
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 0, stringify_by_key(exp, [])), 0);
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 1, stringify_by_key(exp, [])), 0);
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 2, stringify_by_key(exp, [])), 0);
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 200, stringify_by_key(exp, [])), 0);

    r = 0;
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();

  return r;
}

async function case2(conn_str, ws) {
  var r = -1;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, v timestamp)');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, v) values (1662861448752, 1662871448752)');
    await conn.query('insert into t (ts, v) values (1662861449753, 1662871449753)');
    await conn.query('insert into t (ts, v) values (1662861450754, 1662871450754)');
    await conn.query('select * from t');

    var exp = [
      {ts:'2022-09-11 09:57:28.752', v:'2022-09-11 12:44:08.752'},
      {ts:'2022-09-11 09:57:29.753', v:'2022-09-11 12:44:09.753'},
      {ts:'2022-09-11 09:57:30.754', v:'2022-09-11 12:44:10.754'},
      {ts:'2022-09-11 09:57:31.755', v:'2022-09-11 12:44:11.755'},
    ];

    if (!ws) {
      assert.equal(!!await conn_ins_check(conn, 'insert into t values (?, ?)', [1662861451755, 1662871451755], []), 0);
      assert.equal(!!await conn_exec_check(conn, 'select v from t where ts = 1662861451755', 0, stringify_by_key([{v:'2022-09-11 12:44:11.755'}], [])), 0);
      assert.equal(!!await conn_exec_check(conn, 'select * from t', 4, stringify_by_key(exp, [])), 0);
    }

    r = 0;
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case3(conn_str, ws) {
  var r = -1;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, v varchar(10))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, v) values (1662861448752, "name1")');
    await conn.query('insert into t (ts, v) values (1662861449753, "name2")');
    await conn.query('insert into t (ts, v) values (1662861450754, "name3")');
    await conn.query('select * from t');

    var exp = [
      {ts:'2022-09-11 09:57:28.752', v:'name1'},
      {ts:'2022-09-11 09:57:29.753', v:'name2'},
      {ts:'2022-09-11 09:57:30.754', v:'name3'},
      {ts:'2022-09-11 09:57:31.755', v:'name4'},
    ];

    if (!ws) {
      assert.equal(!!await conn_prepare_ins_check(conn, 'insert into t values(?, ?)', [1662861451755, 'name4']), 0);
      assert.equal(!!await conn_exec_check(conn, 'select * from t', 5, stringify_by_key(exp, [])), 0);
    }

    r = 0;
  } catch (error) {
    console.log(error);
  }

  await conn.close();
  return r;
}

async function case4(conn_str, ws) {
  var r = -1;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, v int)');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, v) values (1662861448752, 1)');
    await conn.query('insert into t (ts, v) values (1662861449753, 2)');
    await conn.query('insert into t (ts, v) values (1662861450754, 3)');
    await conn.query('select * from t');

    var exp = [
      {ts:'2022-09-11 09:57:28.752', v:1},
      {ts:'2022-09-11 09:57:29.753', v:2},
      {ts:'2022-09-11 09:57:30.754', v:3},
      {ts:'2022-09-11 09:57:31.755', v:4},
    ];
    if (!ws) {
      assert.equal(!!await conn_prepare_ins_check(conn, 'insert into t values(?, ?)', [1662861451755, 4]), 0);
      assert.equal(!!await conn_exec_check(conn, 'select * from t', 5, stringify_by_key(exp, [])), 0);
    }

    r = 0;
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case5(conn_str, ws) {
  var r = -1;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, name varchar(5), age int, sex varchar(8), text nchar(3))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)');
    await conn.query('select * from t');

    var exps = [
        {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人'},
        {ts:'2022-09-11 09:57:29.753', name:'name2', age:30, sex:'female', text:'苏州人'},
        {ts:'2022-09-11 09:57:30.754', name:'name3', age:null, sex:null, text:null},
        {ts:'2022-09-11 09:57:31.755', name:'name4', age:40, sex:'male', text:'外星人'},
        {ts:'2022-09-11 09:57:32.756', name:'12345', age:50, sex:'female', text:'类地人'},
        // {ts:'2022-09-11 09:57:33.757', name:null, age:null, sex:null, text:null},
        {ts:'2022-09-11 09:57:34.758', name:'54321', age:60, sex:'unknown', text:'测试人'},
    ];

    stmt = await conn.createStatement();

    r = 0;
    if (!ws) {
      try {
        await stmt.prepare('insert into t values (?,?,?,?,?)');
        assert.equal(!!await stmt_bind_exec_check(stmt, [1662861451755, 'name4', 40, 'male', '外星人']), 0);
        assert.equal(!!await conn_exec_check(conn, 'select * from t', 4, stringify_by_key([exps[0],exps[1],exps[2],exps[3]], []), 1), 0);
        assert.equal(!!await stmt_bind_exec_check(stmt, ['1662861452756', '12345', 50, 'female', '类地人']), 0);
        assert.equal(!!await conn_exec_check(conn, 'select * from t', 5, stringify_by_key([exps[0],exps[1],exps[2],exps[3],exps[4]], []), 1), 0);
        // assert.equal(!!await stmt_bind_exec_check(stmt, ['1662861453757', null, null, null, null]), 0);
        // assert.equal(!!await conn_exec_check(conn, 'select * from t', 6, stringify_by_key([exps[0],exps[1],exps[2],exps[3],exps[4],exps[5]], []), 1), 0);

        r = 0;
      } catch (error) {
        console.error(error);
        r = -1;
      }

    }

    await stmt.close();

    if (r == 0 && !ws) {
      r = -1;
      stmt = await conn.createStatement();

      try {
        await stmt.prepare('select * from t where text = ?');

        var rs = [];
        assert.equal(!!await stmt_bind_exec_check(stmt, ['苏州人'], rs), 0);
        result = rs[0];
        {
          var rows = [result[0]];
          var exp = [exps[1]];
          assert.equal(stringify(rows), stringify_by_key(exp, []));
        }

        // TODO: seems like memory leakage here!!!
        //       need to check with raw-c-test-case
        rs = [];
        assert.equal(!!await stmt_bind_exec_check(stmt, ['中国人'], rs), 0);
        result = rs[0];
        {
          var rows = [result[0]];
          var exp = [exps[0]];
          assert.equal(stringify(rows), stringify_by_key(exp, []));
        }
        r = 0;
      } catch (error) {
        r = -1;
      }
      await stmt.close();
    }
  } catch (error) {
    console.log(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case6(conn_str, ws) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query(`
    create table if not exists t (
    ts timestamp, name varchar(5), age int, sex varchar(8), text nchar(3),
    bi bigint, f float, d double, bin binary(10), si smallint, ti tinyint, b bool,
    tu tinyint unsigned, su smallint unsigned, iu int unsigned
    )
    `);
    // FIXME: why `binary` maps to `TSDB_DATA_TYPE_VARCHAR` rather than `TSDB_DATA_TYPE_VARBINARY`
    //        check with 'desc foo.t' in taos shell
    // FIXME: bigint, smallint, tinyint seemly results in memory leakage
    // await conn.query('select * from t');
    // await conn.query('select ts, name, age, sex, text, bi, f, d, bin, si, ti, b from t');
    await conn.query(`
    insert into t (ts, name, age, sex, text, bi, f, d, bin, si, ti, b, tu, su, iu)
    values (1662861448752, "name1", 20, "male", "中国人", 1234567890123, 1.23, 3.45, "def", -32767, -127, 1, 130, 41234, 4123456789)
    `);
    // // await conn.query('insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")');
    // // await conn.query('insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)');
    var result = await conn.query('select * from t');
    var exp = [
      {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人', bi:1234567890123, f:1.23, d:3.45, bin:'def', si:-32767, ti:-127, b:'true', tu:130, su:41234, iu:'4123456789'},
    ];
    var expx = stringify_by_key(exp, ['bi']);
    // NOTE: https://github.com/GoogleChromeLabs/jsbi/issues/30
    //       `Do not know how to serialize a BigInt`
    var resultx = stringify(result);
    assert.equal(resultx, expx);

    var exps = [
        {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人'},
        {ts:'2022-09-11 09:57:29.753', name:'name2', age:30, sex:'female', text:'苏州人'},
        {ts:'2022-09-11 09:57:30.754', name:'name3', age:null, sex:null, text:null},
        {ts:'2022-09-11 09:57:31.755', name:'name4', age:40, sex:'male', text:'外星人'},
        {ts:'2022-09-11 09:57:32.756', name:'12345', age:50, sex:'female', text:'类地人'},
        {ts:'2022-09-11 09:57:33.757', name:null, age:null, sex:null, text:null},
        {ts:'2022-09-11 09:57:34.758', name:'54321', age:60, sex:'unknown', text:'测试人'},
    ];

    if (!ws) {
      // NOTE: taosc: because there's no param-meta info for non-insert-statement
      //       taos_odbc has to fall-back to TSDB_DATA_TYPE_VARCHAR/SQL_VARCHAR pair in SQLDescribeParam,
      //       and returns SQL_VARCHAR to application, such as node.odbc
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where name = ?', ['name1'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where age = ?', [20], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where age = ?', ['20'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where text = ?', ['中国人'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where bi = ?', [1234567890123], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where bi = ?', ['1234567890123'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where f = ?', [1.23], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where f = ?', ['1.23'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where d = ?', [3.45], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where d = ?', ['3.45'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where d = ?', [3.45e+0], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where d = ?', ['3.45e+0'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where d = ?', [345e-2], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where d = ?', ['345e-2'], expx), 0);
      // taosc: this would fail `Invalid timestamp format`
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where ts = ?', ['1662861448752'], expx), 1);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where ts = ?', [1662861448752], expx), 0);
      // taosc: but this goes as expected
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from foo.t where ts = ?', ['2022-09-11 09:57:28.752'], expx), 0);
    }

  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case0(conn_str, ws) {
  var r = -1;
  const conn = await odbc.connect(conn_str);
  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, name varchar(20), age double, sex varchar(8), text nchar(3))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)');

    var exp = [
        {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人'},
        {ts:'2022-09-11 09:57:29.753', name:'name2', age:30, sex:'female', text:'苏州人'},
        {ts:'2022-09-11 09:57:30.754', name:'name3', age:null, sex:null, text:null}];
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 1, stringify_by_key([exp[0]], []), 1), 0);
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 3, stringify_by_key(exp, []), 1), 0);
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 0, stringify_by_key(exp, [])), 0);
    assert.equal(!!await conn_exec_check(conn, 'select * from foo.t', 2, stringify_by_key([exp[0],exp[1]], []), 1), 0);

    r = 0;
  } catch (error) {
    console.log(error);
  }

  await conn.close();
  return r;
}

async function case7(conn_str, ws) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query(`
    create table if not exists t (ts timestamp, age int)
    `);

    if (!ws) {
      assert.equal(!!await conn_ins_check(conn, 'insert into t (ts, age) values (?, ?)', [1662861448752,20], []), 0);
      assert.equal(!!await conn_exec_check(conn, 'select age from t where ts = 1662861448752', 0, stringify_by_key([{age:20}], [])), 0);
    }

  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case8(conn_str, ws) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query(`
    create stable s (ts timestamp, v int) tags (id int)
    `);
    await conn.query(`
    insert into t using s tags (1) values(now(), 11)
    `);

    if (!ws) {
      assert.equal(!!await conn_ins_check(conn, 'insert into t (ts, v) values (?, ?)', [1662861448752,20], []), 0);
      assert.equal(!!await conn_exec_check(conn, 'select v from t where ts = 1662861448752', 0, stringify_by_key([{v:20}], [])), 0);
    }

  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case9(conn_str, ws) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query(`
    create table if not exists t (
    ts timestamp, name varchar(5), age int, sex varchar(8), text nchar(3),
    bi bigint, f float, d double, bin binary(10), si smallint, ti tinyint, b bool
    )
    `);

    var exp = [
      {ts:"2022-09-11 09:57:28.752",name:"name1",age:30,sex:'male',text:'foo',bi:12345,f:123.45,d:543.21,bin:'bin',si:234,ti:34,b:'true'}
    ];
    var expx = stringify_by_key(exp, ['bi']);

    if (!ws) {
      assert.equal(!!await conn_ins_check(conn, `
      insert into t (ts, name, age, sex, text, bi, f, d, bin, si, ti, b)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [1662861448752, 'name1', 30, 'male', 'foo', 12345, 123.45, 543.21, 'bin', 234, 34, 1], []), 0);
      assert.equal(!!await conn_exec_check(conn, 'select * from t where ts = 1662861448752', 0, expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where ts = ?', ['2022-09-11 09:57:28.752'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where name = ?', ['name1'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where age = ?', ['30'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where age = ?', [30], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where age > ?', ['29'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where age > ?', [29], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where age < ?', ['31'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where age < ?', [31], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where age > ? and age < ?', [29, 31], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where sex = ?', ['male'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where text = ?', ['foo'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where bi = ?', ['12345'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where bi = ?', [12345], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where bin = ?', ['bin'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where si = ?', ['234'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where si = ?', [234], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where ti = ?', ['34'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where ti = ?', [34], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where f > ?', [123.44], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where f < ?', [123.46], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where f > ? and f < ?', [123.44, 123.46], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where f > ? and f < ?', [123.45, 123.45], expx), 1);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where f = ?', ['123.45'], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where f = ?', [123.45], expx), 1);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where d = ?', [543.21], expx), 0);
      assert.equal(!!await conn_prepare_exec_check(conn, 'select * from t where d = ? and f < ?', [543.21, 123.46], expx), 0);
    }
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case10(conn_str, ws) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query(`
    create stable s (ts timestamp, v int) tags (id int)
    `);

    if (!ws) {
      assert.equal(!!await conn_ins_check(conn, 'insert into ? using s tags (?) values (?, ?)', ['t',3,1662861448752,33], []), 0);
      assert.equal(!!await conn_exec_check(conn, 'select v from t where ts = 1662861448752', 0, stringify_by_key([{v:33}], [])), 0);
    }
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function test_sql_server() {
  const conn = await odbc.connect('DSN=SQLSERVER_ODBC_DSN')
  var result = await conn.query('select name, mark from x');
  rows = await rs_collect(result);
  console.log(rows);
  await conn.close();
}

async function test_chars(conn_str, ws) {
  const conn = await odbc.connect(conn_str);
  var result = await conn.query('select name, mark from foo.x');
  rows = await rs_collect(result);
  console.log(rows);
  await conn.close();
}

async function test_params(conn_str, ws) {
  var r = -1;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('create table foo.t (ts timestamp, name varchar(20), mark nchar(20))');

    stmt = await conn.createStatement();

    if (!ws) {
      try {
        await stmt.prepare('insert into foo.t(ts, name) values (?,?)');
        assert.equal(!!await stmt_bind_exec_check(stmt, [1662861451755, '检验']), 0);

        r = 0;
      } catch (error) {
        console.error(error);
        r = -1;
      }
    }

    await stmt.close();
  } catch (error) {
    console.log(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function do_test_cases(conn_str, ws) {
  if (false) assert.equal(!!await test_chars(conn_str, ws), 0)
  if (false) assert.equal(!!await test_sql_server(), 0)
  if (false) assert.equal(!!await test_params(conn_str, ws),0);
  if (false) assert.equal(0)
  // // assert.equal(!!await execute('DSN=TAOS_ODBC_WS_DSN', 'select ts, name from foo.t'),0);
  // // assert.equal(!!await execute('DSN=TAOS_ODBC_WS_DSN; LEGACY', 'select * from foo.t'),1);
  assert.equal(!!await case0(conn_str, ws),0);
  assert.equal(!!await case1(conn_str, ws),0);
  assert.equal(!!await case2(conn_str, ws),0);
  assert.equal(!!await case3(conn_str, ws),0);
  assert.equal(!!await case4(conn_str, ws),0);
  assert.equal(!!await case5(conn_str + ';UNSIGNED_PROMOTION=1;CHARSET_ENCODER_FOR_PARAM_BIND=UTF-8', ws),0);
  assert.equal(!!await case6(conn_str + ';UNSIGNED_PROMOTION=1;CHARSET_ENCODER_FOR_PARAM_BIND=UTF-8', ws),0);

  assert.equal(!!await case7(conn_str, ws),0);
  assert.equal(!!await case8(conn_str, ws),0);
  assert.equal(!!await case9(conn_str, ws),0);
  assert.equal(!!await case10(conn_str, ws),0);

  //assert.equal(0, 1);
  return 0;
}

(async () => {
  assert.equal(!!await connectToDatabase('DSN=xyz'),1);
  // assert.equal(!!await execute('DSN=TAOS_ODBC_WS_DSN', 'xshow databases'),1);
  assert.equal(!!await do_test_cases('DSN=TAOS_ODBC_DSN', 0), 0);
  // TODO: how to build taosadapter on Windows Platform?
  // assert.equal(!!await do_test_cases('DSN=TAOS_ODBC_WS_DSN', 1), 0);
  console.log("==Success==");
})()

