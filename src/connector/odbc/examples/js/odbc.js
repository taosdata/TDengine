#!/usr/bin/env node

const odbc = require('odbc');
const path = require('path');

function usage() {
  var arg = path.basename(process.argv[1]);
  console.error(`usage:`);
  console.error(`${arg} --DSN <DSN> --UID <uid> --PWD <pwd> --Server <host:port>`);
  console.error(`${arg} -C <conn_str>`);
  console.error(`  conn_str eg: 'DSN={TAOS_DSN};UID=root;PWD=taosdata;Server=host:port'`);
}

var cfg = { };

if (process.argv.length==2) {
    usage();
    process.exit(0);
}

var i;
for (i=2; i<process.argv.length; ++i) {
  var arg = process.argv[i];
  if (arg=='-h') {
    usage();
    process.exit(0);
  }
  if (arg=="--DSN") {
    ++i;
    if (i>=process.argv.length) {
      console.error(`expecting <dns> after --DSN but got nothing`);
      usage(process.argv[1]);
      process.exit(1);
    }
    arg = process.argv[i];
    cfg.dsn = arg;
    continue;
  }
  if (arg=="--UID") {
    ++i;
    if (i>=process.argv.length) {
      console.error(`expecting <uid> after --UID but got nothing`);
      usage(process.argv[1]);
      process.exit(1);
    }
    arg = process.argv[i];
    cfg.uid = arg;
    continue;
  }
  if (arg=="--PWD") {
    ++i;
    if (i>=process.argv.length) {
      console.error(`expecting <pwd> after --PWD but got nothing`);
      usage(process.argv[1]);
      process.exit(1);
    }
    arg = process.argv[i];
    cfg.pwd = arg;
    continue;
  }
  if (arg=="--Server") {
    ++i;
    if (i>=process.argv.length) {
      console.error(`expecting <host:port> after --Server but got nothing`);
      usage(process.argv[1]);
      process.exit(1);
    }
    arg = process.argv[i];
    cfg.server = arg;
    continue;
  }
  if (arg=="-C") {
    ++i;
    if (i>=process.argv.length) {
      console.error(`expecting <conn_str> after -C but got nothing`);
      console.error(`  conn_str eg: 'DSN={TAOS_DSN};UID=root;PWD=taosdata;Server=host:port'`);
      process.exit(1);
    }
    arg = process.argv[i];
    cfg.conn_str = arg;
    continue;
  }
  console.error(`unknown argument: [${arg}]`);
  usage(process.argv[1]);
  process.exit(1);
}

var connectionString = cfg.conn_str;

if (!cfg.conn_str) {
  connectionString = `DSN={${cfg.dsn}}; UID=${cfg.uid}; PWD=${cfg.pwd}; Server=${cfg.server}`;
}

(async function () {
    const connStr = connectionString;
    try {
      console.log(`connecting [${connStr}]...`);
      const connection = await odbc.connect(connStr);
      await connection.query('create database if not exists m');
      await connection.query('use m');
      await connection.query('drop table if exists t');
      await connection.query('create table t (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(10), name nchar(3))');
      await connection.query('insert into t values("2020-01-02 12:34:56.781", 1, 127, 32767, 32768, 32769, 123.456, 789.987, "hello", "我和你")');
      console.log('.........');
      result = await connection.query('select * from t');
      console.log(result[0]);


      statement = await connection.createStatement();
      await statement.prepare('INSERT INTO t (ts, v1) VALUES(?, ?)');
      await statement.bind(['2020-02-02 11:22:33.449', 89]);
      result = await statement.execute();
      console.log(result);

      result = await connection.query('select * from t');
      console.log(result[0]);
      console.log(result[1]);
    } catch (e) {
      console.log('error:', e);
    }
})();

