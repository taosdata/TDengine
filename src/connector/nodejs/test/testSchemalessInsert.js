const taos = require('../tdengine');
const _ = require('lodash');

var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 6030 });
var c1 = conn.cursor();
executeUpdate("drop database if exists nodedb");
executeUpdate("create database if not exists  nodedb keep 36500 precision 'ns'");
executeUpdate("use nodedb");

let now = Date.now(); //  get utc timestamp with milliseconds.
let lines0 = "schemaless,t1=3i64,t2=4f64,t3=\"t2\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 " + (now - 10) * 1_000_000;

c1.schemalessInsert(lines0, taos.SCHEMALESS_PROTOCOL.LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.NANO_SECONDS);
c1.execute("select count(*) from schemaless");
let row = _.first(c1.fetchall());

if (row[0] != 1) {
  throw "insert done but can't select as expected";
}
console.log("insert line protocol with string input: passed");

let lines1 = [
  "schemaless,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 " + (now - 2) * 1_000_000,
  "schemaless,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 " + (now - 1) * 1_000_000,
];

c1.schemalessInsert(lines1, taos.SCHEMALESS_PROTOCOL.LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.NANO_SECONDS);
c1.execute("select count(*) from schemaless");
row = _.first(c1.fetchall());

if (row[0] != 3) {
  throw "insert done but can't select as expected";
}
console.log("insert line protocol with Array<string> input: passed");
// json protocol

let lines2 = "{"
  + "\"metric\": \"stb0_0\","
  + "\"timestamp\": " + (now - 2) + ","
  + "\"value\": 10,"
  + "\"tags\": {"
  + "\"t1\": true,"
  + "\"t2\": false,"
  + "\"t3\": 10,"
  + "\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
  + "}"
  + "}"

console.log(lines2)
c1.schemalessInsert(lines2, 3, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
console.log("insert json protocol with string input: passed");

let lines3 = "{"
  + "\"metric\": \"stb0_0\","
  + "\"timestamp\": " + (now - 1) + ","
  + "\"value\": 10,"
  + "\"tags\": {"
  + "\"t1\": true,"
  + "\"t2\": false,"
  + "\"t3\": 10,"
  + "\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
  + "}"
  + "}"

console.log(lines3)
c1.schemalessInsert(lines3, taos.SCHEMALESS_PROTOCOL.JSON_PROTOCOL, taos.SCHEMALESS_PRECISION.NOT_CONFIGURED);
console.log("insert json protocol with Array<string> input: passed");

setTimeout(() => conn.close(), 2000);

function executeUpdate(sql) {
  console.log(sql);
  c1.execute(sql);
}



