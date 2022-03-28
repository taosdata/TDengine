const taos = require('../tdengine');
var conn = taos.connect();
var c1 = conn.cursor();
let stime = new Date();
let interval = 1000;

function convertDateToTS(date) {
  let tsArr = date.toISOString().split("T")
  return "\"" + tsArr[0] + " " + tsArr[1].substring(0, tsArr[1].length - 1) + "\"";
}
function R(l, r) {
  return Math.random() * (r - l) - r;
}
function randomBool() {
  if (Math.random() < 0.5) {
    return true;
  }
  return false;
}

// Initialize
//c1.execute('drop database td_connector_test;');
const dbname = 'nodejs_test_us';
c1.execute('create database if not exists ' + dbname + ' precision "us"');
c1.execute('use ' + dbname)
c1.execute('create table if not exists tstest (ts timestamp, _int int);');
c1.execute('insert into tstest values(1625801548423914, 0)');
// Select
console.log('select * from tstest');
c1.execute('select * from tstest');

var d = c1.fetchall();
console.log(c1.fields);
let ts = d[0][0];
console.log(ts);

if (ts.taosTimestamp() != 1625801548423914) {
  throw "microseconds not match!";
}
if (ts.getMicroseconds() % 1000 !== 914) {
  throw "micronsecond precision error";
}
setTimeout(function () {
  c1.query('drop database nodejs_us_test;');
}, 200);

setTimeout(function () {
  conn.close();
}, 2000);
