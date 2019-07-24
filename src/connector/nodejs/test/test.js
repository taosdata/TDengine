const taos = require('td-connector');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0});
var c1 = conn.cursor();
let stime = new Date();
let interval = 1000;

// Timestamps must be in the form of "YYYY-MM-DD HH:MM:SS.MMM" if they are in milliseconds
//                                   "YYYY-MM-DD HH:MM:SS.MMMMMM" if they are in microseconds
// Thus, we create the following function to convert a javascript Date object to the correct formatting
function convertDateToTS(date) {
  let tsArr = date.toISOString().split("T")
  return "\"" + tsArr[0] + " " + tsArr[1].substring(0, tsArr[1].length-1) + "\"";
}
function R(l,r) {
  return Math.random() * (r - l) - r;
}
function randomBool() {
  if (Math.random() <  0.5) {
    return true;
  }
  return false;
}
c1.execute('create database td_connector_test;');
c1.execute('use td_connector_test;')
c1.execute('create table if not exists all_types (ts timestamp, _int int, _bigint bigint, _float float, _double double, _binary binary(40), _smallint smallint, _tinyint tinyint, _bool bool, _nchar nchar(40));');

for (let i = 0; i < 10000; i++) {
  stime.setMilliseconds(stime.getMilliseconds() + interval);
  let insertData = [convertDateToTS(stime), // Timestamp
                    parseInt( R(-Math.pow(2,31) + 1 , Math.pow(2,31) - 1) ), // Int
                    parseInt( R(-Math.pow(2,31) + 1 , Math.pow(2,31) - 1) ), // BigInt
                    parseFloat( R(-3.4E38, 3.4E38) ), // Float
                    parseFloat( R(-1.7E308, 1.7E308) ), // Double
                    "\"Long Binary\"", // Binary
                    parseInt( R(-32767, 32767) ), // Small Int
                    parseInt( R(-127, 127) ), // Tiny Int
                    randomBool(),
                    "\"Nchars 一些中文字幕\""]; // Bool
  c1.execute('insert into td_connector_test.all_types values(' + insertData.join(',') + ' );');
}

c1.execute('select * from td_connector_test.all_types limit 10 offset 1000;');
var d = c1.fetchall();
console.log(c1.fieldNames);
console.log(d);

c1.execute('select count(*), avg(_int), sum(_float), max(_bigint), min(_double) from td_connector_test.all_types;');
var d = c1.fetchall();
console.log(c1.fieldNames);
console.log(d);

c1.execute('drop database td_connector_test;')

conn.close();
