const taos = require('../tdengine');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0});
var c1 = conn.cursor();
let stime = new Date();
let interval = 1000;

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

// Initialize

c1.execute('create database if not exists td_connector_test;');
c1.execute('use td_connector_test;')
c1.execute('create table if not exists all_types (ts timestamp, _int int, _bigint bigint, _float float, _double double, _binary binary(40), _smallint smallint, _tinyint tinyint, _bool bool, _nchar nchar(40));');


// Shell Test : The following uses the cursor to imitate the taos shell

// Insert
for (let i = 0; i < 5000; i++) {
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
  c1.execute('insert into td_connector_test.all_types values(' + insertData.join(',') + ' );', {quiet:true});
  if (i % 1000 == 0) {
    console.log("Insert # " , i);
  }
}

// Select
c1.execute('select * from td_connector_test.all_types limit 10 offset 1000;');
var d = c1.fetchall();
console.log(c1.fields);
console.log(d);

// Functions
c1.execute('select count(*), avg(_int), sum(_float), max(_bigint), min(_double) from td_connector_test.all_types;');
var d = c1.fetchall();
console.log(c1.fields);
console.log(d);

//Immediate Execution like the Shell

c1.query('select count(*), stddev(_double), min(_tinyint) from all_types where _tinyint > 50 and _int < 0', true).then(function(result){
  result.pretty();
})
c1.query('select _tinyint, _bool from all_types where _tinyint > 50 and _int < 0 limit 50;', true).then(function(result){
  result.pretty();
})
c1.query('select stddev(_double), stddev(_bigint), stddev(_float) from all_types', true).then(function(result){
  result.pretty();
})

var q = c1.query('select * from td_connector_test.all_types where ts >= ? and _int > ? limit 100 offset 40;').bind(new Date(1231), 100).execute().then(function(r) {
  r.pretty();
});
console.log(q._query);

c1.execute('drop database td_connector_test;')

conn.close();
