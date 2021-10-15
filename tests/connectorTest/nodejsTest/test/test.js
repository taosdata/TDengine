const taos = require('../tdengine');
var conn = taos.connect();
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
//c1.execute('drop database td_connector_test;');
c1.execute('create database if not exists td_connector_test;');
c1.execute('use td_connector_test;')
c1.execute('create table if not exists all_types (ts timestamp, _int int, _bigint bigint, _float float, _double double, _binary binary(40), _smallint smallint, _tinyint tinyint, _bool bool, _nchar nchar(40));');
c1.execute('create table if not exists stabletest (ts timestamp, v1 int, v2 int, v3 int, v4 double) tags (id int, location binary(20));')

// Shell Test : The following uses the cursor to imitate the taos shell

// Insert
for (let i = 0; i < 10000; i++) {
  let insertData = ["now+" + i + "s", // Timestamp
                    parseInt( R(-Math.pow(2,31) + 1 , Math.pow(2,31) - 1) ), // Int
                    parseInt( R(-Math.pow(2,31) + 1 , Math.pow(2,31) - 1) ), // BigInt
                    parseFloat( R(-3.4E38, 3.4E38) ), // Float
                    parseFloat( R(-1.7E30, 1.7E30) ), // Double
                    "\"Long Binary\"", // Binary
                    parseInt( R(-32767, 32767) ), // Small Int
                    parseInt( R(-127, 127) ), // Tiny Int
                    randomBool(),
                    "\"Nchars\""]; // Bool
  c1.execute('insert into td_connector_test.all_types values(' + insertData.join(',') + ' );', {quiet:true});
  if (i % 1000 == 0) {
    console.log("Insert # " , i);
  }
}

// Select
console.log('select * from td_connector_test.all_types limit 3 offset 100;');
c1.execute('select * from td_connector_test.all_types limit 2 offset 100;');

var d = c1.fetchall();
console.log(c1.fields);
console.log(d);

// Functions
console.log('select count(*), avg(_int), sum(_float), max(_bigint), min(_double) from td_connector_test.all_types;')
c1.execute('select count(*), avg(_int), sum(_float), max(_bigint), min(_double) from td_connector_test.all_types;');
var d = c1.fetchall();
console.log(c1.fields);
console.log(d);

// Immediate Execution like the Shell

c1.query('select count(*), stddev(_double), min(_tinyint) from all_types where _tinyint > 50 and _int < 0;', true).then(function(result){
  result.pretty();
})

c1.query('select _tinyint, _bool from all_types where _tinyint > 50 and _int < 0 limit 50;', true).then(function(result){
  result.pretty();
})

c1.query('select stddev(_double), stddev(_bigint), stddev(_float) from all_types;', true).then(function(result){
  result.pretty();
})
c1.query('select stddev(_double), stddev(_bigint), stddev(_float) from all_types interval(1m) limit 100;', true).then(function(result){
  result.pretty();
})

// Binding arguments, and then using promise
var q = c1.query('select _nchar from td_connector_test.all_types where ts >= ? and _int > ? limit 100 offset 40;').bind(new Date(1231), 100)
console.log(q.query);
q.execute().then(function(r) {
  r.pretty();
});


// test query null value
c1.execute("create table if not exists td_connector_test.weather(ts timestamp, temperature float, humidity int) tags(location nchar(64))");
c1.execute("insert into t1 using weather tags('北京') values(now, 11.11, 11)");
c1.execute("insert into t1(ts, temperature) values(now, 22.22)");
c1.execute("insert into t1(ts, humidity) values(now, 33)");
c1.query('select * from test.t1', true).then(function (result) {
     result.pretty();
});

var q = c1.query('select * from td_connector_test.weather');
console.log(q.query);
q.execute().then(function(r) {
  	r.pretty();
});

function sleep(sleepTime) {
    for(var start = +new Date; +new Date - start <= sleepTime; ) { }
}

sleep(10000);

// Raw Async Testing (Callbacks, not promises)
function cb2(param, result, rowCount, rd) {
  console.log('CB2 Callbacked!');  
  console.log("RES *", result);
  console.log("Async fetched", rowCount, " rows");
  console.log("Passed Param: ", param);
  console.log("Fields ", rd.fields);
  console.log("Data ", rd.data);
}
function cb1(param,result,code) {
  console.log('CB1 Callbacked!');
  console.log("RES * ", result);
  console.log("Status: ", code);
  console.log("Passed Param ", param);
  c1.fetchall_a(result, cb2, param);
}

c1.execute_a("describe td_connector_test.all_types;", cb1, {myparam:3.141});

function cb4(param, result, rowCount, rd) {
  console.log('CB4 Callbacked!');  
  console.log("RES *", result);
  console.log("Async fetched", rowCount, "rows");
  console.log("Passed Param: ", param);
  console.log("Fields", rd.fields);
  console.log("Data", rd.data);
}
// Without directly calling fetchall_a
var thisRes;
function cb3(param,result,code) {
  console.log('CB3 Callbacked!');
  console.log("RES *", result);
  console.log("Status:", code);
  console.log("Passed Param", param);
  thisRes = result;
}
//Test calling execute and fetchall seperately and not through callbacks
var param = c1.execute_a("describe td_connector_test.all_types;", cb3, {e:2.718});
console.log("Passed Param outside of callback: ", param);
console.log(param);
setTimeout(function(){
  c1.fetchall_a(thisRes, cb4, param);
},100);


// Async through promises
var aq = c1.query('select count(*) from td_connector_test.all_types;',false);
aq.execute_a().then(function(data) {
  data.pretty();
});

c1.query('describe td_connector_test.stabletest').execute_a().then(function(r){
	r.pretty()
});

setTimeout(function(){
  c1.query('drop database td_connector_test;');
},200);

setTimeout(function(){
  conn.close();
},2000);
