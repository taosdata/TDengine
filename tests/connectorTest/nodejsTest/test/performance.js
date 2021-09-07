function memoryUsageData() {
  let s = process.memoryUsage()
  for (key in s) {
    s[key] = (s[key]/1000000).toFixed(3) + "MB";
  }
  return s;
}
console.log("initial mem usage:", memoryUsageData());

const { PerformanceObserver, performance } = require('perf_hooks');
const taos = require('../tdengine');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0});
var c1 = conn.cursor();

// Initialize env
c1.execute('create database if not exists td_connector_test;');
c1.execute('use td_connector_test;')
c1.execute('create table if not exists all_types (ts timestamp, _int int, _bigint bigint, _float float, _double double, _binary binary(40), _smallint smallint, _tinyint tinyint, _bool bool, _nchar nchar(40));');
c1.execute('create table if not exists stabletest (ts timestamp, v1 int, v2 int, v3 int, v4 double) tags (id int, location binary(20));')


// Insertion into single table Performance Test
var dataPrepTime = 0;
var insertTime = 0;
var insertTime5000 = 0;
var avgInsert5ktime = 0;
const obs = new PerformanceObserver((items) => {
  let entry = items.getEntries()[0];

  if (entry.name == 'Data Prep') {
    dataPrepTime += entry.duration;
  }
  else if (entry.name == 'Insert'){
    insertTime += entry.duration
  }
  else {
    console.log(entry.name + ': ' + (entry.duration/1000).toFixed(8) + 's');
  }
  performance.clearMarks();
});
obs.observe({ entryTypes: ['measure'] });

function R(l,r) {
  return Math.random() * (r - l) - r;
}
function randomBool() {
  if (Math.random() <  0.5) {
    return true;
  }
  return false;
}
function insertN(n) {
  for (let i = 0; i < n; i++) {
    performance.mark('A3');
    let insertData = ["now + " + i + "m", // Timestamp
                      parseInt( R(-Math.pow(2,31) + 1 , Math.pow(2,31) - 1) ), // Int
                      parseInt( R(-Math.pow(2,31) + 1 , Math.pow(2,31) - 1) ), // BigInt
                      parseFloat( R(-3.4E38, 3.4E38) ), // Float
                      parseFloat( R(-1.7E308, 1.7E308) ), // Double
                      "\"Long Binary\"", // Binary
                      parseInt( R(-32767, 32767) ), // Small Int
                      parseInt( R(-127, 127) ), // Tiny Int
                      randomBool(),
                      "\"Nchars 一些中文字幕\""]; // Bool
    let query = 'insert into td_connector_test.all_types values(' + insertData.join(',') + ' );';
    performance.mark('B3');
    performance.measure('Data Prep', 'A3', 'B3');
    performance.mark('A2');
    c1.execute(query, {quiet:true});
    performance.mark('B2');
    performance.measure('Insert', 'A2', 'B2');
    if ( i % 5000 == 4999) {
      console.log("Insert # " + (i+1));
      console.log('Insert 5k records: ' + ((insertTime - insertTime5000)/1000).toFixed(8) + 's');
      insertTime5000 = insertTime;
      avgInsert5ktime = (avgInsert5ktime/1000 * Math.floor(i / 5000) + insertTime5000/1000) / Math.ceil( i / 5000);
      console.log('DataPrepTime So Far: ' + (dataPrepTime/1000).toFixed(8) + 's | Inserting time So Far: ' + (insertTime/1000).toFixed(8) + 's | Avg. Insert 5k time: ' + avgInsert5ktime.toFixed(8));


    }
  }
}
performance.mark('insert 1E5')
insertN(1E5);
performance.mark('insert 1E5 2')
performance.measure('Insert With Logs', 'insert 1E5', 'insert 1E5 2');
console.log('DataPrepTime: ' + (dataPrepTime/1000).toFixed(8) + 's | Inserting time: ' + (insertTime/1000).toFixed(8) + 's');
dataPrepTime = 0; insertTime = 0;
//'insert into td_connector_test.all_types values (now, null,null,null,null,null,null,null,null,null);'
