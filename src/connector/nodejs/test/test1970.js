const taos = require('../tdengine');
var conn = taos.connect();
var c1 = conn.cursor();
let stime = new Date();
let interval = 1000;
console.log(stime+"===="+stime.getTime())
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
const dbname = "nodejs_1970_db";
const tbname = "t1";
console.log("drop database if exists "+dbname);
c1.execute("drop database if exists "+dbname);
console.log("create database  "+dbname + " keep 36500");
c1.execute("create database  "+dbname + " keep 36500");
console.log("use " + dbname)
c1.execute("use " + dbname);
console.log('create table if not exists ' + tbname);
c1.execute('create table if not exists ' + tbname+"(ts timestamp,id int)");
//c1.execute('use ' + dbname)
//1969-12-31 23:59:59.999
//1970-01-01 00:00:00.000
//1970-01-01 07:59:59.999
//1970-01-01 08:00:00.000a
//1628928479484  2021-08-14 08:07:59.484
console.log("insert into "+dbname+"."+tbname+" values('1969-12-31 23:59:59.999',1)");
c1.execute("insert into "+dbname+"."+tbname+" values('1969-12-31 23:59:59.999',1)");

console.log("insert into "+dbname+"."+tbname+" values('1970-01-01 00:00:00.000',2)");
c1.execute("insert into "+dbname+"."+tbname+" values('1970-01-01 00:00:00.000',2)");
 
console.log("insert into "+dbname+"."+tbname+" values('1970-01-01 07:59:59.999',3)");
  c1.execute("insert into "+dbname+"."+tbname+" values('1970-01-01 07:59:59.999',3)");
	
console.log("insert into "+dbname+"."+tbname+" values('1970-01-01 08:00:00.000',4)");
  c1.execute("insert into "+dbname+"."+tbname+" values('1970-01-01 08:00:00.000',4)");

console.log("insert into "+dbname+"."+tbname+" values('2021-08-14 08:07:59.484',5)");
  c1.execute("insert into "+dbname+"."+tbname+" values('2021-08-14 08:07:59.484',5)");

// Select
console.log("select * from "+dbname+"."+tbname);
c1.execute("select * from "+dbname+"."+tbname);

var d = c1.fetchall();
console.log(c1.fields);
for (let i=0 ; i<d.length;i++)
console.log(d[i][0].valueOf());

//initialize
console.log('drop table if exists ' + tbname);
c1.execute('drop table if exists ' + tbname);

console.log('create table if not exists ' + tbname);
c1.execute('create table if not exists ' + tbname+"(ts timestamp,id int)");
c1.execute('use ' + dbname)
//-28800001 1969-12-31 23:59:59.999
//-28800000 1970-01-01 00:00:00.000
//-1 1970-01-01 07:59:59.999
//0  1970-01-01 08:00:00.00
//1628928479484  2021-08-14 08:07:59.484
console.log("insert into "+dbname+"."+tbname+" values(-28800001,11)");
c1.execute("insert into "+dbname+"."+tbname+" values(-28800001,11)");

console.log("insert into "+dbname+"."+tbname+" values(-28800000,12)");
c1.execute("insert into "+dbname+"."+tbname+" values(-28800000,12)");

console.log("insert into "+dbname+"."+tbname+" values(-1,13)");
  c1.execute("insert into "+dbname+"."+tbname+" values(-1,13)");

console.log("insert into "+dbname+"."+tbname+" values(0,14)");
  c1.execute("insert into "+dbname+"."+tbname+" values(0,14)");

console.log("insert into "+dbname+"."+tbname+" values(1628928479484,15)");
  c1.execute("insert into "+dbname+"."+tbname+" values(1628928479484,15)");

// Select
console.log("select * from "+dbname+"."+tbname);
c1.execute("select * from "+dbname+"."+tbname);

var d = c1.fetchall();
console.log(c1.fields);
for (let i=0 ; i<d.length;i++)
console.log(d[i][0]);


setTimeout(function () {
  conn.close();
}, 2000);
