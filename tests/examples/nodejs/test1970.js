const taos = require('td2.0-connector');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
var c1 = conn.cursor(); // Initializing a new cursor

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
const dbname = "nodejs_1970_db";
const tbname = "t1";

let dropDB = "drop database if exists " + dbname
console.log(dropDB);//asdasdasd
c1.execute(dropDB);///asdasd

let createDB = "create database  " + dbname + " keep 36500"
console.log(createDB);
c1.execute(createDB);

let useTbl = "use " + dbname
console.log(useTbl)
c1.execute(useTbl);

let createTbl = "create table if not exists " + tbname + "(ts timestamp,id int)"
console.log(createTbl);
c1.execute(createTbl);

//1969-12-31 23:59:59.999
//1970-01-01 00:00:00.000
//1970-01-01 07:59:59.999
//1970-01-01 08:00:00.000a
//1628928479484  2021-08-14 08:07:59.484
let sql1 = "insert into " + dbname + "." + tbname + " values('1969-12-31 23:59:59.999',1)"
console.log(sql1);
c1.execute(sql1);

let sql2 = "insert into " + dbname + "." + tbname + " values('1970-01-01 00:00:00.000',2)"
console.log(sql2);
c1.execute(sql2);

let sql3 = "insert into " + dbname + "." + tbname + " values('1970-01-01 07:59:59.999',3)"
console.log(sql3);
c1.execute(sql3);

let sql4 = "insert into " + dbname + "." + tbname + " values('1970-01-01 08:00:00.000',4)"
console.log(sql4);
c1.execute(sql4);

let sql5 = "insert into " + dbname + "." + tbname + " values('2021-08-14 08:07:59.484',5)"
console.log(sql5);
c1.execute(sql5);

// Select
let query1 = "select * from " + dbname + "." + tbname
console.log(query1);
c1.execute(query1);

var d = c1.fetchall();
console.log(c1.fields);
for (let i = 0; i < d.length; i++)
  console.log(d[i][0].valueOf());

//initialize
let initSql1 = "drop table if exists " + tbname
console.log(initSql1);
c1.execute(initSql1);

console.log(createTbl);
c1.execute(createTbl);
c1.execute(useTbl)

//-28800001 1969-12-31 23:59:59.999
//-28800000 1970-01-01 00:00:00.000
//-1 1970-01-01 07:59:59.999
//0  1970-01-01 08:00:00.00
//1628928479484  2021-08-14 08:07:59.484
let sql11 = "insert into " + dbname + "." + tbname + " values(-28800001,11)";
console.log(sql11);
c1.execute(sql11);

let sql12 = "insert into " + dbname + "." + tbname + " values(-28800000,12)"
console.log(sql12);
c1.execute(sql12);

let sql13 = "insert into " + dbname + "." + tbname + " values(-1,13)"
console.log(sql13);
c1.execute(sql13);

let sql14 = "insert into " + dbname + "." + tbname + " values(0,14)"
console.log(sql14);
c1.execute(sql14);

let sql15 = "insert into " + dbname + "." + tbname + " values(1628928479484,15)"
console.log(sql15);
c1.execute(sql15);

// Select
console.log(query1);
c1.execute(query1);

var d = c1.fetchall();
console.log(c1.fields);
for (let i = 0; i < d.length; i++)
  console.log(d[i][0].valueOf());

setTimeout(function () {
  conn.close();
}, 2000);

