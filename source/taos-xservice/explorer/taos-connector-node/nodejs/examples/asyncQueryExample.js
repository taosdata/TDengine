const taos = require('../tdengine');
const conn = taos.connect({ host: "localhost" });
const cursor = conn.cursor();

const table = "qa"
const db = "node_query_a"
const createDB = `create database if not exists ${db}`;
const dropDB = `drop database if exists ${db}`;
const useDB = `use ${db}`;
const createTable = `create table if not exists ${table} ` +
  `(ts timestamp,` +
  `bl bool,` +
  `i8 tinyint,` +
  `i16 smallint,` +
  `i32 int,` +
  `i64 bigint,` +
  `f32 float,` +
  `d64 double,` +
  `bnr binary(20),` +
  `nchr nchar(20),` +
  `u8 tinyint unsigned,` +
  `u16 smallint unsigned,` +
  `u32 int unsigned,` +
  `u64 bigint unsigned` +
  `)`;

function insertData(numOfRows) {
  for (let i = 0; i < numOfRows; i++) {
    let data = [];
    data.push(`now + ${i}s`);//ts
    data.push(Boolean(Math.round(Math.random())))//bool
    data.push(-1 * i); //tinyint
    data.push(-10 * i);//smallint
    data.push(-100 * i); //int
    data.push(BigInt(-1000 * i)); //bigint
    data.push(Math.random() * (10 ** i).toFixed(5)) //float
    data.push(Math.random() * (10 ** i).toFixed(16)) //double
    data.push(`\'binary-data${i}\'`); // binary
    data.push(`\'nchar-data${i}\'`); // nchar
    data.push(1 * i); //tinyint
    data.push(10 * i);//smallint
    data.push(100 * i); //int
    data.push(BigInt(1000 * i)); //bigint

    cursor.execute(`insert into ${table} values(${data.join(",")});`);
  }
}

function initData() {
  cursor.execute(dropDB);
  cursor.execute(createDB);
  cursor.execute(useDB);
  cursor.execute(createTable);
  insertData(5);
}

var thisRes;
// define execute_a()'s callback 
var execute_a_callback = function executeACallback(param, result, resCode) {
  console.log('execute_a()\'s callback execute_a_callback()');
  console.log("RES *", result);
  console.log("Status:", resCode);
  console.log("Passed Param", param);
  thisRes = result;
}
// define fetchAll_a()'s callback
var fetchAll_a_callback = function fetchAll_a_callback(param, result, rowCount, rd) {
  console.log('fetchAll_a()\'s callback fetchAll_a_callback()');
  console.log("RES *", result);
  console.log("Async fetched", rowCount, "rows");
  console.log("Passed Param: ", param);
  console.log("Fields", rd.fields);
  console.log("Data", rd.data);
}
// using cursor.execute_a()
function cursor_execute_a() {
  var param = cursor.execute_a(`select * from ${table};`, execute_a_callback, { userParam: "myOwn" });
  setTimeout(function () {
    cursor.fetchall_a(thisRes, fetchAll_a_callback, param)
  }, 1000)
}


// using cursor.query.execute_a()
function query_execute_a() {
  let query = cursor.query(`select * from ${table};`);
  query.execute_a().then(result => {
    // print data
    console.log(result.data);
    // print meta
    console.log(result.fields);
    // pretty print result
    result.pretty();
  });
}

try {
  initData();
  cursor_execute_a();
  console.log("==================================")
  query_execute_a();
} finally {
  setTimeout(() => {
    cursor.execute(dropDB);
    conn.close();
  }, 2000);
}


