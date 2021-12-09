const taos = require('../tdengine');
var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
var c1 = conn.cursor();

function executeUpdate(sql) {
  console.log(sql);
  c1.execute(sql);
}
function executeQuery(sql, flag = "all") {
  c1.execute(sql)
  var data = c1.fetchall();
  if (flag == "metadata" || flag == "all") {
    // Latest query's Field metadata is stored in cursor.fields
    console.log(c1.fields);
  }2
  if (flag == "data" || flag == "all") {
    // Latest query's result data is stored in cursor.data, also returned by fetchall.
    console.log(c1.data);
  }


}
function prettyQuery(sql) {
  try {
    c1.query(sql).execute().then(function (result) {
      result.pretty();
    });
  }
  catch (err) {
    conn.close();
    throw err;
  }
}

function executeError(sql) {
  console.log(sql);
  try { c1.execute(sql) 
  }catch(e){
    console.log(e);
  }
}

executeUpdate("create database if not exists nodedb keep 36500;");
executeUpdate("use nodedb;");
console.log("# ============== STEP 1 prepare data & validate json string============");
executeUpdate("create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json);");
executeUpdate("insert into jsons1_1 using jsons1 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 1, false, 'json1', '涛思数据') (1591060608000, 23, true, '涛思数据', 'json')")
executeUpdate("insert into jsons1_2 using jsons1 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060628000, 2, true, 'json2', 'sss')")
executeUpdate("insert into jsons1_3 using jsons1 tags('{\"tag1\":false,\"tag2\":\"beijing\"}') values (1591060668000, 3, false, 'json3', 'efwe')")
executeUpdate("insert into jsons1_4 using jsons1 tags('{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}') values (1591060728000, 4, true, 'json4', '323sd')")
executeUpdate("insert into jsons1_5 using jsons1 tags('{\"tag1\":1.232, \"tag2\":null}') values(1591060928000, 1, false, '涛思数据', 'ewe')")
executeUpdate("insert into jsons1_6 using jsons1 tags('{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}') values(1591061628000, 11, false, '涛思数据','')")
executeUpdate("insert into jsons1_7 using jsons1 tags('{\"tag1\":\"涛思数据\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '涛思数据', 'dws')")

console.log("## test duplicate key using the first one. elimate empty key======");
executeUpdate("CREATE TABLE if not exists jsons1_8 using jsons1 tags('{\"tag1\":null, \"tag1\":true, \"tag1\":45, \"1tag$\":2, \" \":90}')")

console.log("## test empty json string, save as jtag is NULL=========");
executeUpdate("insert into jsons1_9  using jsons1 tags('\t') values (1591062328000, 24, NULL, '你就会', '2sdw')")
executeUpdate("CREATE TABLE if not exists jsons1_10 using jsons1 tags('')")
executeUpdate("CREATE TABLE if not exists jsons1_11 using jsons1 tags(' ')")
executeUpdate("CREATE TABLE if not exists jsons1_12 using jsons1 tags('{}')")
executeUpdate("CREATE TABLE if not exists jsons1_13 using jsons1 tags('null')")

console.log("## test invalidate json=============");


executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('\"efwewf\"')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('3333')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('33.33')")
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('false')")
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('[1,true]')")
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{222}')")
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"fe\"}')")
executeQuery("select * from jsons1;", "data");



executeUpdate("drop database nodedb;");


setTimeout(() => conn.close(), 2000);