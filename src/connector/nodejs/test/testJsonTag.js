const taos = require('../tdengine');
var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
var c1 = conn.cursor();

function executeUpdate(sql) {
  console.log(sql);
  c1.execute(sql);
}
function executeQuery(sql, flag = "all") {
  console.log(sql);
  c1.execute(sql)
  var data = c1.fetchall();
  if (flag == "metadata" || flag == "all") {
    // Latest query's Field metadata is stored in cursor.fields
    console.log(c1.fields);
  } 2
  if (flag == "data" || flag == "all") {
    // Latest query's result data is stored in cursor.data, also returned by fetchall.
    console.log(c1.data);
  }
  console.log("");
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
  try {
    c1.execute(sql)
  } catch (e) {
    console.log(e.message);
    console.log("");
  }
}

executeUpdate("create database if not exists nodedb keep 36500;");
executeUpdate("use nodedb;");
console.log("# STEP 1 prepare data & validate json string");
executeUpdate("create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json);");
executeUpdate("insert into jsons1_1 using jsons1 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 1, false, 'json1', '涛思数据') (1591060608000, 23, true, '涛思数据', 'json')");
executeUpdate("insert into jsons1_2 using jsons1 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060628000, 2, true, 'json2', 'sss')");
executeUpdate("insert into jsons1_3 using jsons1 tags('{\"tag1\":false,\"tag2\":\"beijing\"}') values (1591060668000, 3, false, 'json3', 'efwe')");
executeUpdate("insert into jsons1_4 using jsons1 tags('{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}') values (1591060728000, 4, true, 'json4', '323sd')");
executeUpdate("insert into jsons1_5 using jsons1 tags('{\"tag1\":1.232, \"tag2\":null}') values(1591060928000, 1, false, '涛思数据', 'ewe')");
executeUpdate("insert into jsons1_6 using jsons1 tags('{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}') values(1591061628000, 11, false, '涛思数据','')");
executeUpdate("insert into jsons1_7 using jsons1 tags('{\"tag1\":\"涛思数据\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '涛思数据', 'dws')");

console.log("## test duplicate key using the first one. elimate empty key");
executeUpdate("CREATE TABLE if not exists jsons1_8 using jsons1 tags('{\"tag1\":null, \"tag1\":true, \"tag1\":45, \"1tag$\":2, \" \":90}')");

console.log("## test empty json string, save as jtag is NULL");
executeUpdate("insert into jsons1_9  using jsons1 tags('\t') values (1591062328000, 24, NULL, '涛思数据', '2sdw')");
executeUpdate("CREATE TABLE if not exists jsons1_10 using jsons1 tags('')");
executeUpdate("CREATE TABLE if not exists jsons1_11 using jsons1 tags(' ')");
executeUpdate("CREATE TABLE if not exists jsons1_12 using jsons1 tags('{}')");
executeUpdate("CREATE TABLE if not exists jsons1_13 using jsons1 tags('null')");

console.log("## test invalidate json");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('\"efwewf\"')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('3333')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('33.33')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('false')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('[1,true]')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{222}')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"fe\"}')");
executeQuery("select * from jsons1;", "data");

console.log("## test invalidate json key, key must can be printed assic char=");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":[1,true]}')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":{}}')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"。loc\":\"fff\"}')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\":\"fff\"}')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\t\":\"fff\"}')");
executeError("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"涛思数据\":\"fff\"}')");

console.log("#  STEP 2 alter table json tag");
executeError("ALTER STABLE jsons1 add tag tag2 nchar(20)");
executeError("ALTER STABLE jsons1 drop tag jtag");
executeError("ALTER TABLE jsons1_1 SET TAG jtag=4");
executeUpdate("ALTER TABLE jsons1_1 SET TAG jtag='{\"tag1\":\"femail\",\"tag2\":35,\"tag3\":true}'")

console.log("#  STEP 3 query table");
console.log("## test error syntax");
executeError("select * from jsons1 where jtag->tag1='beijing'");
executeError("select * from jsons1 where jtag->'location'");
executeError("select * from jsons1 where jtag->''");
executeError("select * from jsons1 where jtag->''=9");
executeError("select -> from jsons1");
executeError("select * from jsons1 where contains");
executeError("select * from jsons1 where jtag->");
executeError("select jtag->location from jsons1");
executeError("select jtag contains location from jsons1");
executeError("select * from jsons1 where jtag contains location");
executeError("select * from jsons1 where jtag contains''");
executeError("select * from jsons1 where jtag contains 'location'='beijing'");

console.log("## test select normal column");
executeQuery("select dataint from jsons1");

console.log("## test select json tag");
executeQuery("select * from jsons1", "data")
executeQuery("select jtag from jsons1", "data");
executeQuery("select jtag from jsons1 where jtag is null", "data");
executeQuery("select jtag from jsons1 where jtag is not null", "data");
executeQuery("select jtag from jsons1_8", "data");
executeQuery("select jtag from jsons1_1", "data");

console.log("## test jtag is NULL");
executeQuery("select jtag from jsons1_9", "data");

console.log("## test select json tag->'key', value is string");
executeQuery("select jtag->'tag1' from jsons1_1", "data");
executeQuery("select jtag->'tag2' from jsons1_6", "data");

console.log("### test select json tag->'key', value is int");
executeQuery("select jtag->'tag2' from jsons1_1", "data");

console.log("### test select json tag->'key', value is bool");
executeQuery("select jtag->'tag3' from jsons1_1", "data");

console.log("### test select json tag->'key', value is null");
executeQuery("select jtag->'tag1' from jsons1_4", "data");

console.log("### test select json tag->'key', value is double");
executeQuery("select jtag->'tag1' from jsons1_5", "data");

console.log("### test select json tag->'key', key is not exist");
executeQuery("select jtag->'tag10' from jsons1_4", "data");
executeQuery("select jtag->'tag1' from jsons1", "data");

console.log("### test header name");
executeQuery("select jtag->'tag1' from jsons1", "metadata");

console.log("## test where with json tag");
executeError("select * from jsons1_1 where jtag is not null");
executeError("select * from jsons1 where jtag='{\"tag1\":11,\"tag2\":\"\"}'");
executeError("select * from jsons1 where jtag->'tag1'={}");

console.log("### where json value is string");
executeQuery("select * from jsons1 where jtag->'tag2'='beijing'", "data");
executeQuery("select dataint,tbname,jtag->'tag1',jtag from jsons1 where jtag->'tag2'='beijing'");
executeQuery("select * from jsons1 where jtag->'tag1'='beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag1'='涛思数据'", "data");
executeQuery("select * from jsons1 where jtag->'tag2'>'beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag2'>='beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag2'<'beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag2'<='beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag2'!='beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag2'=''", "data");

console.log("### where json value is int");
executeQuery("select * from jsons1 where jtag->'tag1'=5", "data");
executeQuery("select * from jsons1 where jtag->'tag1'=10", "data");
executeQuery("select * from jsons1 where jtag->'tag1'<54", "data");
executeQuery("select * from jsons1 where jtag->'tag1'<=11", "data");
executeQuery("select * from jsons1 where jtag->'tag1'>4", "data");
executeQuery("select * from jsons1 where jtag->'tag1'>=5", "data");
executeQuery("select * from jsons1 where jtag->'tag1'!=5", "data");
executeQuery("select * from jsons1 where jtag->'tag1'!=55", "data");

console.log("### where json value is double");
executeQuery("select * from jsons1 where jtag->'tag1'=1.232", "data");
executeQuery("select * from jsons1 where jtag->'tag1'<1.232", "data");
executeQuery("select * from jsons1 where jtag->'tag1'<=1.232", "data");
executeQuery("select * from jsons1 where jtag->'tag1'>1.23", "data");
executeQuery("select * from jsons1 where jtag->'tag1'>=1.232", "data");
executeQuery("select * from jsons1 where jtag->'tag1'!=1.232", "data");
executeQuery("select * from jsons1 where jtag->'tag1'!=3.232", "data");
executeError("select * from jsons1 where jtag->'tag1'/0=3", "data");
executeError("select * from jsons1 where jtag->'tag1'/5=1", "data");

console.log("### where json value is bool");
executeQuery("select * from jsons1 where jtag->'tag1'=true", "data");
executeQuery("select * from jsons1 where jtag->'tag1'=false", "data");
executeQuery("select * from jsons1 where jtag->'tag1'!=false", "data");
executeError("select * from jsons1 where jtag->'tag1'>false");

console.log("### where json value is null");
executeQuery("select * from jsons1 where jtag->'tag1'=null");     //only json suport =null. This synatx will change later.

console.log("### where json is null");
executeQuery("select * from jsons1 where jtag is null", "data");
executeQuery("select * from jsons1 where jtag is not null", "data");

console.log("### where json key is null");
executeQuery("select * from jsons1 where jtag->'tag_no_exist'=3", "data")

console.log("### where json value is not exist");
executeQuery("select * from jsons1 where jtag->'tag1' is null", "data");
executeQuery("select * from jsons1 where jtag->'tag4' is null", "data");
executeQuery("select * from jsons1 where jtag->'tag3' is not null", "data")

console.log("### test contains");
executeQuery("select * from jsons1 where jtag contains 'tag1'", "data")
executeQuery("select * from jsons1 where jtag contains 'tag3'", "data")
executeQuery("select * from jsons1 where jtag contains 'tag_no_exist'", "data")

console.log("### test json tag in where condition with and/or");
executeQuery("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag1'=false or jtag->'tag2'='beijing'", "data");
executeQuery("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'", "data");
executeQuery("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'", "data");
executeQuery("select * from jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35", "data");
executeQuery("select * from jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35", "data");
executeQuery("select * from jsons1 where jtag->'tag1' is not null and jtag contains 'tag3'", "data");
executeQuery("select * from jsons1 where jtag->'tag1'='femail' and jtag contains 'tag3'", "data");

console.log("### test with tbname/normal column");
executeQuery("select * from jsons1 where tbname = 'jsons1_1'", "data")
executeQuery("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3'", "data")
executeQuery("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=3", "data")
executeQuery("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=23", "data")

console.log("### test where condition like");
executeQuery("select *,tbname from jsons1 where jtag->'tag2' like 'bei%'", "data");
executeQuery("select *,tbname from jsons1 where jtag->'tag1' like 'fe%' and jtag->'tag2' is not null", "data");

console.log("### test where condition in  no support in");
executeError("select * from jsons1 where jtag->'tag1' in ('beijing')");

console.log("### test where condition match");
executeQuery("select * from jsons1 where jtag->'tag1' match 'ma'", "data");
executeQuery("select * from jsons1 where jtag->'tag1' match 'ma$'", "data");
executeQuery("select * from jsons1 where jtag->'tag2' match 'jing$'", "data");
executeQuery("select * from jsons1 where jtag->'tag1' match '收到'", "data");

console.log("### test distinct");
executeUpdate("insert into jsons1_14 using jsons1 tags('{\"tag1\":\"涛思数据\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '涛思数据', 'dws')", "data");
executeQuery("select distinct jtag->'tag1' from jsons1", "data");
executeQuery("select distinct jtag from jsons1", "data");

console.log("### test dumplicate key with normal colomn");
executeUpdate("INSERT INTO jsons1_15 using jsons1 tags('{\"tbname\":\"tt\",\"databool\":true,\"datastr\":\"涛思数据\"}') values(1591060828000, 4, false, 'jjsf', \"涛思数据\")");
executeQuery("select *,tbname,jtag from jsons1 where jtag->'datastr' match '涛思' and datastr match 'js'", "data");
executeQuery("select tbname,jtag->'tbname' from jsons1 where jtag->'tbname'='tt' and tbname='jsons1_14'", "data");

console.log("## test join");
executeUpdate("create table if not exists jsons2(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
executeUpdate("insert into jsons2_1 using jsons2 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 2, false, 'json2', '你是2')")
executeUpdate("insert into jsons2_2 using jsons2 tags('{\"tag1\":5,\"tag2\":null}') values (1591060628000, 2, true, 'json2', 'sss')")
executeUpdate("create table if not exists jsons3(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
executeUpdate("insert into jsons3_1 using jsons3 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 3, false, 'json3', '你是3')")
executeUpdate("insert into jsons3_2 using jsons3 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060638000, 2, true, 'json3', 'sss')")

executeQuery("select 'sss',33,a.jtag->'tag3' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'", "data");
executeQuery("select 'sss',33,a.jtag->'tag3' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'", "metadata");

console.log("## test group by & order by  json tag");

executeQuery("select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' desc", "data");
executeQuery("select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' asc", "data");

console.log("## test stddev with group by json tag");
executeQuery("select stddev(dataint) from jsons1 group by jtag->'tag1'", "data");
executeQuery("select stddev(dataint) from jsons1 group by jsons1.jtag->'tag1'", "metadata");

console.log("## test top/bottom with group by json tag");
executeQuery("select top(dataint,100) from jsons1 group by jtag->'tag1'", "metadata");

console.log("## subquery with json tag");
executeQuery("select * from (select jtag, dataint from jsons1)", "metadata");
executeQuery("select jtag->'tag1' from (select jtag->'tag1', dataint from jsons1)", "metadata");
executeQuery("select jtag->'tag1' from (select jtag->'tag1', dataint from jsons1)", "metada");
executeQuery("select ts,tbname,jtag->'tag1' from (select jtag->'tag1',tbname,ts from jsons1 order by ts)", "data")


executeUpdate("drop database nodedb;");


setTimeout(() => conn.close(), 2000);
