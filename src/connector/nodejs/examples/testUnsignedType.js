const taos = require('../tdengine');
var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
var c1 = conn.cursor();
function executeUpdate(sql) {
    console.log(sql);
    c1.execute(sql);
}
function executeQuery(sql) {
    c1.execute(sql)
    var data = c1.fetchall();
    // Latest query's Field metadata is stored in cursor.fields
    console.log(c1.fields); 
    // Latest query's result data is stored in cursor.data, also returned by fetchall.
    console.log(c1.data); 
}

function prettyQuery(sql){
    try {
        c1.query(sql).execute().then(function(result){
          result.pretty();
        });
      }
      catch (err) {
        conn.close();
        throw err;
      }
}

executeUpdate("create database nodedb;");
executeUpdate("use nodedb;");
executeUpdate("create table unsigntest(ts timestamp,ut tinyint unsigned,us smallint unsigned,ui int unsigned,ub bigint unsigned,bi bigint);");
executeUpdate("insert into unsigntest values (now, 254,65534,4294967294,18446744073709551614,9223372036854775807);");
executeUpdate("insert into unsigntest values (now, 0,0,0,0,-9223372036854775807);");
executeQuery("select * from unsigntest;");
prettyQuery("select * from unsigntest;");
executeUpdate("drop database nodedb;");


setTimeout(()=>conn.close(),2000);

