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


executeUpdate("create database if not exists nodedb keep 36500;");
executeUpdate("use nodedb;");
executeUpdate("create stable if not exists p (ts timestamp, v1 int) tags (info json);");
executeUpdate("create table p1 using p tags ('{\"filetype\": \"insert\",     \"cfgdir\": \"/etc/taos\",     \"host\": \"127.0.0.1\",     \"port\": 6030,     \"user\": \"root\",     \"password\": \"taosdata\",     \"thread_count\": 4,     \"thread_count_create_tbl\": 4,     \"result_file\": \"./insert_res.txt\",     \"confirm_parameter_prompt\": \"no\",     \"insert_interval\": 0,     \"interlace_rows\": 100,     \"num_of_records_per_req\": 100     }')");
executeUpdate("insert into p1 values (1638771303000, 0);");
executeUpdate("insert into p1 values (1638771303100, 1);");
executeUpdate("insert into p1 values (1638771303200, 2);");

executeQuery("select * from p;");
prettyQuery("select * from p;");
executeUpdate("drop database nodedb;");


setTimeout(() => conn.close(), 2000);