const taos = require('../tdengine');

var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 6030 });
var c1 = conn.cursor();
executeUpdate("drop database if exists  nodedb;");
executeUpdate("create database if not exists  nodedb;");
executeUpdate("use nodedb;");


let lines = "schemaless,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
//     'schemaless,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000'
// ]

// executeUpdate("select * from schemaless;");

let line2 = "{"
+"\"metric\": \"stb0_0\","
+"\"timestamp\": 1626006833,"
+"\"value\": 10,"
+"\"tags\": {"
    +" \"t1\": true,"
    +"\"t2\": false,"
    +"\"t3\": 10,"
    +"\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
 +"}"
+"}"

console.log(line2)
let protocal = 3;
let precision = 1;
try{
    // c1.schemalessInsert(line2,1,protocal,precision);
    c1.schemalessInsert(lines,1,1,6);
    
}catch (err ){
    console.log(err)
}
finally{
    setTimeout(()=>conn.close(),2000);
}



function executeUpdate(sql) {
    console.log(sql);
    c1.execute(sql);
}



