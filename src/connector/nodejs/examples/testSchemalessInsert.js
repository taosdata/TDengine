const _ = require('lodash');
const taos = require('../tdengine');

var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
var c1 = conn.cursor();
executeUpdate("drop database if exists  nodedb;");
executeUpdate("create database if not exists  nodedb ;");
executeUpdate("use nodedb;");

let tbname1 = "line_protocol_arr";
let tbname2 = "json_protocol_arr";
let tbname3 = "json_protocol_str";
let tbname4 = "line_protocol_str";


let line1 = [tbname1 + ",t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
tbname1 + ",t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000"
];
let line2 = ["{"
    + "\"metric\": \"" + tbname2 + "\","
    + "\"timestamp\": 1626006833,"
    + "\"value\": 10,"
    + "\"tags\": {"
    + " \"t1\": true,"
    + "\"t2\": false,"
    + "\"t3\": 10,"
    + "\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
    + "}"
    + "}"
];

let line3 = "{"
    + "\"metric\": \"" + tbname3 + "\","
    + "\"timestamp\": 1626006833000,"
    + "\"value\": 10,"
    + "\"tags\": {"
    + " \"t1\": true,"
    + "\"t2\": false,"
    + "\"t3\": 10,"
    + "\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
    + "}"
    + "}";

let line4 = tbname4 + ",t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639";


try {

    c1.schemalessInsert(line1, taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_NANO_SECONDS);
    testSchemaless(tbname1, line1.length);

    c1.schemalessInsert(line2, taos.SCHEMALESS_PROTOCOL.TSDB_SML_JSON_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_SECONDS);
    testSchemaless(tbname2, line2.length);

    c1.schemalessInsert(line3, taos.SCHEMALESS_PROTOCOL.TSDB_SML_JSON_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
    testSchemaless(tbname3, 1);

    c1.schemalessInsert(line4, taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
    testSchemaless(tbname4, 1);

} catch (err) {
    console.log(err)
}
function executeUpdate(sql) {
    console.log(sql);
    c1.execute(sql);
}

function testSchemaless(tbname, numLines) {
    let sql = "select count(*) from " + tbname + ";";
    executeUpdate(sql);
    let affectRows = _.first(c1.fetchall());
    if (affectRows != numLines) {
        console.log(1);
        console.log(line2);
        throw "protocol " + tbname + " schemaless insert success,but can't select as expect."
    }
    else {
        console.log("protocol " + tbname + " schemaless insert success, can select as expect.")
    }
    console.log("===================")
}

setTimeout(() => conn.close(), 2000);
