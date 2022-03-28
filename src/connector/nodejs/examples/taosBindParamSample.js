// const TaosBind = require('../nodetaos/taosBind');
const taos = require('../tdengine');
var conn = taos.connect({ host: "localhost" });
var cursor = conn.cursor();

function executeUpdate(updateSql){
    console.log(updateSql);
    cursor.execute(updateSql);
}
function executeQuery(querySql){
    
    let query = cursor.query(querySql);
    query.execute().then((result=>{
        console.log(querySql);
        result.pretty();
    }));
}

function stmtBindParamSample(){
    let db = 'node_test_db';
    let table = 'stmt_taos_bind_sample';

    let createDB = `create database if not exists ${db} keep 3650;`;
    let dropDB = `drop database if exists ${db};`;
    let useDB = `use ${db}`;
    let createTable = `create table if not exists ${table} `+
                    `(ts timestamp,`+
                    `nil int,`+
                    `bl bool,`+
                    `i8 tinyint,`+
                    `i16 smallint,`+
                    `i32 int,`+
                    `i64 bigint,`+
                    `f32 float,`+
                    `d64 double,`+
                    `bnr binary(20),`+
                    `nchr nchar(20),`+
                    `u8 tinyint unsigned,`+
                    `u16 smallint unsigned,`+
                    `u32 int unsigned,`+
                    `u64 bigint unsigned);`;
    let querySql = `select * from ${table};`;
    let insertSql = `insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);`   

    executeUpdate(dropDB);
    executeUpdate(createDB);
    executeUpdate(useDB);
    executeUpdate(createTable);

    let binds = new taos.TaosBind(15);
    binds.bindTimestamp(1642435200000);
    binds.bindNil();
    binds.bindBool(true);
    binds.bindTinyInt(127);
    binds.bindSmallInt(32767);
    binds.bindInt(1234555);
    binds.bindBigInt(-164243520000011111n);
    binds.bindFloat(214.02);
    binds.bindDouble(2.01);
    binds.bindBinary('taosdata涛思数据');
    binds.bindNchar('TDengine数据');
    binds.bindUTinyInt(254);
    binds.bindUSmallInt(65534);
    binds.bindUInt(4294967294);
    binds.bindUBigInt(164243520000011111n);

    cursor.stmtInit();
    cursor.stmtPrepare(insertSql);
    cursor.stmtSetTbname(table);
    cursor.bindParam(binds.getBind());
    cursor.addBatch();
    cursor.stmtExecute();
    cursor.stmtClose();

    executeQuery(querySql);
    executeUpdate(dropDB);
}

stmtBindParamSample();
setTimeout(()=>{
    conn.close();
},2000);