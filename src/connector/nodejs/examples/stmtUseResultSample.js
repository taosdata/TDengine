const taos = require('../tdengine');
var conn = taos.connect({ host: "localhost" });
var cursor = conn.cursor();

function executeUpdate(updateSql) {
    console.log(updateSql);
    cursor.execute(updateSql);
}
function executeQuery(querySql) {
    let query = cursor.query(querySql);
    query.execute().then((result => {
        console.log(querySql);
        result.pretty();
    }));
}

function stmtUseResultSample() {
    let db = 'node_test_db';
    let table = 'stmt_use_result';
    let subTable = 's1_0';

    let createDB = `create database if not exists ${db} keep 3650;`;
    let dropDB = `drop database if exists ${db};`;
    let useDB = `use ${db}`;
    let createTable = `create table if not exists ${table} ` +
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
        `)tags(` +
        `jsonTag json` +
        `);`;
    let createSubTable = `create table if not exists ${subTable} using ${table} tags('{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":3.2}')`;
    let querySql = `select * from ${table} where i32>?  and bnr = ? `;
    let insertSql = `insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;

    let mBinds = new taos.TaosMultiBindArr(14);
    mBinds.multiBindTimestamp([1642435200000,1642435300000,1642435400000,1642435500000,1642435600000]);
    mBinds.multiBindBool([true,false,true,undefined,null]);
    mBinds.multiBindTinyInt([-127,3,127,null,undefined]);
    mBinds.multiBindSmallInt([-256,0,256,null,undefined]);
    mBinds.multiBindInt([-1299,0,1233,null,undefined]);
    mBinds.multiBindBigInt([16424352000002222n,-16424354000001111n,0,null,undefined]);
    mBinds.multiBindFloat([12.33,0,-3.1415,null,undefined]);
    mBinds.multiBindDouble([3.141592653,0,-3.141592653,null,undefined]);
    mBinds.multiBindBinary(['TDengine_Binary','','taosdata涛思数据',null,undefined]);
    mBinds.multiBindNchar(['taos_data_nchar','taosdata涛思数据','',null,undefined]);
    mBinds.multiBindUTinyInt([0,127, 254,null,undefined]);
    mBinds.multiBindUSmallInt([0,256,512,null,undefined]);
    mBinds.multiBindUInt([0,1233,4294967294,null,undefined]);
    mBinds.multiBindUBigInt([16424352000002222n,36424354000001111n,0,null,undefined]);

    // executeUpdate(dropDB);
    executeUpdate(createDB);
    executeUpdate(useDB);
    executeUpdate(createTable);
    executeUpdate(createSubTable);

    //stmt bind values
    cursor.stmtInit();
    cursor.stmtPrepare(insertSql);
    cursor.loadTableInfo([subTable]);
    cursor.stmtSetTbname(subTable);
    cursor.stmtBindParamBatch(mBinds.getMultiBindArr());
    cursor.stmtAddBatch();
    cursor.stmtExecute();
    cursor.stmtClose();

    // stmt select with normal column.
    let condition1 = new taos.TaosBind(2);
    condition1.bindInt(0);
    condition1.bindNchar('taosdata涛思数据');
    cursor.stmtInit();
    cursor.stmtPrepare(querySql);
    cursor.stmtBindParam(condition1.getBind());
    cursor.stmtExecute();
    cursor.stmtUseResult();   
    cursor.stmtClose();
    
    cursor.fetchall();
    console.log(cursor.fields);
    console.log(cursor.data);

    executeUpdate(dropDB);
}

stmtUseResultSample();
setTimeout(() => {
    conn.close();
}, 2000);