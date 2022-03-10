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

function stmtSingleParaBatchSample() {
    let db = 'node_test_db';
    let table = 'stmt_taos_bind_single_bind_batch';

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
    let querySql = `select * from ${table};`;
    let insertSql = `insert into ? using ${table} tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?);`

    executeUpdate(dropDB);
    executeUpdate(createDB);
    executeUpdate(useDB);
    executeUpdate(createTable);

    // normal colum values.
    let mbind = new taos.TaosMultiBind();
    let tsMBind = mbind.multiBindTimestamp([1642435200000, 1642435300000, 1642435400000, 1642435500000, 1642435600000])
    let boolMbind = mbind.multiBindBool([true, false, true, undefined, null]);
    let tinyIntMbind = mbind.multiBindTinyInt([-127, 3, 127, null, undefined]);
    let smallIntMbind = mbind.multiBindSmallInt([-256, 0, 256, null, undefined]);
    let intMbind = mbind.multiBindInt([-1299, 0, 1233, null, undefined]);
    let bigIntMbind = mbind.multiBindBigInt([16424352000002222n, -16424354000001111n, 0, null, undefined]);
    let floatMbind = mbind.multiBindFloat([12.33, 0, -3.1415, null, undefined]);
    let doubleMbind = mbind.multiBindDouble([3.141592653, 0, -3.141592653, null, undefined]);
    let binaryMbind = mbind.multiBindBinary(['TDengine_Binary', '', 'taosdata涛思数据', null, undefined]);
    let ncharMbind = mbind.multiBindNchar(['taos_data_nchar', 'taosdata涛思数据', '', null, undefined]);
    let uTinyIntMbind = mbind.multiBindUTinyInt([0, 127, 254, null, undefined]);
    let uSmallIntMbind = mbind.multiBindUSmallInt([0, 256, 512, null, undefined]);
    let uIntMbind = mbind.multiBindUInt([0, 1233, 4294967294, null, undefined]);
    let uBigIntMbind = mbind.multiBindUBigInt([16424352000002222n, 36424354000001111n, 0, null, undefined]);

    // tags value.
    let tags = new taos.TaosBind(1);
    tags.bindJson('{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":3.2}');

    cursor.stmtInit();
    cursor.stmtPrepare(insertSql);
    cursor.stmtSetTbnameTags('s_01', tags.getBind());
    cursor.stmtBindSingleParamBatch(tsMBind, 0);
    cursor.stmtBindSingleParamBatch(boolMbind, 1);
    cursor.stmtBindSingleParamBatch(tinyIntMbind, 2);
    cursor.stmtBindSingleParamBatch(smallIntMbind, 3);
    cursor.stmtBindSingleParamBatch(intMbind, 4);
    cursor.stmtBindSingleParamBatch(bigIntMbind, 5);
    cursor.stmtBindSingleParamBatch(floatMbind, 6);
    cursor.stmtBindSingleParamBatch(doubleMbind, 7);
    cursor.stmtBindSingleParamBatch(binaryMbind, 8);
    cursor.stmtBindSingleParamBatch(ncharMbind, 9);
    cursor.stmtBindSingleParamBatch(uTinyIntMbind, 10);
    cursor.stmtBindSingleParamBatch(uSmallIntMbind, 11);
    cursor.stmtBindSingleParamBatch(uIntMbind, 12);
    cursor.stmtBindSingleParamBatch(uBigIntMbind, 13);

    cursor.stmtAddBatch();
    cursor.stmtExecute();
    cursor.stmtClose();

    executeQuery(querySql);
    executeUpdate(dropDB);
}
stmtSingleParaBatchSample();
setTimeout(() => {
    conn.close();
}, 2000);
