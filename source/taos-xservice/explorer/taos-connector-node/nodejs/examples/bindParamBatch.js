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

function stmtBindParamBatchSample() {
    let db = 'stmt_db';
    let table = 'bind_param_batch';

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
        `t_bl bool,` +
        `t_i8 tinyint,` +
        `t_i16 smallint,` +
        `t_i32 int,` +
        `t_i64 bigint,` +
        `t_f32 float,` +
        `t_d64 double,` +
        `t_bnr binary(20),` +
        `t_nchr nchar(20),` +
        `t_u8 tinyint unsigned,` +
        `t_u16 smallint unsigned,` +
        `t_u32 int unsigned,` +
        `t_u64 bigint unsigned` +
        `);`;
    let querySql = `select * from ${table};`;
    let insertSql = `insert into ? using ${table} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;

    executeUpdate(dropDB);
    executeUpdate(createDB);
    executeUpdate(useDB);
    executeUpdate(createTable);

    let mBinds = new taos.TaosMultiBindArr(14);
    mBinds.multiBindTimestamp([1642435200000, 1642435300000, 1642435400000, 1642435500000, 1642435600000]);
    mBinds.multiBindBool([true, false, true, undefined, null]);
    mBinds.multiBindTinyInt([-127, 3, 127, null, undefined]);
    mBinds.multiBindSmallInt([-256, 0, 256, null, undefined]);
    mBinds.multiBindInt([-1299, 0, 1298, null, undefined]);
    mBinds.multiBindBigInt([16424352000002222n, -16424354000001111n, 0, null, undefined]);
    mBinds.multiBindFloat([12.33, 0, -3.1415, null, undefined]);
    mBinds.multiBindDouble([3.141592653, 0, -3.141592653, null, undefined]);
    mBinds.multiBindBinary(['TDengine_binary', 'taos涛思', '', null, undefined]);
    mBinds.multiBindNchar(['taos_data_nchar', '数据data', '', null, undefined]);
    mBinds.multiBindUTinyInt([0, 127, 254, null, undefined]);
    mBinds.multiBindUSmallInt([0, 256, 512, null, undefined]);
    mBinds.multiBindUInt([0, 1233, 4294967294, null, undefined]);
    mBinds.multiBindUBigInt([16424352000002222n, 36424354000001111n, 0, null, undefined]);

    let tags = new taos.TaosMultiBindArr(13);

    tags.multiBindBool([true]);
    tags.multiBindTinyInt([-1]);
    tags.multiBindSmallInt([-2]);
    tags.multiBindInt([-3]);
    tags.multiBindBigInt([BigInt(-4)]);
    tags.multiBindFloat([parseFloat(3.14129)]);
    tags.multiBindDouble([parseFloat(3.141296531)]);
    tags.multiBindBinary(['abcdefg']);
    tags.multiBindNchar(['涛思数据']);
    tags.multiBindUTinyInt([1]);
    tags.multiBindUSmallInt([2]);
    tags.multiBindUInt([3]);
    tags.multiBindUBigInt([BigInt(4)]);

    cursor.stmtInit();
    cursor.stmtPrepare(insertSql);
    cursor.stmtSetTbnameTags('s_01', tags.getMultiBindArr());
    cursor.stmtBindParamBatch(mBinds.getMultiBindArr());
    cursor.stmtAddBatch();
    cursor.stmtExecute();
    cursor.stmtClose();

    executeQuery(querySql);
    // executeUpdate(dropDB);    

}

stmtBindParamBatchSample();
setTimeout(() => {
    conn.close();
}, 2000);
