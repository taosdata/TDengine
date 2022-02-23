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
    let db = 'node_test_db';
    let table = 'stmt_taos_bind_param_batch';

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
        `blob nchar(20),` +
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
        `t_blob nchar(20),` +
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

    let data = [1642435200000, true, -127, -256, -1299, 16424352000002222n, 12.33, 3.141592653, 'TDengine_Binary', 'taos_data_nchar', 0, 0, 0, 16424352000002222n, 1642435300000, false, 3, 0, 0, -16424354000001111n, 0, 0, '', 'taosdata涛思数据', 127, 256, 1233, 36424354000001111n, 1642435400000, true, 127, 256, 1233, 0, -3.1415, -3.141592653, 'taosdata涛思数据', '', 254, 512, 4294967294, 0, 1642435500000, false, 1, 2, 3, 123456789n, 1.2345, 4.123456789, '!@#$%^&*()abc数据', '!@#$%^&*()abc数据', 123, 123, 654321, 87654321n, 1642435600000, null, null, null, null, null, null, null, null, null, null, null, null, null];
    let tags = new taos.TaosBind(13);
    tags.bindBool(true);
    tags.bindTinyInt(127);
    tags.bindSmallInt(32767);
    tags.bindInt(1234555);
    tags.bindBigInt(-164243520000011111n);
    tags.bindFloat(214.02);
    tags.bindDouble(2.01);
    tags.bindBinary('taosdata涛思数据');
    tags.bindNchar('TDengine数据');
    tags.bindUTinyInt(254);
    tags.bindUSmallInt(65534);
    tags.bindUInt(4294967290 / 2);
    tags.bindUBigInt(164243520000011111n);

    cursor.stmtInit();
    cursor.stmtPrepare(insertSql);
    cursor.stmtSetTbnameTags('s_01', tags.getBind());

    for (let i = 0; i < data.length - 14; i += 14) {
        let bind = new taos.TaosBind(14);
        bind.bindTimestamp(data[i]);
        bind.bindBool(data[i + 1]);
        bind.bindTinyInt(data[i + 2]);
        bind.bindSmallInt(data[i + 3]);
        bind.bindInt(data[i + 4]);
        bind.bindBigInt(data[i + 5]);
        bind.bindFloat(data[i + 6]);
        bind.bindDouble(data[i + 7]);
        bind.bindBinary(data[i + 8]);
        bind.bindNchar(data[i + 9]);
        bind.bindUTinyInt(data[i + 10]);
        bind.bindUSmallInt(data[i + 11]);
        bind.bindUInt(data[i + 12]);
        bind.bindUBigInt(data[i + 13]);
        cursor.stmtBindParam(bind.getBind());
        cursor.stmtAddBatch();
    }

    let bind2 = new taos.TaosBind(14);
    bind2.bindTimestamp(data[14 * 4]);
    for (let j = 0; j < 13; j++) {
        bind2.bindNil();
    }
    cursor.stmtBindParam(bind2.getBind());
    cursor.stmtAddBatch();

    cursor.stmtExecute();

    // ===============================
    let tags2 = new taos.TaosBind(13);
    tags2.bindBool(true);
    tags2.bindTinyInt(0);
    tags2.bindSmallInt(32767);
    tags2.bindInt(1234555);
    tags2.bindBigInt(-164243520000011111n);
    tags2.bindFloat(214.02);
    tags2.bindDouble(2.01);
    tags2.bindBinary('taosdata涛思数据');
    tags2.bindNchar('TDengine数据');
    tags2.bindUTinyInt(254);
    tags2.bindUSmallInt(65534);
    tags2.bindUInt(4294967290 / 2);
    tags2.bindUBigInt(164243520000011111n);

    cursor.stmtSetTbnameTags('s_02', tags2.getBind());
    for (let i = 0; i < data.length - 14; i += 14) {
        let bind01 = new taos.TaosBind(14);
        bind01.bindTimestamp(data[i]);
        bind01.bindBool(data[i + 1]);
        bind01.bindTinyInt(data[i + 2]);
        bind01.bindSmallInt(data[i + 3]);
        bind01.bindInt(data[i + 4]);
        bind01.bindBigInt(data[i + 5]);
        bind01.bindFloat(data[i + 6]);
        bind01.bindDouble(data[i + 7]);
        bind01.bindBinary(data[i + 8]);
        bind01.bindNchar(data[i + 9]);
        bind01.bindUTinyInt(data[i + 10]);
        bind01.bindUSmallInt(data[i + 11]);
        bind01.bindUInt(data[i + 12]);
        bind01.bindUBigInt(data[i + 13]);
        cursor.stmtBindParam(bind01.getBind());
        cursor.stmtAddBatch();
    }

    let bind02 = new taos.TaosBind(14);
    bind02.bindTimestamp(data[14 * 4]);
    for (let j = 0; j < 13; j++) {
        bind02.bindNil();
    }
    cursor.stmtBindParam(bind2.getBind());
    cursor.stmtAddBatch();
    cursor.stmtExecute();
    // =============================
    cursor.stmtClose();
    executeQuery(querySql);
    // executeUpdate(dropDB);

}

stmtBindParamBatchSample();
setTimeout(() => {
    conn.close();
}, 2000);
