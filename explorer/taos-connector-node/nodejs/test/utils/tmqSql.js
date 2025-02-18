const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

function CreateSql(table, json = false) {
    if (json == false) {
        return `create table if not exists ${table} ` +
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
            `);`
    } else {
        return `create table if not exists ${table} ` +
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
            `json_tag json` +
            `);`;
    }
}

function createTopic(topic,table,db=''){
    if(db){
        return `create topic if not exists ${topic} as database ${db}`
    }else{
        return `create topic if not exists ${topic} as select * from ${table}`
    }
}
function dropTopic(topic){
    return `drop topic if exists ${topic}`
}
const colData1 = [1658286671000, true, -1, -2, -3, -4n, parseFloat(3.1415), parseFloat(3.141592654), 'binary_col_1', 'nchar_col_1', 1, 2, 3, 4n
    , 1658286672000, false, -2, -3, -4, -5n,parseFloat((3.1415 * 2).toFixed(5)), parseFloat((3.141592654*2).toFixed(16)), 'binary_col_2', 'nchar_col_2', 2, 3, 4, 5n];
const colData2 = [1658286673000, true, -3, -4, -5, -6n, parseFloat((3.1415 * 3).toFixed(5)), parseFloat((3.141592654*3).toFixed(16)), 'binary_col_3', 'nchar_col_3', 3, 4, 5, 6n
    , 1658286674000, false, -4, -5, -6, -7n, parseFloat((3.1415 * 4).toFixed(5)), parseFloat((3.141592654*4).toFixed(16)), 'binary_col_4', 'nchar_col_4', 4, 5, 6, 7n];
const colData3 = [1658286675000, true, -5, -6, -7, -8n, parseFloat((3.1415 * 5).toFixed(5)), parseFloat((3.141592654*5).toFixed(16)), 'binary_col_5', 'nchar_col_5', 5, 6, 7, 8n
,1658286676000, false, -6, -7, -8, -9n, parseFloat((3.1415 * 6).toFixed(5)), parseFloat((3.141592654*6).toFixed(16)), 'binary_col_6', 'nchar_col_6', 6, 7, 8, 9n];

const tagData1 = [true, 1, 2, 3, 4n, parseFloat(3.1415), parseFloat(3.141592654), 'binary_tag_1', 'nchar_tag_1', 1, 2, 3, 4n];
const tagData2 = [false, 2, 3, 4,5n, parseFloat((3.1415 * 2).toFixed(5)) , parseFloat((3.141592654*2).toFixed(16)), 'binary_tag_2', 'nchar_tag_2', 2, 3, 4,5n];

const jsonTag1 = ['{\"tag1\":false,\"tag2\":\"beijing\",\"tag3\":1}'];
const jsonTag2 = ['{\"tag1\":false,\"tag2\":\"shanghai\",\"tag3\":2}'];


module.exports = { CreateSql,createTopic,dropTopic,colData1,colData2,colData3,tagData1,tagData2,jsonTag1,jsonTag2 }

