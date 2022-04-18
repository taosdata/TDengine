import { options, connect } from '../tdengine_rest'
options.path = '/rest/sqlt'
options.host = 'localhost'

const db = 'rest_ts_db';
const table = 'rest'
const createDB = `create database if not exists ${db} keep 3650`;
const dropDB = `drop database if exists ${db}`;
const createTB = `create table if not exists ${db}.${table}(ts timestamp,i8 tinyint,i16 smallint,i32 int,i64 bigint,bnr binary(40),nchr nchar(40))`;
const addColumn = `alter table ${db}.${table} add column new_column nchar(40) `;
const dropColumn = `alter table ${db}.${table} drop column new_column`;
const insertSql = `insert into ${db}.${table} values('2022-03-30 18:30:51.567',1,2,3,4,'binary1','nchar1')` +
    `('2022-03-30 18:30:51.568',5,6,7,8,'binary2','nchar2')` +
    `('2022-03-30 18:30:51.569',9,0,1,2,'binary3','nchar3')`;
const querySql = `select * from ${db}.${table}`;
const errorSql = 'show database';

let conn = connect(options);
let cursor = conn.cursor();

async function execute(sql: string, pure = false) {
    let result = await cursor.query(sql, pure);
    // print query result as taos shell
    result.toString();
    // Get Result object, return Result object.
    console.log("result.getResult()",result.getResult());
    // Get status, return 'succ'|'error'.
    console.log("result.getStatus()",result.getStatus());
    // Get head,return response head (Array<any>|undefined,when execute failed this is undefined).
    console.log("result.getHead()",result.getHead());
    // Get Meta data, return Meta[]|undefined(when execute failed this is undefined).
    console.log("result.getMeta()",result.getMeta());
    // Get data,return Array<Array<any>>|undefined(when execute failed this is undefined).
    console.log("result.getData()",result.getData());
    // Get affect rows,return number|undefined(when execute failed this is undefined).
    console.log("result.getAffectRows()",result.getAffectRows());
    // Get command,return SQL send to server(need to `query(sql,false)`,set 'pure=false',default true).
    console.log("result.getCommand()",result.getCommand());
    // Get error code ,return number|undefined(when execute failed this is undefined).
    console.log("result.getErrCode()",result.getErrCode());
    // Get error string,return string|undefined(when execute failed this is undefined).
    console.log("result.getErrStr()",result.getErrStr());
}

(async () => {
    let start = new Date().getTime(); // 开始时间
    await execute(createDB);
    await execute(createTB);
    await execute(addColumn);
    await execute(dropColumn);
    await execute(insertSql);
    await execute(querySql);
    await execute(errorSql);
    await execute(dropDB);
    let end = new Date().getTime(); // 结束时间
    console.log("total spend time:%d ms",end - start);
})()





