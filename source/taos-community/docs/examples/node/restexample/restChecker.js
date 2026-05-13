const { options, connect } = require("@tdengine/rest");
options.path = '/rest/sql/'
options.host = 'localhost';
options.port = 6041;
options.user = "root";
options.passwd = "taosdata";

//optional 
// options.url = "http://127.0.0.1:6041";

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

async function execute(sql, pure = true) {
    let result = await cursor.query(sql, pure);
    // print query result as taos shell
    // Get Result object, return Result object.
    console.log("result.getResult()",result.getResult());
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
    // start execute time
    let start = new Date().getTime(); 
    await execute(createDB);
    console.log("-----------------------------------")

    await execute(createTB);
    console.log("-----------------------------------")

    await execute(addColumn);
    console.log("----------------------------------")

    await execute(dropColumn);
    console.log("-----------------------------------")

    await execute(insertSql);
    console.log("-----------------------------------")

    await execute(querySql);
    console.log("-----------------------------------")

    await execute(errorSql);
    console.log("-----------------------------------")

    await execute(dropDB);
    // finish time
    let end = new Date().getTime(); 
    console.log("total spend time:%d ms",end - start);
})()




