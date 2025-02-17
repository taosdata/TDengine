import { options, connect } from '../index'

// get TDengine cloud token form env variables
let tokenEnv = process.env["TDENGINE_CLOUD_TOKEN"];

// get TDengine cloud token form env variables
// eg: http://localhost:6041
let urlEnv = process.env["TDENGINE_CLOUD_URL"];

if (tokenEnv != null || tokenEnv != undefined) {
    // set your cloud token
    options.query = { token: tokenEnv }
} else {
    throw new Error("TDENGINE_CLOUD_TOKEN is undefined,please set TDENGINE_CLOUD_TOKEN.");
}

if (urlEnv != null || urlEnv != undefined) {
    // add your cloud url
    options.url = urlEnv;
} else {
    throw new Error("TDENGINE_CLOUD_URL is undefined,please set TDENGINE_CLOUD_URL.");
}

const sql = 'show databases';

let conn = connect(options);
let cursor = conn.cursor();

async function execute(sql: string, pure = false) {
    let result = await cursor.query(sql, pure).catch(e => {
        throw new Error(e);
    });
    // print query result as taos shell
    result.toString();
    // Get Result object, return Result object.
    console.log("result.getResult()", result.getResult());
    // Get Meta data, return Meta[]|undefined(when execute failed this is undefined).
    console.log("result.getMeta()", result.getMeta());
    // Get data,return Array<Array<any>>|undefined(when execute failed this is undefined).
    console.log("result.getData()", result.getData());
    // Get affect rows,return number|undefined(when execute failed this is undefined).
    console.log("result.getAffectRows()", result.getAffectRows());
    // Get command,return SQL send to server(need to `query(sql,false)`,set 'pure=false',default true).
    console.log("result.getCommand()", result.getCommand());
    // Get error code ,return number|undefined(when execute failed this is undefined).
    console.log("result.getErrCode()", result.getErrCode());
    // Get error string,return string|undefined(when execute failed this is undefined).
    console.log("result.getErrStr()", result.getErrStr());
}

(async () => {
    let start = new Date().getTime(); // start time
    await execute(sql)
    let end = new Date().getTime(); // end time
    console.log("total spend time:%d ms", end - start);
})()


